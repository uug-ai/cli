package loadtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/uug-ai/models/pkg/models"
)

const (
	monitorQueue    = "kcloud-monitor-queue"
	sequenceQueue   = "kcloud-sequence-queue"
	deadLetterQueue = "dead-letter-queue"
	runLockQueue    = "loadtest-run-lock"
)

type rabbitTransport struct {
	conn       *amqp.Connection
	socket     net.Conn
	timeout    time.Duration
	prefetch   int
	consumer   *amqp.Channel
	deliveries <-chan amqp.Delivery
	runID      string
	mu         sync.Mutex
	observing  bool
}

// Only locally constructed diagnostics are safe to report verbatim.
type rabbitDiagnostic string

func (e rabbitDiagnostic) Error() string { return string(e) }

func rabbitSafeCause(err error) string {
	var brokerError *amqp.Error
	if errors.As(err, &brokerError) {
		switch brokerError.Code {
		case 403:
			return "AMQP 403: access refused"
		case 404:
			return "AMQP 404: queue not found"
		case 405:
			return "AMQP 405: resource locked by another connection"
		case 406:
			return "AMQP 406: queue declaration mismatch or queue is not empty/unused"
		default:
			return fmt.Sprintf("AMQP %d", brokerError.Code)
		}
	}
	return "AMQP operation failed"
}

// validateRabbitURL never includes parser errors: those can contain credentials.
func validateRabbitURL(raw string) error {
	return validateRabbitURLTarget(raw, false)
}

func validateRabbitURLTarget(raw string, allowRoot bool) error {
	u, err := url.Parse(raw)
	if err != nil || u == nil {
		return errors.New("invalid RabbitMQ URL")
	}
	if (u.Scheme != "amqp" && u.Scheme != "amqps") || u.Hostname() == "" || u.Opaque != "" {
		return errors.New("RabbitMQ URL requires amqp(s) and an explicit host")
	}
	if u.User == nil || u.User.Username() == "" {
		return errors.New("RabbitMQ URL requires explicit credentials")
	}
	if password, ok := u.User.Password(); !ok || password == "" {
		return errors.New("RabbitMQ URL requires explicit credentials")
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(raw, "#") {
		return errors.New("RabbitMQ URL query options and fragments are not allowed")
	}
	parsed, err := amqp.ParseURI(raw)
	if err != nil {
		return errors.New("invalid RabbitMQ URL")
	}
	if allowRoot && parsed.Vhost == "/" && (u.Path == "/" || strings.EqualFold(u.EscapedPath(), "/%2f")) {
		return nil
	}
	vhost := strings.TrimPrefix(u.Path, "/")
	if !strings.HasPrefix(vhost, "loadtest_") || len(vhost) == len("loadtest_") {
		return errors.New("RabbitMQ virtual host must begin with loadtest_")
	}
	for _, r := range vhost {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return errors.New("RabbitMQ virtual host contains unsupported characters")
		}
	}
	return nil
}

func NewRabbitTransport(ctx context.Context, cfg Config) (Transport, error) {
	if err := validateRabbitURLTarget(cfg.RabbitURL, cfg.AllowDefaultNames && cfg.ConfirmTestTarget); err != nil {
		return nil, err
	}
	if cfg.OperationTimeout <= 0 {
		return nil, errors.New("RabbitMQ operation timeout must be positive")
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.OperationTimeout)
	defer cancel()
	r := &rabbitTransport{timeout: cfg.OperationTimeout, prefetch: cfg.Concurrency}
	if r.prefetch < 1 || r.prefetch > 128 {
		r.prefetch = 128
	}
	var stop func() bool
	conn, err := amqp.DialConfig(cfg.RabbitURL, amqp.Config{
		Dial: func(network, address string) (net.Conn, error) {
			socket, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			r.socket = socket
			deadline, _ := ctx.Deadline()
			if err := socket.SetDeadline(deadline); err != nil {
				_ = socket.Close()
				return nil, err
			}
			stop = context.AfterFunc(ctx, func() { _ = socket.Close() })
			return socket, nil
		},
	})
	if stop != nil {
		stop()
	}
	if err != nil || ctx.Err() != nil {
		if r.socket != nil {
			_ = r.socket.Close()
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("RabbitMQ connection failed: %w", ctx.Err())
		}
		return nil, errors.New("RabbitMQ connection or TLS/AMQP handshake failed")
	}
	// amqp091-go openComplete clears the TLS/AMQP handshake deadline before
	// DialConfig returns; subsequent read deadlines belong to its heartbeater.
	r.conn = conn
	return r, nil
}

// AMQP 0.9.1 methods do not honor context during socket writes. Closing the
// underlying socket interrupts them without extending a shared socket deadline
// (which would otherwise interfere with the completion collector).
func (r *rabbitTransport) bounded(ctx context.Context, operation string, fn func() error) error {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("RabbitMQ %s: %w", operation, err)
	}
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		if err != nil {
			var diagnostic rabbitDiagnostic
			if errors.As(err, &diagnostic) {
				return fmt.Errorf("RabbitMQ %s: %w", operation, diagnostic)
			}
			return fmt.Errorf("RabbitMQ %s failed: %s", operation, rabbitSafeCause(err))
		}
		return nil
	case <-ctx.Done():
		_ = r.socket.Close()
		// All callers use AMQP socket operations; closing the socket releases
		// them. Join before returning so result fields cannot race the caller.
		<-done
		return fmt.Errorf("RabbitMQ %s interrupted; delivery may be uncertain: %w", operation, ctx.Err())
	}
}

func (r *rabbitTransport) Setup(ctx context.Context, m Manifest) error {
	if err := validateFixtureManifest(m); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.consumer != nil {
		return errors.New("RabbitMQ transport already set up")
	}
	return r.bounded(ctx, "setup (check queue types, idle queues, live workers and run lock)", func() error {
		ch, err := r.conn.Channel()
		if err != nil {
			return err
		}
		r.consumer = ch
		// This ephemeral, exclusive queue fences all runners in this vhost,
		// including runners pointed at different MongoDB databases.
		if _, err := ch.QueueDeclare(runLockQueue, false, true, true, false, amqp.Table{"x-queue-type": "classic"}); err != nil {
			return rabbitDiagnostic("vhost-wide run lock unavailable: " + rabbitSafeCause(err))
		}
		for _, name := range []string{EventQueue, monitorQueue, sequenceQueue, deadLetterQueue, CompletionQueue(m)} {
			if _, err := ch.QueueDeclare(name, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
				return rabbitDiagnostic(fmt.Sprintf("declare durable quorum queue %s: %s", name, rabbitSafeCause(err)))
			}
			q, err := ch.QueueInspect(name)
			if err != nil {
				return rabbitDiagnostic(fmt.Sprintf("inspect queue %s: %s", name, rabbitSafeCause(err)))
			}
			if q.Messages != 0 {
				return rabbitDiagnostic(fmt.Sprintf("queue %s has pre-existing traffic (%d ready messages)", name, q.Messages))
			}
			if name == EventQueue || name == monitorQueue || name == sequenceQueue {
				if q.Consumers == 0 {
					return rabbitDiagnostic(fmt.Sprintf("pipeline worker is absent: queue %s has no consumers", name))
				}
			} else if q.Consumers != 0 {
				return rabbitDiagnostic(fmt.Sprintf("queue %s has unexpected completion or dead-letter consumers", name))
			}
		}
		if err := ch.Qos(r.prefetch, 0, false); err != nil {
			return err
		}
		// Setup success is the readiness barrier: consume-ok has been received
		// before the scheduler may publish even its first recording.
		r.deliveries, err = ch.Consume(CompletionQueue(m), "", false, true, false, false, nil)
		if err == nil {
			r.runID = m.RunID
		}
		return err
	})
}

type confirmChannel interface {
	PublishWithContext(context.Context, string, string, bool, bool, amqp.Publishing) error
	Close() error
}

type rabbitPublisher struct {
	transport *rabbitTransport
	channel   confirmChannel
	confirms  <-chan amqp.Confirmation
	returns   <-chan amqp.Return
	closed    <-chan *amqp.Error
	next      uint64
	failed    bool
}

func (r *rabbitTransport) Publisher(ctx context.Context) (Publisher, error) {
	p := &rabbitPublisher{transport: r, next: 1}
	err := r.bounded(ctx, "publisher initialization", func() error {
		ch, err := r.conn.Channel()
		if err != nil {
			return err
		}
		p.channel = ch
		p.returns = ch.NotifyReturn(make(chan amqp.Return, 1))
		p.confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
		p.closed = ch.NotifyClose(make(chan *amqp.Error, 1))
		return ch.Confirm(false)
	})
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (p *rabbitPublisher) Publish(ctx context.Context, body []byte) error {
	if p.failed {
		return errors.New("RabbitMQ publisher unusable after an earlier failure")
	}
	ctx, cancel := context.WithTimeout(ctx, p.transport.timeout)
	defer cancel()
	err := p.transport.bounded(ctx, "publish", func() error {
		return p.channel.PublishWithContext(ctx, "", EventQueue, true, false, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		})
	})
	if err == nil {
		err = awaitConfirmation(ctx, p.next, p.confirms, p.returns, p.closed)
	}
	p.next++
	if err != nil {
		p.failed = true
		// Confirmation timeouts are uncertain delivery, never an invitation to
		// retry. Aborting also unblocks all other workers and the collector.
		if ctx.Err() != nil {
			_ = p.transport.socket.Close()
		}
	}
	return err
}

func awaitConfirmation(ctx context.Context, tag uint64, confirms <-chan amqp.Confirmation, returns <-chan amqp.Return, closed <-chan *amqp.Error) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("RabbitMQ confirmation interrupted; delivery may be uncertain: %w", ctx.Err())
		case _, ok := <-returns:
			if !ok {
				return errors.New("RabbitMQ return stream closed; delivery may be uncertain")
			}
			return errors.New("RabbitMQ mandatory publish returned as unroutable")
		case confirmation, ok := <-confirms:
			if !ok {
				return errors.New("RabbitMQ confirm stream closed; delivery may be uncertain")
			}
			// The library dispatches basic.return before basic.ack, but select
			// may choose either buffered notification. Drain returns first.
			select {
			case _, ok := <-returns:
				if !ok {
					return errors.New("RabbitMQ return stream closed; delivery may be uncertain")
				}
				return errors.New("RabbitMQ mandatory publish returned as unroutable")
			default:
			}
			if confirmation.DeliveryTag != tag {
				return errors.New("RabbitMQ confirmation delivery tag mismatch")
			}
			if !confirmation.Ack {
				return errors.New("RabbitMQ negatively acknowledged publish")
			}
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("RabbitMQ confirmation interrupted; delivery may be uncertain: %w", err)
			}
			return nil
		case <-closed:
			return errors.New("RabbitMQ publisher closed; delivery may be uncertain")
		}
	}
}

func (p *rabbitPublisher) Close() error {
	return p.transport.bounded(context.Background(), "publisher close", p.channel.Close)
}

func completionIndex(body []byte, m Manifest) (int, error) {
	var event models.PipelineEvent
	if err := json.Unmarshal(body, &event); err != nil {
		return -1, errors.New("malformed completion payload")
	}
	parts := strings.Split(event.Payload.FileName, "/")
	if len(parts) != 2 {
		return -1, errors.New("foreign or invalid completion filename")
	}
	attributes := strings.Split(parts[1], "_")
	if len(attributes) != 6 {
		return -1, errors.New("foreign or invalid completion filename")
	}
	index, err := strconv.Atoi(attributes[1])
	if err != nil || index < 0 || index >= m.Total || event.Payload.FileName != RecordingKey(m, index) ||
		event.Operation != "event" || event.Request != "persist" ||
		len(event.Stages) != 1 || event.Stages[0] != CompletionStage(m) || event.MonitorStage == nil {
		return -1, errors.New("foreign or invalid completion payload")
	}
	identity, err := ExpectedIdentity(m, index)
	if err != nil || event.MonitorStage.OrganisationId != identity.OrganisationID.Hex() ||
		event.MonitorStage.ProjectId == nil || *event.MonitorStage.ProjectId != identity.ProjectID {
		return -1, errors.New("completion ownership snapshot does not match fixture")
	}
	return index, nil
}

func (r *rabbitTransport) Observe(ctx context.Context, m Manifest, callback func(Observation)) (result error) {
	r.mu.Lock()
	if r.deliveries == nil || r.runID != m.RunID || r.observing || callback == nil {
		r.mu.Unlock()
		return errors.New("RabbitMQ completion consumer is not ready or is already observing")
	}
	r.observing = true
	ch, deliveries := r.consumer, r.deliveries
	r.mu.Unlock()
	defer func() {
		// Closing the channel also requeues any deliveries prefetched but not
		// yet acknowledged, unlike cancelling the consumer alone.
		result = errors.Join(result, r.bounded(context.Background(), "completion consumer close", ch.Close))
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("RabbitMQ completion delivery stream closed unexpectedly")
			}
			if err := r.observeDelivery(delivery, m, callback); err != nil {
				return err
			}
		}
	}
}

func (r *rabbitTransport) observeDelivery(delivery amqp.Delivery, m Manifest, callback func(Observation)) error {
	index, err := completionIndex(delivery.Body, m)
	callback(Observation{Index: index, ReceivedAt: time.Now()})
	// The callback may complete the run and cancel observation. Always settle
	// this delivered message afterwards, with a separate bounded context.
	if err != nil {
		nackErr := r.bounded(context.Background(), "invalid completion requeue", func() error { return delivery.Nack(false, true) })
		return errors.Join(err, nackErr)
	}
	return r.bounded(context.Background(), "completion acknowledgment", func() error { return delivery.Ack(false) })
}

func (r *rabbitTransport) DLQDepth(ctx context.Context) (int, error) {
	return r.queueDepth(ctx, deadLetterQueue)
}

func (r *rabbitTransport) PendingCompletions(ctx context.Context, m Manifest) (int, error) {
	return r.queueDepth(ctx, CompletionQueue(m))
}

func (r *rabbitTransport) queueDepth(ctx context.Context, name string) (int, error) {
	depth := 0
	err := r.bounded(ctx, "queue depth inspection", func() error {
		ch, err := r.conn.Channel()
		if err != nil {
			return err
		}
		defer ch.Close()
		q, err := ch.QueueInspect(name)
		depth = q.Messages
		return err
	})
	return depth, err
}

func (r *rabbitTransport) Cleanup(ctx context.Context, m Manifest) error {
	if err := validateFixtureManifest(m); err != nil {
		return err
	}
	return r.bounded(ctx, "completion queue cleanup (must be empty and unused)", func() error {
		ch, err := r.conn.Channel()
		if err != nil {
			return err
		}
		defer ch.Close()
		q, err := ch.QueueInspect(CompletionQueue(m))
		var brokerError *amqp.Error
		if errors.As(err, &brokerError) && brokerError.Code == 404 {
			return nil
		}
		if err != nil {
			return err
		}
		if q.Messages != 0 || q.Consumers != 0 {
			return errors.New("completion queue still has deliveries or consumers")
		}
		// Conditional deletion is unsupported for some quorum-queue versions.
		// Retain the queue to preserve any late deliveries instead of racing an
		// unconditional delete against them.
		return nil
	})
}

func (r *rabbitTransport) Close() error {
	if r.conn.IsClosed() {
		return nil
	}
	if err := r.conn.CloseDeadline(time.Now().Add(r.timeout)); err != nil {
		return errors.New("RabbitMQ connection close failed")
	}
	return nil
}
