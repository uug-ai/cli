package loadtest

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/uug-ai/models/pkg/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestRabbitSafeDiagnosticPreservesLocalCausesOnly(t *testing.T) {
	r := &rabbitTransport{timeout: time.Second}
	err := r.bounded(context.Background(), "setup", func() error {
		return rabbitDiagnostic("queue kcloud-monitor-queue has no consumers")
	})
	if err == nil || !strings.Contains(err.Error(), "kcloud-monitor-queue has no consumers") {
		t.Fatalf("lost safe local diagnostic: %v", err)
	}
	for _, cause := range []error{
		errors.New("amqp://user:secret@host/"),
		&amqp.Error{Code: 406, Reason: "amqp://user:secret@host/"},
	} {
		err := r.bounded(context.Background(), "setup", func() error { return cause })
		if err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("unsafe provider diagnostic: %v", err)
		}
	}
	if got := rabbitSafeCause(&amqp.Error{Code: 405}); !strings.Contains(got, "locked") {
		t.Fatalf("missing actionable lock error: %s", got)
	}
}

func TestRabbitURLSafety(t *testing.T) {
	for _, raw := range []string{
		"amqp://user:secret@localhost/loadtest_local",
		"amqps://user:secret@localhost/loadtest_local",
		"amqp://user:secret@[::1]:5672/loadtest_local",
		"amqp://user:p%40ss@localhost/%6coadtest_local",
	} {
		if err := validateRabbitURL(raw); err != nil {
			t.Errorf("safe URL rejected: %v", err)
		}
	}
	for _, raw := range []string{
		"", "localhost", "https://user:secret@localhost/loadtest_local",
		"amqp://localhost/loadtest_local", "amqp://user@localhost/loadtest_local",
		"amqp://user:@localhost/loadtest_local", "amqp://:secret@localhost/loadtest_local",
		"amqp://user:secret@/loadtest_local", "amqp://user:secret@localhost/",
		"amqp://user:secret@localhost/%2f", "amqp://user:secret@localhost",
		"amqp://user:secret@localhost/production", "amqp://user:secret@localhost/loadtest_",
		"amqp://user:secret@localhost/loadtest_local/other",
		"amqp://user:secret@localhost/loadtest_local%2fother",
		"amqp://user:secret@localhost/loadtest_local?heartbeat=0",
		"amqp://user:secret@localhost/loadtest_local?",
		"amqp://user:secret@localhost/loadtest_local#secret",
		"amqp://user:secret@localhost/loadtest_local#",
		"amqp://user:secret@localhost:bad/loadtest_local",
		"amqp://user:secret@localhost/%ZZ",
	} {
		err := validateRabbitURL(raw)
		if err == nil {
			t.Error("unsafe URL accepted")
		} else if strings.Contains(err.Error(), "secret") {
			t.Error("URL validation leaked credentials")
		}
	}
}

func TestRabbitDefaultVhostRequiresBothAcknowledgments(t *testing.T) {
	for _, path := range []string{"/", "/%2f", "/%2F"} {
		for _, allow := range []bool{false, true} {
			for _, confirm := range []bool{false, true} {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, err := NewRabbitTransport(ctx, Config{
					RabbitURL:         "amqp://user:secret@localhost" + path,
					OperationTimeout:  time.Second,
					AllowDefaultNames: allow,
					ConfirmTestTarget: confirm,
				})
				if err == nil {
					t.Fatal("cancelled constructor unexpectedly succeeded")
				}
				if errors.Is(err, context.Canceled) != (allow && confirm) {
					t.Fatalf("root guard allow=%t confirm=%t: %v", allow, confirm, err)
				}
			}
		}
	}
	for _, raw := range []string{
		"amqp://user:secret@localhost/production",
		"amqp://user:secret@localhost/?heartbeat=0",
		"amqp://user:secret@localhost/#ignored",
		"amqp://localhost/",
		"amqp://user:secret@localhost",
	} {
		if err := validateRabbitURLTarget(raw, true); err == nil {
			t.Fatal("default-name exception weakened unrelated URL guard")
		}
	}
}

func TestRabbitConstructorDoesNotDialCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := NewRabbitTransport(ctx, Config{
		RabbitURL:        "amqp://user:secret@localhost/loadtest_local",
		OperationTimeout: time.Second,
	})
	if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("expected redacted cancellation: %v", err)
	}
}

func TestAwaitRabbitConfirmation(t *testing.T) {
	for _, name := range []string{"ack", "nack", "returned", "returned-and-ack", "wrong-tag", "closed", "closed-confirms", "closed-returns", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			for attempt := 0; attempt < 50; attempt++ {
				confirms := make(chan amqp.Confirmation, 1)
				returns := make(chan amqp.Return, 1)
				closed := make(chan *amqp.Error, 1)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				switch name {
				case "ack":
					confirms <- amqp.Confirmation{DeliveryTag: 1, Ack: true}
				case "nack":
					confirms <- amqp.Confirmation{DeliveryTag: 1, Ack: false}
				case "returned":
					returns <- amqp.Return{}
				case "returned-and-ack":
					returns <- amqp.Return{}
					confirms <- amqp.Confirmation{DeliveryTag: 1, Ack: true}
				case "wrong-tag":
					confirms <- amqp.Confirmation{DeliveryTag: 2, Ack: true}
				case "closed":
					close(closed)
				case "closed-confirms":
					close(confirms)
				case "closed-returns":
					close(returns)
				case "cancelled":
					cancel()
				}
				err := awaitConfirmation(ctx, 1, confirms, returns, closed)
				cancel()
				if (err == nil) != (name == "ack") {
					t.Fatalf("unexpected result: %v", err)
				}
				if name == "cancelled" && !errors.Is(err, context.Canceled) {
					t.Fatalf("missing cancellation: %v", err)
				}
			}
		})
	}
}

type fakePublishChannel struct {
	calls   int
	publish func(context.Context, string, string, bool, bool, amqp.Publishing) error
}

func (f *fakePublishChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	f.calls++
	return f.publish(ctx, exchange, key, mandatory, immediate, msg)
}

func (f *fakePublishChannel) Close() error { return nil }

func TestRabbitPublisherPersistentMandatoryAndNoRetry(t *testing.T) {
	confirms := make(chan amqp.Confirmation, 1)
	ch := &fakePublishChannel{publish: func(_ context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
		if exchange != "" || key != EventQueue || !mandatory || immediate ||
			msg.DeliveryMode != amqp.Persistent || msg.ContentType != "application/json" || string(msg.Body) != "{}" {
			t.Error("publisher did not use persistent mandatory event routing")
		}
		confirms <- amqp.Confirmation{DeliveryTag: 1, Ack: true}
		return nil
	}}
	p := &rabbitPublisher{
		transport: &rabbitTransport{timeout: time.Second},
		channel:   ch, confirms: confirms, returns: make(chan amqp.Return, 1), closed: make(chan *amqp.Error, 1), next: 1,
	}
	if err := p.Publish(context.Background(), []byte("{}")); err != nil {
		t.Fatal(err)
	}
	ch.publish = func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		return errors.New("secret URL from driver")
	}
	if err := p.Publish(context.Background(), nil); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("publish failure not safely surfaced: %v", err)
	}
	if err := p.Publish(context.Background(), nil); err == nil || ch.calls != 2 {
		t.Fatal("publisher retried uncertain delivery")
	}
}

func TestRabbitPublisherConfirmationTimeoutClosesSocket(t *testing.T) {
	socket, peer := net.Pipe()
	defer socket.Close()
	defer peer.Close()
	ch := &fakePublishChannel{publish: func(context.Context, string, string, bool, bool, amqp.Publishing) error {
		return nil
	}}
	p := &rabbitPublisher{
		transport: &rabbitTransport{socket: socket, timeout: 20 * time.Millisecond},
		channel:   ch, confirms: make(chan amqp.Confirmation), returns: make(chan amqp.Return), closed: make(chan *amqp.Error), next: 1,
	}
	err := p.Publish(context.Background(), []byte("{}"))
	if !errors.Is(err, context.DeadlineExceeded) || !p.failed {
		t.Fatalf("unconfirmed publish was not marked uncertain: %v", err)
	}
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := peer.Read(make([]byte, 1)); err == nil {
		t.Fatal("confirmation timeout did not close socket")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("socket remained open after confirmation timeout")
	}
}

func TestRabbitBoundedWriteClosesSocket(t *testing.T) {
	socket, peer := net.Pipe()
	defer peer.Close()
	defer socket.Close()
	r := &rabbitTransport{socket: socket, timeout: 20 * time.Millisecond}
	start := time.Now()
	err := r.bounded(context.Background(), "test write", func() error {
		_, err := socket.Write([]byte("blocked"))
		return err
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected bounded write timeout, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("write did not respect timeout")
	}
}

func rabbitTestManifest() Manifest {
	return Manifest{
		Schema: Schema, RunID: "rabbit-test", Timestamp: 1700000000,
		Organisations: 2, ProjectsPerOrganisation: 2, DevicesPerProject: 2, Total: 20,
	}
}

func rabbitCompletion(t *testing.T, m Manifest, index int) models.PipelineEvent {
	t.Helper()
	event, err := BuildEvent(m, index)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := ExpectedIdentity(m, index)
	if err != nil {
		t.Fatal(err)
	}
	event.Stages = []string{CompletionStage(m)}
	event.Operation = "event"
	event.MonitorStage = &models.MonitorStage{OrganisationId: identity.OrganisationID.Hex(), ProjectId: &identity.ProjectID}
	return event
}

func TestRabbitCompletionValidation(t *testing.T) {
	m := rabbitTestManifest()
	for _, index := range []int{0, 1, 7, 19} {
		event := rabbitCompletion(t, m, index)
		body, _ := json.Marshal(event)
		got, err := completionIndex(body, m)
		if err != nil || got != index {
			t.Fatalf("completion %d got %d: %v", index, got, err)
		}

	}
	for name, mutate := range map[string]func(*models.PipelineEvent){
		"wrong operation":        func(e *models.PipelineEvent) { e.Operation = "delete" },
		"wrong create operation": func(e *models.PipelineEvent) { e.Operation = "create" },
		"wrong request":          func(e *models.PipelineEvent) { e.Request = "ondemand" },
		"extra stage":            func(e *models.PipelineEvent) { e.Stages = append(e.Stages, "analysis") },
		"wrong stage":            func(e *models.PipelineEvent) { e.Stages[0] = "loadtest-other" },
		"missing monitor":        func(e *models.PipelineEvent) { e.MonitorStage = nil },
		"missing project":        func(e *models.PipelineEvent) { e.MonitorStage.ProjectId = nil },
		"wrong project":          func(e *models.PipelineEvent) { id := primitive.NewObjectID(); e.MonitorStage.ProjectId = &id },
		"wrong organisation":     func(e *models.PipelineEvent) { e.MonitorStage.OrganisationId = primitive.NewObjectID().Hex() },
		"wrong filename":         func(e *models.PipelineEvent) { e.Payload.FileName += ".other" },
		"foreign run": func(e *models.PipelineEvent) {
			e.Payload.FileName = strings.Replace(e.Payload.FileName, m.RunID, "other", 1)
		},
		"wrong index": func(e *models.PipelineEvent) {
			e.Payload.FileName = strings.Replace(e.Payload.FileName, "_1_", "_999_", 1)
		},
		"invalid index": func(e *models.PipelineEvent) {
			e.Payload.FileName = strings.Replace(e.Payload.FileName, "_1_", "_bad_", 1)
		},
		"missing filename": func(e *models.PipelineEvent) { e.Payload.FileName = "" },
	} {
		t.Run(name, func(t *testing.T) {
			event := rabbitCompletion(t, m, 1)
			mutate(&event)
			body, _ := json.Marshal(event)
			if got, err := completionIndex(body, m); err == nil || got != -1 {
				t.Fatalf("invalid completion accepted: %d, %v", got, err)
			}
		})
	}
	if got, err := completionIndex([]byte("{"), m); err == nil || got != -1 {
		t.Fatal("malformed JSON accepted")
	}
}

type completionAcknowledger struct {
	ack  func(uint64, bool) error
	nack func(uint64, bool, bool) error
}

func (a completionAcknowledger) Ack(tag uint64, multiple bool) error {
	return a.ack(tag, multiple)
}
func (a completionAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	return a.nack(tag, multiple, requeue)
}
func (a completionAcknowledger) Reject(uint64, bool) error { panic("unexpected reject") }

func TestRabbitObservationsSettleAfterCallbackAndPreserveInvalid(t *testing.T) {
	m := rabbitTestManifest()
	valid, _ := json.Marshal(rabbitCompletion(t, m, 1))
	for _, body := range [][]byte{valid, []byte("{")} {
		validMessage := string(body) == string(valid)
		called, acknowledged, requeued := 0, 0, 0
		delivery := amqp.Delivery{
			Body: body, DeliveryTag: 7,
			Acknowledger: completionAcknowledger{
				ack: func(tag uint64, multiple bool) error {
					if !validMessage || called != acknowledged+1 || tag != 7 || multiple {
						t.Error("ack happened before callback or acknowledged invalid data")
					}
					acknowledged++
					return nil
				},
				nack: func(tag uint64, multiple, requeue bool) error {
					if validMessage || called == 0 || tag != 7 || multiple || !requeue {
						t.Error("invalid delivery was not preserved after callback")
					}
					requeued++
					return nil
				},
			},
		}
		r := &rabbitTransport{timeout: time.Second}
		callback := func(o Observation) {
			called++
			if o.ReceivedAt.IsZero() || (validMessage && o.Index != 1) || (!validMessage && o.Index != -1) {
				t.Error("incorrect completion observation")
			}
		}
		// Duplicates must reach the callback so the runner can count them.
		for i := 0; i < 2; i++ {
			err := r.observeDelivery(delivery, m, callback)
			if (err == nil) != validMessage {
				t.Fatalf("unexpected settlement result: %v", err)
			}
		}
		if called != 2 || (validMessage && acknowledged != 2) || (!validMessage && requeued != 2) {
			t.Fatal("duplicate delivery was silently discarded")
		}
	}
}
