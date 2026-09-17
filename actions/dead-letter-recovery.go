package actions

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"text/tabwriter"

	sharedqueue "github.com/uug-ai/queue/pkg/queue"
)

const (
	maxRecoveryMessages   = 1_000_000
	maxVaultResponseBytes = 32 << 20
)

type vaultMediaURLRequest struct {
	Provider      string `json:"provider"`
	Filename      string `json:"filename"`
	URIExpiryTime string `json:"uriExpiryTime,omitempty"`
}

type vaultURLRefresher interface {
	RefreshSignedURLs(context.Context, []vaultMediaURLRequest) (map[string]string, error)
}

type vaultHTTPURLRefresher struct {
	endpoint  string
	accessKey string
	secret    string
	client    *http.Client
}

type pipelineRecoveryMessage struct {
	message  sharedqueue.DeadLetterMessage
	root     map[string]json.RawMessage
	payload  map[string]json.RawMessage
	fileName string
	provider string
	stage    string
	onDemand bool
}

type pipelineRecoveryRequest struct {
	request vaultMediaURLRequest
	indexes []int
}

type deadLetterRecoveryResult struct {
	Replay     sharedqueue.DeadLetterReplayResult
	Batches    int
	Candidates int
	Refreshed  int
	Bypassed   int
	ByStage    map[string]int
}

func validateRecoveryConfig(config dlqCommandConfig) error {
	if config.destination == "" {
		return fmt.Errorf("--destination is required so recovered events are sent through the pipeline router")
	}
	if config.limit < 1 || config.limit > maxRecoveryMessages {
		return fmt.Errorf("--limit must be between 1 and %d", maxRecoveryMessages)
	}
	if config.batchSize < 1 || config.batchSize > 10000 {
		return fmt.Errorf("--batch-size must be between 1 and 10000")
	}
	if config.batchDelay < 0 {
		return fmt.Errorf("--batch-delay cannot be negative")
	}
	if config.execute {
		if config.vaultURI == "" {
			return fmt.Errorf("--vault-uri is required with --execute")
		}
		if config.vaultAccessKey == "" {
			return fmt.Errorf("--vault-access-key is required with --execute")
		}
		if config.vaultSecret == "" {
			return fmt.Errorf("--vault-secret is required with --execute")
		}
	}
	return nil
}

func newVaultHTTPURLRefresher(baseURI, accessKey, secret string, allowInsecureHTTP bool, client *http.Client) (*vaultHTTPURLRefresher, error) {
	endpoint, err := vaultBulkEndpoint(baseURI, allowInsecureHTTP)
	if err != nil {
		return nil, err
	}
	if accessKey == "" {
		return nil, fmt.Errorf("Vault access key is required")
	}
	if secret == "" {
		return nil, fmt.Errorf("Vault secret is required")
	}
	if client == nil {
		client = &http.Client{}
	}
	safeClient := *client
	safeClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &vaultHTTPURLRefresher{
		endpoint:  endpoint,
		accessKey: accessKey,
		secret:    secret,
		client:    &safeClient,
	}, nil
}

func vaultBulkEndpoint(baseURI string, allowInsecureHTTP bool) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURI))
	if err != nil {
		return "", fmt.Errorf("parse Vault URI: %w", err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", fmt.Errorf("Vault URI must be an absolute HTTP or HTTPS URL")
	}
	if parsed.User != nil {
		return "", fmt.Errorf("Vault URI must not contain user information")
	}
	if parsed.Scheme == "http" && !allowInsecureHTTP && !isLoopbackHost(parsed.Hostname()) {
		return "", fmt.Errorf("Vault URI must use HTTPS unless --vault-allow-insecure-http is set")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("Vault URI must not contain a query or fragment")
	}
	path := strings.TrimRight(parsed.Path, "/")
	switch {
	case strings.HasSuffix(path, "/api/storage/bulk"):
	case strings.HasSuffix(path, "/api/storage"):
		path += "/bulk"
	case strings.HasSuffix(path, "/api"):
		path += "/storage/bulk"
	default:
		path += "/api/storage/bulk"
	}
	parsed.Path = path
	return parsed.String(), nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (v *vaultHTTPURLRefresher) RefreshSignedURLs(ctx context.Context, media []vaultMediaURLRequest) (map[string]string, error) {
	body, err := json.Marshal(media)
	if err != nil {
		return nil, fmt.Errorf("encode Vault bulk URL request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, v.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create Vault bulk URL request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Kerberos-Storage-AccessKey", v.accessKey)
	request.Header.Set("X-Kerberos-Storage-SecretAccessKey", v.secret)

	response, err := v.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("request Vault bulk URLs: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("Vault bulk URL request returned HTTP %d", response.StatusCode)
	}
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxVaultResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read Vault bulk URL response: %w", err)
	}
	if len(responseBody) > maxVaultResponseBytes {
		return nil, fmt.Errorf("Vault bulk URL response exceeds %d bytes", maxVaultResponseBytes)
	}
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(responseBody, &envelope); err != nil {
		return nil, fmt.Errorf("decode Vault bulk URL response: %w", err)
	}
	var urls map[string]string
	var encoded string
	if err := json.Unmarshal(envelope.Data, &encoded); err == nil {
		if err := json.Unmarshal([]byte(encoded), &urls); err != nil {
			return nil, fmt.Errorf("decode Vault bulk URL map: %w", err)
		}
	} else if err := json.Unmarshal(envelope.Data, &urls); err != nil {
		return nil, fmt.Errorf("decode Vault bulk URL map: %w", err)
	}
	if urls == nil {
		return nil, fmt.Errorf("Vault bulk URL response has no URL map")
	}
	return urls, nil
}

func transformPipelineRecoveryBatch(
	ctx context.Context,
	messages []sharedqueue.DeadLetterMessage,
	execute bool,
	fallbackProvider string,
	expiry string,
	refresher vaultURLRefresher,
	result *deadLetterRecoveryResult,
) ([]sharedqueue.DeadLetterReplayTransformation, error) {
	transformations := make([]sharedqueue.DeadLetterReplayTransformation, len(messages))
	parsed := make([]*pipelineRecoveryMessage, len(messages))
	requestByFile := make(map[string]*pipelineRecoveryRequest)
	conflictingFiles := make(map[string]struct{})
	for index, message := range messages {
		recoveryMessage, err := parsePipelineRecoveryMessage(message, fallbackProvider)
		if err != nil {
			transformations[index].Skip = true
			continue
		}
		parsed[index] = &recoveryMessage

		if recoveryMessage.onDemand {
			continue
		}
		request := vaultMediaURLRequest{
			Provider:      recoveryMessage.provider,
			Filename:      recoveryMessage.fileName,
			URIExpiryTime: expiry,
		}
		if existing, ok := requestByFile[request.Filename]; ok {
			existing.indexes = append(existing.indexes, index)
			if existing.request.Provider != request.Provider || existing.request.URIExpiryTime != request.URIExpiryTime {
				conflictingFiles[request.Filename] = struct{}{}
			}
			continue
		}
		requestByFile[request.Filename] = &pipelineRecoveryRequest{
			request: request,
			indexes: []int{index},
		}
	}

	requests := make([]vaultMediaURLRequest, 0, len(requestByFile))
	for fileName, grouped := range requestByFile {
		if _, conflict := conflictingFiles[fileName]; conflict {
			for _, index := range grouped.indexes {
				transformations[index].Skip = true
				parsed[index] = nil
			}
			continue
		}
		requests = append(requests, grouped.request)
	}
	sort.Slice(requests, func(left, right int) bool {
		return requests[left].Filename < requests[right].Filename
	})

	for index, recoveryMessage := range parsed {
		if recoveryMessage == nil {
			continue
		}
		result.Candidates++
		if result.ByStage == nil {
			result.ByStage = make(map[string]int)
		}
		result.ByStage[recoveryMessage.stage]++
		if !execute || recoveryMessage.onDemand {
			transformations[index].Payload = append([]byte(nil), messages[index].Payload...)
		}
		if recoveryMessage.onDemand {
			result.Bypassed++
		}
	}
	if !execute || len(requests) == 0 {
		return transformations, nil
	}
	if refresher == nil {
		return nil, fmt.Errorf("Vault URL refresher is required for executed recovery")
	}
	urls, err := refresher.RefreshSignedURLs(ctx, requests)
	if err != nil {
		return nil, err
	}

	for index, recoveryMessage := range parsed {
		if recoveryMessage == nil || recoveryMessage.onDemand {
			continue
		}
		signedURL := strings.TrimSpace(urls[recoveryMessage.fileName])
		if signedURL == "" {
			return nil, fmt.Errorf("Vault returned no signed URL for dead-letter message %q file %q", recoveryMessage.message.ID, recoveryMessage.fileName)
		}
		payload, err := replacePipelineSignedURL(*recoveryMessage, signedURL)
		if err != nil {
			return nil, err
		}
		transformations[index].Payload = payload
		result.Refreshed++
	}
	return transformations, nil
}

func parsePipelineRecoveryMessage(message sharedqueue.DeadLetterMessage, fallbackProvider string) (pipelineRecoveryMessage, error) {
	var projection struct {
		Request  string   `json:"request"`
		Stages   []string `json:"events"`
		Provider string   `json:"source"`
		Storage  string   `json:"provider"`
		Payload  struct {
			FileName string `json:"key"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(message.Payload, &projection); err != nil {
		return pipelineRecoveryMessage{}, fmt.Errorf("decode pipeline event for dead-letter message %q: %w", message.ID, err)
	}
	if len(projection.Stages) == 0 || strings.TrimSpace(projection.Stages[0]) == "" {
		return pipelineRecoveryMessage{}, fmt.Errorf("dead-letter message %q has no current pipeline stage", message.ID)
	}
	fileName := strings.TrimSpace(projection.Payload.FileName)
	if fileName == "" {
		return pipelineRecoveryMessage{}, fmt.Errorf("dead-letter message %q has no payload file key", message.ID)
	}

	var root map[string]json.RawMessage
	if err := json.Unmarshal(message.Payload, &root); err != nil {
		return pipelineRecoveryMessage{}, fmt.Errorf("decode pipeline event object for dead-letter message %q: %w", message.ID, err)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(root["payload"], &payload); err != nil || payload == nil {
		if err == nil {
			err = fmt.Errorf("payload is not an object")
		}
		return pipelineRecoveryMessage{}, fmt.Errorf("decode pipeline payload for dead-letter message %q: %w", message.ID, err)
	}
	provider := strings.TrimSpace(projection.Provider)
	if provider == "" {
		provider = strings.TrimSpace(projection.Storage)
	}
	if provider == "" {
		provider = strings.TrimSpace(fallbackProvider)
	}
	return pipelineRecoveryMessage{
		message:  message,
		root:     root,
		payload:  payload,
		fileName: fileName,
		provider: provider,
		stage:    strings.TrimSpace(projection.Stages[0]),
		onDemand: projection.Request == "ondemand",
	}, nil
}

func replacePipelineSignedURL(message pipelineRecoveryMessage, signedURL string) ([]byte, error) {
	encodedURL, err := json.Marshal(signedURL)
	if err != nil {
		return nil, fmt.Errorf("encode signed URL for dead-letter message %q: %w", message.message.ID, err)
	}
	message.payload["signedUrl"] = encodedURL
	encodedPayload, err := json.Marshal(message.payload)
	if err != nil {
		return nil, fmt.Errorf("encode pipeline payload for dead-letter message %q: %w", message.message.ID, err)
	}
	message.root["payload"] = encodedPayload
	encodedEvent, err := json.Marshal(message.root)
	if err != nil {
		return nil, fmt.Errorf("encode pipeline event for dead-letter message %q: %w", message.message.ID, err)
	}
	return encodedEvent, nil
}

func recoverDeadLetters(ctx context.Context, admin sharedqueue.DeadLetterAdmin, refresher vaultURLRefresher, config dlqCommandConfig) (deadLetterRecoveryResult, error) {
	result := deadLetterRecoveryResult{ByStage: make(map[string]int)}
	batchResult, err := admin.ReplayDeadLetters(ctx, sharedqueue.DeadLetterReplayRequest{
		Limit:        config.limit,
		BatchSize:    config.batchSize,
		BatchDelay:   config.batchDelay,
		BatchTimeout: config.timeout,
		Source:       config.source,
		Destination:  config.destination,
		Execute:      config.execute,
		IdleTimeout:  config.idleTimeout,
		Transform: func(transformCtx context.Context, messages []sharedqueue.DeadLetterMessage) ([]sharedqueue.DeadLetterReplayTransformation, error) {
			result.Batches++
			return transformPipelineRecoveryBatch(
				transformCtx,
				messages,
				config.execute,
				config.vaultProvider,
				config.vaultURLExpiry,
				refresher,
				&result,
			)
		},
	})
	result.Replay = batchResult
	if err != nil {
		return result, err
	}
	return result, nil
}

func printRecovery(output io.Writer, result deadLetterRecoveryResult, execute bool) {
	mode := "dry-run"
	if execute {
		mode = "executed"
	}
	fmt.Fprintf(output, "Mode: %s\nBatches: %d\n", mode, result.Batches)
	stages := make([]string, 0, len(result.ByStage))
	for stage := range result.ByStage {
		stages = append(stages, stage)
	}
	sort.Strings(stages)
	if len(stages) > 0 {
		writer := tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
		fmt.Fprintln(writer, "CURRENT STAGE\tMESSAGES")
		for _, stage := range stages {
			fmt.Fprintf(writer, "%s\t%d\n", stage, result.ByStage[stage])
		}
		writer.Flush()
	}
	printReplayDestinations(output, result.Replay.Destinations)
	fmt.Fprintf(output, "Scanned: %d\nMatched: %d\nPlanned: %d\nRecovery candidates: %d\nURL refresh bypassed: %d\nURLs refreshed: %d\nUnrecoverable: %d\nReplayed: %d\nRetained: %d\nLegacy/unknown: %d\nUnroutable: %d\n",
		result.Replay.Scanned,
		result.Replay.Matched,
		result.Replay.Planned,
		result.Candidates,
		result.Bypassed,
		result.Refreshed,
		result.Replay.Skipped,
		result.Replay.Replayed,
		result.Replay.Retained,
		result.Replay.Legacy,
		result.Replay.Unroutable,
	)
	if !execute {
		fmt.Fprintln(output, "No URLs were requested and no messages were moved. Pass --execute to recover.")
	}
}
