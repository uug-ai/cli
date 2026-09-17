package actions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	sharedqueue "github.com/uug-ai/queue/pkg/queue"
)

const (
	maxRecoveryMessages          = 1_000_000
	maxVaultResponseBytes        = 32 << 20
	maxPipelineRecoveryStages    = 64
	defaultHistoricalTailMaxAge  = 15 * time.Minute
	pipelineObjectIDHexLength    = 24
	pipelineRecoveryUnknownError = "invalid-event"
)

var pipelineStageNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

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
	changed  bool

	auditRemoved   bool
	tailSuppressed bool
}

type pipelineRecoveryRequest struct {
	request vaultMediaURLRequest
	indexes []int
}

type deadLetterRecoveryResult struct {
	Replay         sharedqueue.DeadLetterReplayResult
	Batches        int
	Candidates     int
	Refreshed      int
	Bypassed       int
	AuditRemoved   int
	TailSuppressed int
	ByStage        map[string]int
	ByFailure      map[string]int
}

type pipelineRecoverySafetyPolicy struct {
	historicalTailMaxAge time.Duration
	allowHistoricalTail  bool
	legacyUserOwnership  bool
}

type pipelineRecoveryValidationError struct {
	reason string
	err    error
}

func (e *pipelineRecoveryValidationError) Error() string {
	return e.err.Error()
}

func (e *pipelineRecoveryValidationError) Unwrap() error {
	return e.err
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
	if config.historicalTailMaxAge < 0 {
		return fmt.Errorf("--historical-tail-max-age cannot be negative")
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
	safety pipelineRecoverySafetyPolicy,
	result *deadLetterRecoveryResult,
) ([]sharedqueue.DeadLetterReplayTransformation, error) {
	transformations := make([]sharedqueue.DeadLetterReplayTransformation, len(messages))
	parsed := make([]*pipelineRecoveryMessage, len(messages))
	requestByFile := make(map[string]*pipelineRecoveryRequest)
	conflictingFiles := make(map[string]struct{})
	for index, message := range messages {
		recoveryMessage, err := parsePipelineRecoveryMessage(message, fallbackProvider, safety)
		if err != nil {
			transformations[index].Skip = true
			if result.ByFailure == nil {
				result.ByFailure = make(map[string]int)
			}
			result.ByFailure[pipelineRecoveryFailureReason(err)]++
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
		if recoveryMessage.auditRemoved {
			result.AuditRemoved++
		}
		if recoveryMessage.tailSuppressed {
			result.TailSuppressed++
		}
		result.Candidates++
		if result.ByStage == nil {
			result.ByStage = make(map[string]int)
		}
		result.ByStage[recoveryMessage.stage]++
		if !execute {
			transformations[index].Payload = append([]byte(nil), messages[index].Payload...)
		} else if recoveryMessage.onDemand {
			if recoveryMessage.changed {
				payload, err := encodePipelineRecoveryMessage(*recoveryMessage)
				if err != nil {
					return nil, err
				}
				transformations[index].Payload = payload
			} else {
				transformations[index].Payload = append([]byte(nil), messages[index].Payload...)
			}
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
		if err := validatePipelineHTTPURL(signedURL); err != nil {
			return nil, fmt.Errorf("Vault returned an invalid signed URL for dead-letter message %q file %q: %w", recoveryMessage.message.ID, recoveryMessage.fileName, err)
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

func parsePipelineRecoveryMessage(message sharedqueue.DeadLetterMessage, fallbackProvider string, safety pipelineRecoverySafetyPolicy) (pipelineRecoveryMessage, error) {
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
		return pipelineRecoveryMessage{}, newPipelineRecoveryValidationError("invalid-json", "decode pipeline event for dead-letter message %q: %v", message.ID, err)
	}
	if projection.Request != "" && projection.Request != "persist" && projection.Request != "ondemand" {
		return pipelineRecoveryMessage{}, newPipelineRecoveryValidationError("invalid-request", "dead-letter message %q has an unsupported pipeline request", message.ID)
	}
	if err := validatePipelineStages(projection.Stages); err != nil {
		return pipelineRecoveryMessage{}, newPipelineRecoveryValidationError("invalid-stage", "dead-letter message %q: %v", message.ID, err)
	}
	fileName := strings.TrimSpace(projection.Payload.FileName)
	if fileName == "" {
		return pipelineRecoveryMessage{}, newPipelineRecoveryValidationError("missing-file-key", "dead-letter message %q has no payload file key", message.ID)
	}
	if fileName != projection.Payload.FileName {
		return pipelineRecoveryMessage{}, newPipelineRecoveryValidationError("invalid-file-key", "dead-letter message %q has an invalid payload file key", message.ID)
	}

	var root map[string]json.RawMessage
	if err := json.Unmarshal(message.Payload, &root); err != nil {
		return pipelineRecoveryMessage{}, newPipelineRecoveryValidationError("invalid-json", "decode pipeline event object for dead-letter message %q: %v", message.ID, err)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(root["payload"], &payload); err != nil || payload == nil {
		if err == nil {
			err = fmt.Errorf("payload is not an object")
		}
		return pipelineRecoveryMessage{}, newPipelineRecoveryValidationError("invalid-payload", "decode pipeline payload for dead-letter message %q: %v", message.ID, err)
	}

	recoveryMessage := pipelineRecoveryMessage{
		message:  message,
		root:     root,
		payload:  payload,
		fileName: fileName,
		stage:    projection.Stages[0],
		onDemand: projection.Request == "ondemand",
	}
	if err := sanitizePipelineMonitorSnapshot(&recoveryMessage, safety); err != nil {
		return recoveryMessage, err
	}
	if recoveryMessage.onDemand {
		if signedURL, ok, err := optionalJSONString(payload, "signedUrl"); err != nil {
			return recoveryMessage, newPipelineRecoveryValidationError("invalid-signed-url", "dead-letter message %q has a non-string signed URL", message.ID)
		} else if ok && signedURL != "" {
			if err := validatePipelineHTTPURL(signedURL); err != nil {
				return recoveryMessage, newPipelineRecoveryValidationError("invalid-signed-url", "dead-letter message %q has an invalid signed URL: %v", message.ID, err)
			}
		}
	}
	if err := suppressHistoricalPipelineTail(&recoveryMessage, projection.Stages, safety); err != nil {
		return recoveryMessage, err
	}

	provider := strings.TrimSpace(projection.Provider)
	if projection.Provider != "" && provider != projection.Provider {
		return recoveryMessage, newPipelineRecoveryValidationError("invalid-provider", "dead-letter message %q has an invalid storage provider", message.ID)
	}
	if provider == "" {
		provider = strings.TrimSpace(projection.Storage)
		if projection.Storage != "" && provider != projection.Storage {
			return recoveryMessage, newPipelineRecoveryValidationError("invalid-provider", "dead-letter message %q has an invalid storage provider", message.ID)
		}
	}
	if provider == "" {
		provider = strings.TrimSpace(fallbackProvider)
	}
	if provider == "" && !recoveryMessage.onDemand {
		return recoveryMessage, newPipelineRecoveryValidationError("missing-provider", "dead-letter message %q has no storage provider", message.ID)
	}
	recoveryMessage.provider = provider
	return recoveryMessage, nil
}

func replacePipelineSignedURL(message pipelineRecoveryMessage, signedURL string) ([]byte, error) {
	encodedURL, err := json.Marshal(signedURL)
	if err != nil {
		return nil, fmt.Errorf("encode signed URL for dead-letter message %q: %w", message.message.ID, err)
	}
	message.payload["signedUrl"] = encodedURL
	message.changed = true
	return encodePipelineRecoveryMessage(message)
}

func encodePipelineRecoveryMessage(message pipelineRecoveryMessage) ([]byte, error) {
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

func defaultPipelineRecoverySafetyPolicy() pipelineRecoverySafetyPolicy {
	return pipelineRecoverySafetyPolicy{historicalTailMaxAge: defaultHistoricalTailMaxAge}
}

func pipelineRecoverySafetyPolicyFromConfig(config dlqCommandConfig) pipelineRecoverySafetyPolicy {
	maxAge := config.historicalTailMaxAge
	if maxAge == 0 {
		maxAge = defaultHistoricalTailMaxAge
	}
	return pipelineRecoverySafetyPolicy{
		historicalTailMaxAge: maxAge,
		allowHistoricalTail:  config.allowHistoricalTail,
		legacyUserOwnership:  config.legacyUserOwnership,
	}
}

func newPipelineRecoveryValidationError(reason, format string, args ...any) error {
	return &pipelineRecoveryValidationError{
		reason: reason,
		err:    fmt.Errorf(format, args...),
	}
}

func pipelineRecoveryFailureReason(err error) string {
	var validationError *pipelineRecoveryValidationError
	if errors.As(err, &validationError) && validationError.reason != "" {
		return validationError.reason
	}
	return pipelineRecoveryUnknownError
}

func validatePipelineStages(stages []string) error {
	if len(stages) == 0 {
		return fmt.Errorf("has no current pipeline stage")
	}
	if len(stages) > maxPipelineRecoveryStages {
		return fmt.Errorf("has %d pipeline stages, maximum is %d", len(stages), maxPipelineRecoveryStages)
	}
	for _, stage := range stages {
		if !pipelineStageNamePattern.MatchString(stage) {
			return fmt.Errorf("contains an invalid pipeline stage")
		}
		if stage == "end" {
			return fmt.Errorf("contains the terminal pipeline stage")
		}
	}
	return nil
}

func sanitizePipelineMonitorSnapshot(message *pipelineRecoveryMessage, safety pipelineRecoverySafetyPolicy) error {
	monitorStage, present, err := optionalJSONObject(message.root, "monitorStage")
	if err != nil {
		return newPipelineRecoveryValidationError("invalid-monitor-stage", "dead-letter message %q has an invalid monitor stage", message.message.ID)
	}
	if !present {
		if pipelineStageRequiresMonitor(message.stage) {
			return newPipelineRecoveryValidationError("missing-monitor-stage", "dead-letter message %q has no monitor stage", message.message.ID)
		}
		return nil
	}

	user, present, err := optionalJSONObject(monitorStage, "user")
	if err != nil {
		return newPipelineRecoveryValidationError("invalid-user", "dead-letter message %q has an invalid monitor user", message.message.ID)
	}
	if !present {
		if pipelineStageRequiresMonitor(message.stage) {
			return newPipelineRecoveryValidationError("missing-user", "dead-letter message %q has no monitor user", message.message.ID)
		}
		return nil
	}

	if audit, ok := user["audit"]; ok && !isJSONNull(audit) {
		delete(user, "audit")
		message.auditRemoved = true
		message.changed = true
	}

	userID, hasUserID, err := optionalJSONString(user, "id")
	if err != nil || (pipelineStageRequiresMonitor(message.stage) && (!hasUserID || !isPipelineObjectID(userID))) {
		return newPipelineRecoveryValidationError("invalid-user-id", "dead-letter message %q has no valid monitor user ID", message.message.ID)
	}
	userID = strings.ToLower(userID)
	if pipelineStageRequiresMonitor(message.stage) {
		email, hasEmail, err := optionalJSONString(user, "email")
		if err != nil || !hasEmail || strings.TrimSpace(email) == "" {
			return newPipelineRecoveryValidationError("missing-user-email", "dead-letter message %q has no monitor user email", message.message.ID)
		}
	}

	organisationID, _, err := optionalCanonicalObjectID(monitorStage, "organisationId")
	if err != nil {
		return newPipelineRecoveryValidationError("invalid-ownership", "dead-letter message %q has an invalid canonical organisation", message.message.ID)
	}
	projectID, _, err := optionalCanonicalObjectID(monitorStage, "projectId")
	if err != nil {
		return newPipelineRecoveryValidationError("invalid-ownership", "dead-letter message %q has an invalid canonical project", message.message.ID)
	}
	if safety.legacyUserOwnership && hasUserID &&
		((organisationID != "" && organisationID != userID) || (projectID != "" && projectID != userID)) {
		return newPipelineRecoveryValidationError("legacy-owner-conflict", "dead-letter message %q cannot be safely processed by legacy workers that scope ownership from the monitor user", message.message.ID)
	}

	for _, field := range []string{"storage", "archive_storage"} {
		storage, present, err := optionalJSONObject(user, field)
		if err != nil {
			return newPipelineRecoveryValidationError("invalid-storage", "dead-letter message %q has invalid embedded storage configuration", message.message.ID)
		}
		if !present {
			continue
		}
		uri, present, err := optionalJSONString(storage, "uri")
		if err != nil {
			return newPipelineRecoveryValidationError("invalid-storage-uri", "dead-letter message %q has a non-string embedded storage URI", message.message.ID)
		}
		if present && strings.TrimSpace(uri) != "" {
			if err := validatePipelineHTTPURL(uri); err != nil {
				return newPipelineRecoveryValidationError("invalid-storage-uri", "dead-letter message %q has an invalid embedded storage URI: %v", message.message.ID, err)
			}
		}
	}

	if message.auditRemoved {
		encodedUser, err := json.Marshal(user)
		if err != nil {
			return fmt.Errorf("encode sanitized monitor user for dead-letter message %q: %w", message.message.ID, err)
		}
		monitorStage["user"] = encodedUser
		encodedMonitorStage, err := json.Marshal(monitorStage)
		if err != nil {
			return fmt.Errorf("encode sanitized monitor stage for dead-letter message %q: %w", message.message.ID, err)
		}
		message.root["monitorStage"] = encodedMonitorStage
	}
	return nil
}

func suppressHistoricalPipelineTail(message *pipelineRecoveryMessage, stages []string, safety pipelineRecoverySafetyPolicy) error {
	if safety.allowHistoricalTail || !containsPipelineTailStage(stages) {
		return nil
	}
	maxAge := safety.historicalTailMaxAge
	if maxAge == 0 {
		maxAge = defaultHistoricalTailMaxAge
	}
	timestamp, err := pipelineMediaTimestamp(message.payload, message.root)
	if err != nil {
		return newPipelineRecoveryValidationError("invalid-media-timestamp", "dead-letter message %q has no valid media timestamp for safe tail recovery", message.message.ID)
	}
	if !time.Unix(timestamp, 0).Before(time.Now().Add(-maxAge)) {
		return nil
	}
	if isPipelineTailStage(stages[0]) {
		return newPipelineRecoveryValidationError("historical-tail-current", "dead-letter message %q is already at an unsafe historical tail stage", message.message.ID)
	}

	filtered := make([]string, 0, len(stages))
	for _, stage := range stages {
		if !isPipelineTailStage(stage) {
			filtered = append(filtered, stage)
		}
	}
	if len(filtered) == 0 {
		return newPipelineRecoveryValidationError("historical-tail-only", "dead-letter message %q has no safe stages remaining", message.message.ID)
	}
	encodedStages, err := json.Marshal(filtered)
	if err != nil {
		return fmt.Errorf("encode safe pipeline stages for dead-letter message %q: %w", message.message.ID, err)
	}
	message.root["events"] = encodedStages
	message.stage = filtered[0]
	message.tailSuppressed = true
	message.changed = true
	return nil
}

func pipelineMediaTimestamp(payload, root map[string]json.RawMessage) (int64, error) {
	metadata, present, err := optionalJSONObject(payload, "metadata")
	if err != nil {
		return 0, err
	}
	if present {
		deviceID, hasDeviceID, err := optionalJSONString(metadata, "productid")
		if err != nil {
			return 0, err
		}
		if hasDeviceID && deviceID != "" {
			raw, ok := metadata["event-timestamp"]
			if !ok || isJSONNull(raw) {
				return 0, fmt.Errorf("structured media timestamp is unavailable")
			}
			return parsePipelineUnixTimestamp(raw)
		}
	}

	fileName, ok, err := optionalJSONString(payload, "key")
	if err == nil && ok {
		baseName := fileName
		if slash := strings.LastIndex(baseName, "/"); slash >= 0 {
			baseName = baseName[slash+1:]
		}
		timestampText := strings.SplitN(baseName, "_", 2)[0]
		if timestamp, parseErr := strconv.ParseInt(timestampText, 10, 64); parseErr == nil && timestamp > 0 {
			return timestamp, nil
		}
	}

	for _, candidate := range []struct {
		object map[string]json.RawMessage
		field  string
	}{
		{payload, "timestamp"},
		{root, "date"},
	} {
		if raw, ok := candidate.object[candidate.field]; ok && !isJSONNull(raw) {
			return parsePipelineUnixTimestamp(raw)
		}
	}
	return 0, fmt.Errorf("media timestamp is unavailable")
}

func parsePipelineUnixTimestamp(raw json.RawMessage) (int64, error) {
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		var number json.Number
		if numberErr := json.Unmarshal(raw, &number); numberErr != nil {
			return 0, fmt.Errorf("media timestamp is not numeric")
		}
		text = number.String()
	}
	if text != strings.TrimSpace(text) {
		return 0, fmt.Errorf("media timestamp is invalid")
	}
	timestamp, err := strconv.ParseInt(text, 10, 64)
	if err != nil || timestamp <= 0 {
		return 0, fmt.Errorf("media timestamp is invalid")
	}
	return timestamp, nil
}

func optionalJSONObject(object map[string]json.RawMessage, field string) (map[string]json.RawMessage, bool, error) {
	raw, ok := object[field]
	if !ok || isJSONNull(raw) {
		return nil, false, nil
	}
	var value map[string]json.RawMessage
	if err := json.Unmarshal(raw, &value); err != nil || value == nil {
		return nil, false, fmt.Errorf("%s is not an object", field)
	}
	return value, true, nil
}

func optionalJSONString(object map[string]json.RawMessage, field string) (string, bool, error) {
	raw, ok := object[field]
	if !ok || isJSONNull(raw) {
		return "", false, nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", false, err
	}
	return value, true, nil
}

func optionalCanonicalObjectID(object map[string]json.RawMessage, field string) (string, bool, error) {
	value, present, err := optionalJSONString(object, field)
	if err != nil || !present || value == "" || value == strings.Repeat("0", pipelineObjectIDHexLength) {
		return "", false, err
	}
	if !isPipelineObjectID(value) {
		return "", false, fmt.Errorf("%s is not an ObjectID", field)
	}
	return strings.ToLower(value), true, nil
}

func isPipelineObjectID(value string) bool {
	if len(value) != pipelineObjectIDHexLength {
		return false
	}
	for _, character := range value {
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f') || (character >= 'A' && character <= 'F')) {
			return false
		}
	}
	return true
}

func validatePipelineHTTPURL(value string) error {
	if value != strings.TrimSpace(value) {
		return fmt.Errorf("must not contain surrounding whitespace")
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("must be an absolute HTTP or HTTPS URL")
	}
	if parsed.User != nil {
		return fmt.Errorf("must not contain user information")
	}
	return nil
}

func pipelineStageRequiresMonitor(stage string) bool {
	switch stage {
	case "sequence", "analysis", "throttler", "notification":
		return true
	default:
		return false
	}

}

func containsPipelineTailStage(stages []string) bool {
	for _, stage := range stages {
		if isPipelineTailStage(stage) {
			return true
		}
	}
	return false
}

func isPipelineTailStage(stage string) bool {
	return stage == "throttler" || stage == "notification"
}

func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func recoverDeadLetters(ctx context.Context, admin sharedqueue.DeadLetterAdmin, refresher vaultURLRefresher, config dlqCommandConfig) (deadLetterRecoveryResult, error) {
	result := deadLetterRecoveryResult{ByStage: make(map[string]int)}
	safety := pipelineRecoverySafetyPolicyFromConfig(config)
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
				safety,
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
	failures := make([]string, 0, len(result.ByFailure))
	for reason := range result.ByFailure {
		failures = append(failures, reason)
	}
	sort.Strings(failures)
	if len(failures) > 0 {
		writer := tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
		fmt.Fprintln(writer, "UNRECOVERABLE REASON\tMESSAGES")
		for _, reason := range failures {
			fmt.Fprintf(writer, "%s\t%d\n", reason, result.ByFailure[reason])
		}
		writer.Flush()
	}
	printReplayDestinations(output, result.Replay.Destinations)
	fmt.Fprintf(output, "Scanned: %d\nMatched: %d\nPlanned: %d\nRecovery candidates: %d\nLegacy user audit sanitizations: %d\nHistorical tail suppressions: %d\nURL refresh bypassed: %d\nURLs refreshed: %d\nUnrecoverable: %d\nReplayed: %d\nRetained: %d\nLegacy/unknown: %d\nUnroutable: %d\n",
		result.Replay.Scanned,
		result.Replay.Matched,
		result.Replay.Planned,
		result.Candidates,
		result.AuditRemoved,
		result.TailSuppressed,
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
