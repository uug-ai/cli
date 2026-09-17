package actions

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	sharedqueue "github.com/uug-ai/queue/pkg/queue"
)

type fakeVaultURLRefresher struct {
	calls    [][]vaultMediaURLRequest
	response map[string]string
	err      error
}

func (f *fakeVaultURLRefresher) RefreshSignedURLs(_ context.Context, media []vaultMediaURLRequest) (map[string]string, error) {
	f.calls = append(f.calls, append([]vaultMediaURLRequest(nil), media...))
	if f.err != nil {
		return nil, f.err
	}
	urls := make(map[string]string, len(media))
	for _, item := range media {
		if value := f.response[item.Filename]; value != "" {
			urls[item.Filename] = value
		} else {
			urls[item.Filename] = "https://vault.test/" + item.Filename
		}
	}
	return urls, nil
}

type scriptedRecoveryAdmin struct {
	messages []sharedqueue.DeadLetterMessage
	requests []sharedqueue.DeadLetterReplayRequest
	payloads [][]byte
}

func (*scriptedRecoveryAdmin) InspectDeadLetters(context.Context, sharedqueue.DeadLetterInspectRequest) (sharedqueue.DeadLetterInspectResult, error) {
	return sharedqueue.DeadLetterInspectResult{}, nil
}

func (f *scriptedRecoveryAdmin) ReplayDeadLetters(ctx context.Context, request sharedqueue.DeadLetterReplayRequest) (sharedqueue.DeadLetterReplayResult, error) {
	f.requests = append(f.requests, request)
	count := request.Limit
	if count > len(f.messages) {
		count = len(f.messages)
	}
	result := sharedqueue.DeadLetterReplayResult{
		Scanned:      count,
		Matched:      count,
		Planned:      count,
		Destinations: map[string]int{request.Destination: count},
	}
	if !request.Execute {
		result.Retained = count
	}
	batchSize := request.BatchSize
	if batchSize == 0 || batchSize > count {
		batchSize = count
	}
	for start := 0; start < count; start += batchSize {
		end := start + batchSize
		if end > count {
			end = count
		}
		transformations, err := request.Transform(ctx, f.messages[start:end])
		if err != nil {
			return result, err
		}
		for _, transformation := range transformations {
			if transformation.Skip {
				result.Skipped++
				result.Planned--
				if request.Execute {
					result.Retained++
				}
				continue
			}
			f.payloads = append(f.payloads, transformation.Payload)
			if request.Execute {
				result.Replayed++
			}
		}
	}
	result.Destinations[request.Destination] = result.Planned
	return result, nil
}

func (*scriptedRecoveryAdmin) PublishDeadLetter(context.Context, []byte, sharedqueue.DeadLetterMetadata) error {
	return nil
}

func TestTransformPipelineRecoveryBatchRefreshesOnlySignedURL(t *testing.T) {
	message := recoveryTestMessage("message-1", "sequence", "recording.mp4", "azure", map[string]any{
		"topLevel": map[string]any{"keep": true},
	})
	refresher := &fakeVaultURLRefresher{response: map[string]string{
		"recording.mp4": "https://vault.test/fresh",
	}}
	result := deadLetterRecoveryResult{}
	transformations, err := transformPipelineRecoveryBatch(
		context.Background(),
		[]sharedqueue.DeadLetterMessage{message},
		true,
		"",
		"24h",
		refresher,
		defaultPipelineRecoverySafetyPolicy(),
		&result,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(refresher.calls) != 1 || len(refresher.calls[0]) != 1 {
		t.Fatalf("Vault calls = %+v", refresher.calls)
	}
	request := refresher.calls[0][0]
	if request.Filename != "recording.mp4" || request.Provider != "azure" || request.URIExpiryTime != "24h" {
		t.Fatalf("Vault request = %+v", request)
	}

	var event map[string]any
	if err := json.Unmarshal(transformations[0].Payload, &event); err != nil {
		t.Fatal(err)
	}
	payload := event["payload"].(map[string]any)
	if payload["signedUrl"] != "https://vault.test/fresh" || payload["unknown"] != "preserved" {
		t.Fatalf("payload = %+v", payload)
	}
	if !reflect.DeepEqual(event["topLevel"], map[string]any{"keep": true}) {
		t.Fatalf("top-level extension = %+v", event["topLevel"])
	}
	if result.Candidates != 1 || result.Refreshed != 1 || result.ByStage["sequence"] != 1 {
		t.Fatalf("result = %+v", result)
	}
}

func TestTransformPipelineRecoveryBatchDryRunValidatesWithoutVault(t *testing.T) {
	result := deadLetterRecoveryResult{}
	message := recoveryTestMessage("message-1", "analysis", "recording.mp4", "", nil)
	transformations, err := transformPipelineRecoveryBatch(
		context.Background(),
		[]sharedqueue.DeadLetterMessage{message},
		false,
		"fallback-provider",
		"",
		nil,
		defaultPipelineRecoverySafetyPolicy(),
		&result,
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(transformations[0].Payload) != string(message.Payload) {
		t.Fatal("dry-run changed the payload")
	}
	if result.Candidates != 1 || result.Refreshed != 0 || result.ByStage["analysis"] != 1 {
		t.Fatalf("result = %+v", result)
	}
}

func TestTransformPipelineRecoveryBatchUsesEventStorageProviderFallback(t *testing.T) {
	result := deadLetterRecoveryResult{}
	refresher := &fakeVaultURLRefresher{}
	message := recoveryTestMessage("message-1", "analysis", "recording.mp4", "", map[string]any{
		"provider": "s3",
	})
	_, err := transformPipelineRecoveryBatch(
		context.Background(),
		[]sharedqueue.DeadLetterMessage{message},
		true,
		"default-provider",
		"",
		refresher,
		defaultPipelineRecoverySafetyPolicy(),
		&result,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(refresher.calls) != 1 || len(refresher.calls[0]) != 1 ||
		refresher.calls[0][0].Provider != "s3" {
		t.Fatalf("Vault calls = %+v", refresher.calls)
	}
}

func TestTransformPipelineRecoveryBatchBypassesOnDemandURLRefresh(t *testing.T) {
	result := deadLetterRecoveryResult{}
	refresher := &fakeVaultURLRefresher{err: errors.New("Vault must not be called")}
	message := recoveryTestMessage("message-1", "sequence", "recording.mp4", "minio-local", map[string]any{
		"request": "ondemand",
	})
	transformations, err := transformPipelineRecoveryBatch(
		context.Background(),
		[]sharedqueue.DeadLetterMessage{message},
		true,
		"",
		"",
		refresher,
		defaultPipelineRecoverySafetyPolicy(),
		&result,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(refresher.calls) != 0 {
		t.Fatalf("Vault calls = %+v", refresher.calls)
	}
	if len(transformations) != 1 || transformations[0].Skip ||
		string(transformations[0].Payload) != string(message.Payload) {
		t.Fatalf("transformations = %+v", transformations)
	}
	if result.Candidates != 1 || result.Bypassed != 1 || result.Refreshed != 0 ||
		result.ByStage["sequence"] != 1 {
		t.Fatalf("result = %+v", result)
	}
}

func TestTransformPipelineRecoveryBatchRefreshesPersistAndBypassesOnDemand(t *testing.T) {
	result := deadLetterRecoveryResult{}
	refresher := &fakeVaultURLRefresher{}
	onDemand := recoveryTestMessage("message-1", "sequence", "ondemand.mp4", "minio-local", map[string]any{
		"request": "ondemand",
	})
	persist := recoveryTestMessage("message-2", "sequence", "persist.mp4", "ceph", map[string]any{
		"request": "persist",
	})
	transformations, err := transformPipelineRecoveryBatch(
		context.Background(),
		[]sharedqueue.DeadLetterMessage{onDemand, persist},
		true,
		"",
		"",
		refresher,
		defaultPipelineRecoverySafetyPolicy(),
		&result,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(refresher.calls) != 1 || len(refresher.calls[0]) != 1 ||
		refresher.calls[0][0].Filename != "persist.mp4" {
		t.Fatalf("Vault calls = %+v", refresher.calls)
	}
	if string(transformations[0].Payload) != string(onDemand.Payload) {
		t.Fatal("on-demand payload was changed")
	}
	if string(transformations[1].Payload) == string(persist.Payload) {
		t.Fatal("persist payload signed URL was not refreshed")
	}
	if result.Candidates != 2 || result.Bypassed != 1 || result.Refreshed != 1 {
		t.Fatalf("result = %+v", result)
	}
}

func TestTransformPipelineRecoveryBatchSkipsMissingStage(t *testing.T) {
	result := deadLetterRecoveryResult{}
	message := recoveryTestMessage("message-1", "", "recording.mp4", "azure", nil)
	transformations, err := transformPipelineRecoveryBatch(
		context.Background(),
		[]sharedqueue.DeadLetterMessage{message},
		false,
		"",
		"",
		nil,
		defaultPipelineRecoverySafetyPolicy(),
		&result,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(transformations) != 1 || !transformations[0].Skip || result.Candidates != 0 {
		t.Fatalf("transformations=%+v result=%+v", transformations, result)
	}
}

func TestTransformPipelineRecoveryBatchContinuesPastInvalidMessage(t *testing.T) {
	result := deadLetterRecoveryResult{}
	refresher := &fakeVaultURLRefresher{}
	messages := []sharedqueue.DeadLetterMessage{
		recoveryTestMessage("message-1", "sequence", "one.mp4", "azure", nil),
		recoveryTestMessage("message-2", "", "poison.mp4", "azure", nil),
		recoveryTestMessage("message-3", "analysis", "three.mp4", "azure", nil),
	}
	transformations, err := transformPipelineRecoveryBatch(
		context.Background(),
		messages,
		true,
		"",
		"",
		refresher,
		defaultPipelineRecoverySafetyPolicy(),
		&result,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(transformations) != 3 || transformations[0].Skip || !transformations[1].Skip || transformations[2].Skip {
		t.Fatalf("transformations = %+v", transformations)
	}
	if len(refresher.calls) != 1 || len(refresher.calls[0]) != 2 {
		t.Fatalf("Vault calls = %+v", refresher.calls)
	}
	if result.Candidates != 2 || result.Refreshed != 2 ||
		result.ByStage["sequence"] != 1 || result.ByStage["analysis"] != 1 {
		t.Fatalf("result = %+v", result)
	}
}

func TestTransformPipelineRecoveryBatchRemovesEmbeddedUserAudit(t *testing.T) {
	for _, audit := range []any{
		map[string]any{"createdAt": "legacy"},
		[]any{map[string]any{"create": map[string]any{"createdAt": 1}}},
	} {
		t.Run(reflect.TypeOf(audit).String(), func(t *testing.T) {
			result := deadLetterRecoveryResult{}
			refresher := &fakeVaultURLRefresher{}
			message := recoveryTestMessage("message-1", "sequence", "recording.mp4", "ceph", map[string]any{
				"monitorStage": recoveryTestMonitorStage(map[string]any{
					"audit":       audit,
					"futureField": map[string]any{"keep": true},
				}),
			})

			transformations, err := transformPipelineRecoveryBatch(
				context.Background(),
				[]sharedqueue.DeadLetterMessage{message},
				true,
				"",
				"",
				refresher,
				defaultPipelineRecoverySafetyPolicy(),
				&result,
			)
			if err != nil {
				t.Fatal(err)
			}
			var event map[string]any
			if err := json.Unmarshal(transformations[0].Payload, &event); err != nil {
				t.Fatal(err)
			}
			user := event["monitorStage"].(map[string]any)["user"].(map[string]any)
			if _, present := user["audit"]; present {
				t.Fatalf("audit was not removed: %+v", user)
			}
			if !reflect.DeepEqual(user["futureField"], map[string]any{"keep": true}) {
				t.Fatalf("future user field was not preserved: %+v", user)
			}
			if result.AuditRemoved != 1 || result.Candidates != 1 || result.Refreshed != 1 {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}

func TestTransformPipelineRecoveryBatchDryRunReportsAuditRemovalWithoutChangingPayload(t *testing.T) {
	result := deadLetterRecoveryResult{}
	message := recoveryTestMessage("message-1", "sequence", "recording.mp4", "ceph", map[string]any{
		"monitorStage": recoveryTestMonitorStage(map[string]any{
			"audit": map[string]any{"createdAt": "legacy"},
		}),
	})
	transformations, err := transformPipelineRecoveryBatch(
		context.Background(),
		[]sharedqueue.DeadLetterMessage{message},
		false,
		"",
		"",
		nil,
		defaultPipelineRecoverySafetyPolicy(),
		&result,
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(transformations[0].Payload) != string(message.Payload) {
		t.Fatal("dry-run changed the payload")
	}
	if result.AuditRemoved != 1 || result.Candidates != 1 {
		t.Fatalf("result = %+v", result)
	}
}

func TestTransformPipelineRecoveryBatchSuppressesHistoricalTail(t *testing.T) {
	result := deadLetterRecoveryResult{}
	refresher := &fakeVaultURLRefresher{}
	message := recoveryTestMessage("message-1", "sequence", "recording.mp4", "ceph", map[string]any{
		"date":   time.Now().Add(-time.Hour).Unix(),
		"events": []string{"sequence", "analysis", "throttler", "notification"},
	})
	transformations, err := transformPipelineRecoveryBatch(
		context.Background(),
		[]sharedqueue.DeadLetterMessage{message},
		true,
		"",
		"",
		refresher,
		defaultPipelineRecoverySafetyPolicy(),
		&result,
	)
	if err != nil {
		t.Fatal(err)
	}
	var event struct {
		Stages []string `json:"events"`
	}
	if err := json.Unmarshal(transformations[0].Payload, &event); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(event.Stages, []string{"sequence", "analysis"}) {
		t.Fatalf("stages = %v", event.Stages)
	}
	if result.TailSuppressed != 1 || result.Candidates != 1 || result.Refreshed != 1 {
		t.Fatalf("result = %+v", result)
	}
}

func TestTransformPipelineRecoveryBatchRetainsHistoricalTailAtCurrentStage(t *testing.T) {
	result := deadLetterRecoveryResult{}
	refresher := &fakeVaultURLRefresher{}
	message := recoveryTestMessage("message-1", "notification", "recording.mp4", "ceph", map[string]any{
		"date":         time.Now().Add(-time.Hour).Unix(),
		"events":       []string{"notification"},
		"monitorStage": recoveryTestMonitorStage(map[string]any{"audit": map[string]any{"legacy": true}}),
	})
	transformations, err := transformPipelineRecoveryBatch(
		context.Background(),
		[]sharedqueue.DeadLetterMessage{message},
		true,
		"",
		"",
		refresher,
		defaultPipelineRecoverySafetyPolicy(),
		&result,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !transformations[0].Skip || len(refresher.calls) != 0 ||
		result.ByFailure["historical-tail-current"] != 1 ||
		result.AuditRemoved != 0 || result.TailSuppressed != 0 {
		t.Fatalf("transformations=%+v result=%+v calls=%+v", transformations, result, refresher.calls)
	}
}

func TestTransformPipelineRecoveryBatchAllowsHistoricalTailExplicitly(t *testing.T) {
	result := deadLetterRecoveryResult{}
	refresher := &fakeVaultURLRefresher{}
	message := recoveryTestMessage("message-1", "sequence", "recording.mp4", "ceph", map[string]any{
		"date": time.Now().Add(-time.Hour).Unix(),
	})
	transformations, err := transformPipelineRecoveryBatch(
		context.Background(),
		[]sharedqueue.DeadLetterMessage{message},
		true,
		"",
		"",
		refresher,
		pipelineRecoverySafetyPolicy{
			historicalTailMaxAge: defaultHistoricalTailMaxAge,
			allowHistoricalTail:  true,
		},
		&result,
	)
	if err != nil {
		t.Fatal(err)
	}
	var event struct {
		Stages []string `json:"events"`
	}
	if err := json.Unmarshal(transformations[0].Payload, &event); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(event.Stages, []string{"sequence", "notification"}) ||
		result.TailSuppressed != 0 {
		t.Fatalf("event=%+v result=%+v", event, result)
	}
}

func TestTransformPipelineRecoveryBatchRejectsUnsafeStageAndOwnership(t *testing.T) {
	tests := []struct {
		name   string
		extra  map[string]any
		reason string
	}{
		{
			name:   "empty later stage",
			extra:  map[string]any{"events": []string{"sequence", ""}},
			reason: "invalid-stage",
		},
		{
			name:   "terminal stage",
			extra:  map[string]any{"events": []string{"sequence", "end"}},
			reason: "invalid-stage",
		},
		{
			name: "signed URL whitespace",
			extra: map[string]any{
				"request": "ondemand",
				"payload": map[string]any{
					"key":       "recording.mp4",
					"signedUrl": " https://vault.test/recording.mp4 ",
				},
			},
			reason: "invalid-signed-url",
		},
		{
			name: "legacy owner conflict",
			extra: map[string]any{
				"monitorStage": map[string]any{
					"organisationId": "bbbbbbbbbbbbbbbbbbbbbbbb",
					"projectId":      "bbbbbbbbbbbbbbbbbbbbbbbb",
					"user": map[string]any{
						"id":    recoveryTestUserID,
						"email": "user@example.com",
					},
				},
			},
			reason: "legacy-owner-conflict",
		},
		{
			name: "malformed embedded storage URI",
			extra: map[string]any{
				"monitorStage": recoveryTestMonitorStage(map[string]any{
					"storage": map[string]any{"uri": "[https://vault.test](https://vault.test)"},
				}),
			},
			reason: "invalid-storage-uri",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := deadLetterRecoveryResult{}
			refresher := &fakeVaultURLRefresher{}
			message := recoveryTestMessage("message-1", "sequence", "recording.mp4", "ceph", test.extra)
			safety := defaultPipelineRecoverySafetyPolicy()
			if test.reason == "legacy-owner-conflict" {
				safety.legacyUserOwnership = true
			}
			transformations, err := transformPipelineRecoveryBatch(
				context.Background(),
				[]sharedqueue.DeadLetterMessage{message},
				true,
				"",
				"",
				refresher,
				safety,
				&result,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !transformations[0].Skip || len(refresher.calls) != 0 || result.ByFailure[test.reason] != 1 {
				t.Fatalf("transformations=%+v result=%+v calls=%+v", transformations, result, refresher.calls)
			}
		})
	}
}

func TestTransformPipelineRecoveryBatchRejectsInvalidVaultSignedURL(t *testing.T) {
	result := deadLetterRecoveryResult{}
	refresher := &fakeVaultURLRefresher{response: map[string]string{
		"recording.mp4": "[https://vault.test/fresh](https://vault.test/fresh)",
	}}
	message := recoveryTestMessage("message-1", "sequence", "recording.mp4", "ceph", nil)
	_, err := transformPipelineRecoveryBatch(
		context.Background(),
		[]sharedqueue.DeadLetterMessage{message},
		true,
		"",
		"",
		refresher,
		defaultPipelineRecoverySafetyPolicy(),
		&result,
	)
	if err == nil || !strings.Contains(err.Error(), "invalid signed URL") {
		t.Fatalf("error = %v", err)
	}
}

func TestTransformPipelineRecoveryBatchSafelyNormalizesLegacySequenceEvent(t *testing.T) {
	recordingTimestamp := time.Now().Add(-time.Hour).Unix()
	event := map[string]any{
		"request":   "persist",
		"operation": "event",
		"events":    []string{"sequence", "analysis", "throttler", "notification"},
		"date":      recordingTimestamp,
		"source":    "ceph",
		"provider":  "kstorage",
		"monitorStage": map[string]any{
			"name":           "monitor",
			"organisationId": recoveryTestUserID,
			"projectId":      recoveryTestUserID,
			"user": map[string]any{
				"id":    recoveryTestUserID,
				"email": "user@example.com",
				"audit": map[string]any{
					"createdAt": "0001-01-01T00:00:00Z",
					"updatedAt": "0001-01-01T00:00:00Z",
				},
				"storage": map[string]any{"uri": "http://vault.internal/api"},
			},
		},
		"payload": map[string]any{
			"key":           "user@example.com/recording.mp4",
			"signedUrl":     "https://vault.test/expired",
			"is_fragmented": false,
			"metadata": map[string]any{
				"event-timestamp": recordingTimestamp,
				"duration":        "30000",
				"productid":       "device-key",
			},
		},
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	message := sharedqueue.DeadLetterMessage{ID: "message-1", Payload: payload}
	refresher := &fakeVaultURLRefresher{response: map[string]string{
		"user@example.com/recording.mp4": "https://vault.test/fresh",
	}}
	result := deadLetterRecoveryResult{}
	safety := defaultPipelineRecoverySafetyPolicy()
	safety.legacyUserOwnership = true
	transformations, err := transformPipelineRecoveryBatch(
		context.Background(),
		[]sharedqueue.DeadLetterMessage{message},
		true,
		"",
		"",
		refresher,
		safety,
		&result,
	)
	if err != nil {
		t.Fatal(err)
	}
	if transformations[0].Skip {
		t.Fatal("legacy sequence event was retained")
	}
	var recovered map[string]any
	if err := json.Unmarshal(transformations[0].Payload, &recovered); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(recovered["events"], []any{"sequence", "analysis"}) {
		t.Fatalf("stages = %+v", recovered["events"])
	}
	user := recovered["monitorStage"].(map[string]any)["user"].(map[string]any)
	if _, present := user["audit"]; present {
		t.Fatalf("audit was not removed: %+v", user)
	}
	recoveredPayload := recovered["payload"].(map[string]any)
	if recoveredPayload["signedUrl"] != "https://vault.test/fresh" ||
		recoveredPayload["is_fragmented"] != false {
		t.Fatalf("payload = %+v", recoveredPayload)
	}
	if result.AuditRemoved != 1 || result.TailSuppressed != 1 ||
		result.Candidates != 1 || result.Refreshed != 1 {
		t.Fatalf("result = %+v", result)
	}
}

func TestVaultHTTPURLRefresherUsesBulkEndpointWithoutLeakingSecrets(t *testing.T) {
	const (
		accessKey = "access-value"
		secret    = "secret-value"
	)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/storage/bulk" {
			t.Errorf("path = %q", request.URL.Path)
		}
		if request.Header.Get("X-Kerberos-Storage-AccessKey") != accessKey ||
			request.Header.Get("X-Kerberos-Storage-SecretAccessKey") != secret {
			t.Errorf("authentication headers are missing")
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		var media []vaultMediaURLRequest
		if err := json.Unmarshal(body, &media); err != nil {
			t.Fatal(err)
		}
		if len(media) != 1 || media[0].Filename != "recording.mp4" {
			t.Errorf("media = %+v", media)
		}
		_, _ = writer.Write([]byte(`{"data":"{\"recording.mp4\":\"https://vault.test/fresh\"}"}`))
	}))
	defer server.Close()

	refresher, err := newVaultHTTPURLRefresher(server.URL+"/api", accessKey, secret, false, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	urls, err := refresher.RefreshSignedURLs(context.Background(), []vaultMediaURLRequest{{Filename: "recording.mp4"}})
	if err != nil {
		t.Fatal(err)
	}
	if urls["recording.mp4"] != "https://vault.test/fresh" {
		t.Fatalf("URLs = %+v", urls)
	}

	failingServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusUnauthorized)
		_, _ = writer.Write([]byte("credentials: " + accessKey + " " + secret))
	}))
	defer failingServer.Close()
	failingRefresher, err := newVaultHTTPURLRefresher(failingServer.URL, accessKey, secret, false, failingServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = failingRefresher.RefreshSignedURLs(context.Background(), []vaultMediaURLRequest{{Filename: "recording.mp4"}})
	if err == nil {
		t.Fatal("expected Vault error")
	}
	if strings.Contains(err.Error(), accessKey) || strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaked credentials: %v", err)
	}

	redirectTargetCalled := false
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirectTargetCalled = true
	}))
	defer redirectTarget.Close()
	redirectSource := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, redirectTarget.URL, http.StatusTemporaryRedirect)
	}))
	defer redirectSource.Close()
	redirectRefresher, err := newVaultHTTPURLRefresher(redirectSource.URL, accessKey, secret, false, redirectSource.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = redirectRefresher.RefreshSignedURLs(context.Background(), []vaultMediaURLRequest{{Filename: "recording.mp4"}})
	if err == nil || !strings.Contains(err.Error(), "HTTP 307") {
		t.Fatalf("redirect error = %v", err)
	}
	if redirectTargetCalled {
		t.Fatal("Vault client followed a redirect with credential headers")
	}
}

func TestVaultBulkEndpointRequiresHTTPSOutsideLoopback(t *testing.T) {
	_, err := vaultBulkEndpoint("http://vault.internal/api", false)
	if err == nil || !strings.Contains(err.Error(), "must use HTTPS") {
		t.Fatalf("error = %v", err)
	}
	endpoint, err := vaultBulkEndpoint("http://vault.internal/api", true)
	if err != nil {
		t.Fatal(err)
	}
	if endpoint != "http://vault.internal/api/storage/bulk" {
		t.Fatalf("endpoint = %q", endpoint)
	}
}

func TestValidateRecoveryConfigAllowsSourceFilteredSQSExecution(t *testing.T) {
	err := validateRecoveryConfig(dlqCommandConfig{
		provider:       "sqs",
		source:         "sequence",
		destination:    "event",
		limit:          100,
		batchSize:      10,
		timeout:        time.Minute,
		execute:        true,
		vaultURI:       "https://vault.example/api",
		vaultAccessKey: "access",
		vaultSecret:    "secret",
	})
	if err != nil {
		t.Fatalf("error = %v", err)
	}
}

func TestRecoverDeadLettersProcessesBoundedBatches(t *testing.T) {
	admin := &scriptedRecoveryAdmin{}
	for index := 0; index < 5; index++ {
		admin.messages = append(admin.messages, recoveryTestMessage(
			"message-"+string(rune('a'+index)),
			"sequence",
			"recording-"+string(rune('a'+index))+".mp4",
			"azure",
			nil,
		))
	}
	refresher := &fakeVaultURLRefresher{}
	result, err := recoverDeadLetters(context.Background(), admin, refresher, dlqCommandConfig{
		limit:       5,
		batchSize:   2,
		batchDelay:  3 * time.Second,
		timeout:     time.Second,
		destination: "kcloud-event-queue",
		execute:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(admin.requests) != 1 {
		t.Fatalf("replay calls = %d", len(admin.requests))
	}
	request := admin.requests[0]
	if request.Limit != 5 || request.BatchSize != 2 || request.BatchDelay != 3*time.Second ||
		request.BatchTimeout != time.Second || request.Destination != "kcloud-event-queue" {
		t.Fatalf("replay request = %+v", request)
	}
	if result.Batches != 3 || result.Replay.Scanned != 5 || result.Replay.Replayed != 5 ||
		result.Candidates != 5 || result.Refreshed != 5 || len(refresher.calls) != 3 {
		t.Fatalf("result = %+v, Vault calls = %d", result, len(refresher.calls))
	}
}

func TestRecoverDeadLettersDryRunScansConfiguredLimit(t *testing.T) {
	admin := &scriptedRecoveryAdmin{
		messages: []sharedqueue.DeadLetterMessage{
			recoveryTestMessage("message-1", "sequence", "one.mp4", "azure", nil),
			recoveryTestMessage("message-2", "analysis", "two.mp4", "azure", nil),
			recoveryTestMessage("message-3", "notification", "three.mp4", "azure", nil),
		},
	}
	result, err := recoverDeadLetters(context.Background(), admin, nil, dlqCommandConfig{
		limit:       3,
		batchSize:   2,
		timeout:     time.Second,
		destination: "kcloud-event-queue",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(admin.requests) != 1 || result.Batches != 2 ||
		result.Replay.Scanned != 3 || result.Replay.Replayed != 0 || result.Candidates != 3 ||
		result.ByStage["sequence"] != 1 || result.ByStage["analysis"] != 1 ||
		result.ByStage["notification"] != 1 {
		t.Fatalf("result = %+v, requests = %+v", result, admin.requests)
	}
}

func recoveryTestMessage(id, stage, fileName, provider string, extra map[string]any) sharedqueue.DeadLetterMessage {
	event := map[string]any{
		"events": []string{stage, "notification"},
		"date":   time.Now().Unix(),
		"source": provider,
		"payload": map[string]any{
			"key":       fileName,
			"signedUrl": "https://vault.test/expired",
			"unknown":   "preserved",
		},
		"monitorStage": recoveryTestMonitorStage(nil),
	}
	for key, value := range extra {
		event[key] = value
	}
	payload, err := json.Marshal(event)
	if err != nil {
		panic(err)
	}
	return sharedqueue.DeadLetterMessage{ID: id, Payload: payload}
}

const recoveryTestUserID = "aaaaaaaaaaaaaaaaaaaaaaaa"

func recoveryTestMonitorStage(userExtra map[string]any) map[string]any {
	user := map[string]any{
		"id":    recoveryTestUserID,
		"email": "user@example.com",
	}
	for key, value := range userExtra {
		user[key] = value
	}
	return map[string]any{
		"organisationId": recoveryTestUserID,
		"projectId":      recoveryTestUserID,
		"user":           user,
	}
}
