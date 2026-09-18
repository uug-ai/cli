package actions

import (
	"bytes"
	"strings"
	"testing"
)

func TestLoadTestCommandWiring(t *testing.T) {
	t.Setenv("LOADTEST_MONGODB_URI", "")
	t.Setenv("LOADTEST_RABBITMQ_URL", "")
	var stdout, stderr bytes.Buffer
	if code := RunLoadTest([]string{"prepare", "--run-id", "wiring"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"dryRun": true`) {
		t.Fatalf("unexpected report: %s", stdout.String())
	}
}

func TestLoadTestCommandHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := RunLoadTest([]string{"--help"}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "prepare|run|verify|cleanup") {
		t.Fatalf("help code=%d out=%s err=%s", code, stdout.String(), stderr.String())
	}
}
