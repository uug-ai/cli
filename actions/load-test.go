package actions

import (
	"context"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/uug-ai/cli/internal/loadtest"
)

func RunLoadTest(args []string, stdout, stderr io.Writer) int {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	return loadtest.Command(ctx, args, stdout, stderr)
}
