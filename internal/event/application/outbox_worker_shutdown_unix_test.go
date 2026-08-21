//go:build unix

package application

// Real-signal coverage: raises an actual SIGTERM against the test process.
// Signals cannot be self-delivered portably on Windows, hence the unix
// build tag; the portable equivalent lives in
// outbox_worker_shutdown_test.go.

import (
	"context"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

func TestOutboxWorkerStopsOnRealSIGTERM(t *testing.T) {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGTERM,
	)
	defer stop()

	w := newIdleWorker()

	done := make(chan error, 1)

	go func() {
		done <- w.Run(ctx)
	}()

	waitWorkerState(t, w, WorkerStateRunning)

	if err := syscall.Kill(
		syscall.Getpid(),
		syscall.SIGTERM,
	); err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}

	if err := waitRunResult(t, done, 3*time.Second); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	if got := w.State(); got != WorkerStateStopped {
		t.Errorf("state = %s, want %s", got, WorkerStateStopped)
	}
}
