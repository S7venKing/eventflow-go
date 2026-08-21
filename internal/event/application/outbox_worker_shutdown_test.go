package application

// Graceful shutdown tests for the outbox worker.
//
// The idle and signal-context tests need no database: with a one-hour
// interval the worker never claims, so they run everywhere. The in-flight
// tests reuse the Postgres harness from outbox_worker_integration_test.go
// and skip when no database is reachable.

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/s7venking/eventflow/internal/event/domain"
	"github.com/s7venking/eventflow/internal/platform/postgres"
)

// ========================================
// Helpers
// ========================================

// newIdleWorker never ticks within a test's lifetime, so it needs no
// database behind its repository.
func newIdleWorker() *OutboxWorker {
	return NewOutboxWorker(
		postgres.NewOutboxRepository(nil),
		&fakePublisher{},
		time.Hour,
		1,
		3,
		time.Second,
		30*time.Second,
		time.Second,
	)
}

func waitWorkerState(
	t *testing.T,
	w *OutboxWorker,
	want string,
) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		if w.State() == want {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("worker state = %s, want %s", w.State(), want)
}

func waitRunResult(
	t *testing.T,
	done <-chan error,
	timeout time.Duration,
) error {
	t.Helper()

	select {
	case err := <-done:
		return err

	case <-time.After(timeout):
		t.Fatal("worker did not stop in time")
		return nil
	}
}

// blockingPublisher parks every Publish call until release is closed or
// the publish context is canceled, so tests control exactly when in-flight
// work finishes.
type blockingPublisher struct {
	mu      sync.Mutex
	started chan uuid.UUID
	release chan struct{}
	calls   map[uuid.UUID]int
}

func newBlockingPublisher() *blockingPublisher {
	return &blockingPublisher{
		started: make(chan uuid.UUID, 16),
		release: make(chan struct{}),
	}
}

func (p *blockingPublisher) Publish(
	ctx context.Context,
	event domain.OutboxEvent,
) error {
	p.mu.Lock()

	if p.calls == nil {
		p.calls = make(map[uuid.UUID]int)
	}

	p.calls[event.ID]++
	p.mu.Unlock()

	p.started <- event.ID

	select {
	case <-p.release:
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *blockingPublisher) callCount(id uuid.UUID) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.calls[id]
}

func waitPublishStarted(
	t *testing.T,
	pub *blockingPublisher,
) uuid.UUID {
	t.Helper()

	select {
	case id := <-pub.started:
		return id

	case <-time.After(5 * time.Second):
		t.Fatal("publish never started")
		return uuid.Nil
	}
}

// ========================================
// Idle shutdown (no database needed)
// ========================================

func TestOutboxWorkerIdleShutdown(t *testing.T) {
	w := newIdleWorker()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- w.Run(ctx)
	}()

	waitWorkerState(t, w, WorkerStateRunning)

	cancel()

	if err := waitRunResult(t, done, 2*time.Second); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	if got := w.State(); got != WorkerStateStopped {
		t.Errorf("state = %s, want %s", got, WorkerStateStopped)
	}
}

// ========================================
// Signal handling
// ========================================

// SIGTERM cannot be raised portably on Windows, so this test drives the
// exact context that signal.NotifyContext cancels on signal delivery; the
// real-signal path is covered by the unix-only test.
func TestOutboxWorkerStopsOnSignalContext(t *testing.T) {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	w := newIdleWorker()

	done := make(chan error, 1)

	go func() {
		done <- w.Run(ctx)
	}()

	waitWorkerState(t, w, WorkerStateRunning)

	stop()

	if err := waitRunResult(t, done, 2*time.Second); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	if got := w.State(); got != WorkerStateStopped {
		t.Errorf("state = %s, want %s", got, WorkerStateStopped)
	}
}

// ========================================
// In-flight work completes during shutdown
// ========================================

func TestOutboxWorkerShutdownWaitsForInFlightWork(t *testing.T) {
	db := acquireIntegrationDB(t)
	id := insertReadyEvent(t, db)

	pub := newBlockingPublisher()

	w := NewOutboxWorker(
		postgres.NewOutboxRepository(db),
		pub,
		10*time.Millisecond,
		10,
		3,
		time.Second,
		30*time.Second,
		5*time.Second,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- w.Run(ctx)
	}()

	waitPublishStarted(t, pub)

	// Shutdown begins while the publish is in flight.
	cancel()

	waitWorkerState(t, w, WorkerStateStopping)

	// The worker must still be draining, not exited.
	select {
	case err := <-done:
		t.Fatalf("worker exited before in-flight work finished: %v", err)
	default:
	}

	close(pub.release)

	if err := waitRunResult(t, done, 3*time.Second); err != nil {
		t.Fatalf("Run returned %v, want nil (clean drain)", err)
	}

	if got := w.State(); got != WorkerStateStopped {
		t.Errorf("state = %s, want %s", got, WorkerStateStopped)
	}

	row := readOutboxRow(t, db, id)

	if row.status != "PUBLISHED" {
		t.Errorf(
			"in-flight event status = %s, want PUBLISHED",
			row.status,
		)
	}
}

// ========================================
// Shutdown timeout force-cancels in-flight work
// ========================================

func TestOutboxWorkerShutdownTimeoutForcesCancellation(t *testing.T) {
	db := acquireIntegrationDB(t)
	id := insertReadyEvent(t, db)

	pub := newBlockingPublisher() // never released

	w := NewOutboxWorker(
		postgres.NewOutboxRepository(db),
		pub,
		10*time.Millisecond,
		10,
		3,
		time.Second,
		30*time.Second,
		150*time.Millisecond,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- w.Run(ctx)
	}()

	waitPublishStarted(t, pub)

	cancel()

	err := waitRunResult(t, done, 3*time.Second)

	if !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("Run returned %v, want ErrShutdownTimeout", err)
	}

	if got := w.State(); got != WorkerStateStopped {
		t.Errorf("state = %s, want %s", got, WorkerStateStopped)
	}

	// The forced cancellation reached the publisher through its context;
	// the event stays PROCESSING with attempts untouched, exactly like a
	// crash, and is left for reclaim.
	row := readOutboxRow(t, db, id)

	if row.status != "PROCESSING" {
		t.Errorf(
			"force-canceled event status = %s, want PROCESSING",
			row.status,
		)
	}

	if row.attempts != 0 {
		t.Errorf(
			"force-canceled event attempts = %d, want 0",
			row.attempts,
		)
	}
}

// ========================================
// No new claims once shutdown has begun
// ========================================

func TestOutboxWorkerNoNewClaimsAfterShutdownBegins(t *testing.T) {
	db := acquireIntegrationDB(t)

	old := time.Now().Add(-time.Hour)

	first := insertOutboxEvent(t, db, "PENDING", 0, old, old)
	second := insertOutboxEvent(t, db, "PENDING", 0, old, old.Add(time.Second))

	pub := newBlockingPublisher()

	w := NewOutboxWorker(
		postgres.NewOutboxRepository(db),
		pub,
		10*time.Millisecond,
		1, // one event per batch, so `second` needs a second claim
		3,
		time.Second,
		30*time.Second,
		5*time.Second,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- w.Run(ctx)
	}()

	if claimed := waitPublishStarted(t, pub); claimed != first {
		t.Fatalf("first claimed event = %s, want %s", claimed, first)
	}

	// Shutdown begins while `first` is in flight; releasing it afterwards
	// must finish the batch without claiming `second`.
	cancel()
	close(pub.release)

	if err := waitRunResult(t, done, 3*time.Second); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	if row := readOutboxRow(t, db, first); row.status != "PUBLISHED" {
		t.Errorf(
			"in-flight event status = %s, want PUBLISHED",
			row.status,
		)
	}

	row := readOutboxRow(t, db, second)

	if row.status != "PENDING" || row.attempts != 0 {
		t.Errorf(
			"unclaimed event mutated: status=%s attempts=%d, want PENDING/0",
			row.status,
			row.attempts,
		)
	}

	if got := pub.callCount(second); got != 0 {
		t.Errorf(
			"event claimed after shutdown began, publish calls = %d, want 0",
			got,
		)
	}
}
