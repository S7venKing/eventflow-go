package application

// Failure-injection and recovery tests for the outbox worker.
//
// These prove the reliability half of the outbox contract: transient
// publish failures retry until published, permanently failing events end
// in CLOSE without blocking the queue, and a crashed worker's PROCESSING
// rows are reclaimed and published by the survivors. The crash here is
// real in the sense that matters: the worker's in-flight context is
// force-canceled mid-publish, so no MarkFailed and no cleanup ever runs —
// the rows are simply left behind in PROCESSING.
//
// The database harness (throwaway database per test, seeding, counting)
// is shared with outbox_worker_concurrency_test.go.

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/s7venking/eventflow/internal/event/domain"
	"github.com/s7venking/eventflow/internal/platform/postgres"
)

const testPoisonEventType = "poison.event"

// blockingPublisher hangs every publish until the context is canceled,
// pinning a claimed batch in PROCESSING for as long as the test wants.
type blockingPublisher struct{}

func (blockingPublisher) Publish(
	ctx context.Context,
	_ domain.OutboxEvent,
	_ *slog.Logger,
) error {
	<-ctx.Done()

	return ctx.Err()
}

func queryInt(
	t *testing.T,
	db *postgres.DB,
	query string,
	args ...any,
) int {
	t.Helper()

	var n int

	if err := db.Pool.QueryRow(
		context.Background(),
		query,
		args...,
	).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}

	return n
}

func waitProcessingCount(
	t *testing.T,
	db *postgres.DB,
	want int,
	timeout time.Duration,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for {
		got := countOutboxByStatus(t, db).processing

		if got == want {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf(
				"PROCESSING = %d, want %d within %s",
				got,
				want,
				timeout,
			)
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func stopWorkersAndCollect(
	t *testing.T,
	cancel context.CancelFunc,
	group *sync.WaitGroup,
	errs chan error,
) []error {
	t.Helper()

	cancel()
	waitWorkerGroupDone(t, group, 15*time.Second)
	close(errs)

	collected := make([]error, 0)

	for err := range errs {
		collected = append(collected, err)
	}

	return collected
}

// ========================================
// Part A — transient failures retry to PUBLISHED
// ========================================

func TestOutboxWorkersTransientFailuresRetryUntilPublished(t *testing.T) {
	rates := []struct {
		name string
		rate float64
	}{
		{"10 percent", 0.10},
		{"30 percent", 0.30},
	}

	for _, tc := range rates {
		t.Run(tc.name, func(t *testing.T) {
			db := newConcurrencyTestDB(t)

			const (
				totalEvents = 1000
				workers     = 4
				batchSize   = 10
				maxRetries  = 3
			)

			seedPendingOutboxEvents(t, db, totalEvents)

			counting := newCountingPublisher()

			// The failure cap equals the retry budget, so even an
			// unlucky event publishes on its final attempt instead of
			// flaking the test into CLOSE.
			injector := NewFailingPublisher(
				counting,
				tc.rate,
				maxRetries,
			)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			group, errs := startOutboxWorkersCfg(
				ctx,
				postgres.NewOutboxRepository(db),
				injector,
				workerFixture{
					workers:         workers,
					batchSize:       batchSize,
					interval:        50 * time.Millisecond,
					maxRetries:      maxRetries,
					retryBaseDelay:  50 * time.Millisecond,
					retryMaxDelay:   500 * time.Millisecond,
					shutdownTimeout: 10 * time.Second,
				},
			)

			counts := waitOutboxDrained(t, db, 120*time.Second)

			for _, err := range stopWorkersAndCollect(
				t, cancel, group, errs,
			) {
				if err != nil {
					t.Errorf("worker returned %v, want nil", err)
				}
			}

			if counts.published != totalEvents {
				t.Errorf(
					"published = %d, want %d",
					counts.published,
					totalEvents,
				)
			}

			if counts.closed != 0 {
				t.Errorf("CLOSE = %d, want 0", counts.closed)
			}

			if counts.pending != 0 || counts.processing != 0 {
				t.Errorf(
					"leftover pending=%d processing=%d, want 0/0",
					counts.pending,
					counts.processing,
				)
			}

			if lost := totalEvents - counts.total(); lost != 0 {
				t.Errorf("lost = %d, want 0", lost)
			}

			if got := counting.duplicates(); got != 0 {
				t.Errorf("duplicated publishes = %d, want 0", got)
			}

			if got := counting.distinct(); got != totalEvents {
				t.Errorf(
					"distinct published = %d, want %d",
					got,
					totalEvents,
				)
			}

			injected := injector.InjectedFailures()

			if injected == 0 {
				t.Error(
					"no failures were injected; the test exercised nothing",
				)
			}

			if got := injector.Attempts(); got != totalEvents+injected {
				t.Errorf(
					"publish attempts = %d, want %d (published) + %d (injected)",
					got,
					totalEvents,
					injected,
				)
			}

			// Every injected failure must have gone through MarkFailed:
			// attempts incremented and last_error recorded.
			sumAttempts := queryInt(
				t,
				db,
				"SELECT COALESCE(SUM(attempts), 0)::int FROM outbox_events",
			)

			if sumAttempts != injected {
				t.Errorf(
					"SUM(attempts) = %d, want %d injected failures",
					sumAttempts,
					injected,
				)
			}

			missingError := queryInt(
				t,
				db,
				`SELECT COUNT(*)::int FROM outbox_events
				 WHERE attempts > 0 AND last_error IS NULL`,
			)

			if missingError != 0 {
				t.Errorf(
					"%d retried events have no last_error, want 0",
					missingError,
				)
			}
		})
	}
}

// ========================================
// Part B — permanent failures end in CLOSE without blocking the queue
// ========================================

func TestOutboxWorkersPermanentFailuresEndInClose(t *testing.T) {
	db := newConcurrencyTestDB(t)

	const (
		normalEvents = 200
		poisonEvents = 20
		workers      = 4
		batchSize    = 10
		maxRetries   = 3
	)

	seedPendingOutboxEvents(t, db, normalEvents)
	seedTypedPendingOutboxEvents(t, db, poisonEvents, testPoisonEventType)

	counting := newCountingPublisher()

	injector := NewFailingPublisher(
		counting,
		0,
		0,
		testPoisonEventType,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	group, errs := startOutboxWorkersCfg(
		ctx,
		postgres.NewOutboxRepository(db),
		injector,
		workerFixture{
			workers:         workers,
			batchSize:       batchSize,
			interval:        20 * time.Millisecond,
			maxRetries:      maxRetries,
			retryBaseDelay:  30 * time.Millisecond,
			retryMaxDelay:   200 * time.Millisecond,
			shutdownTimeout: 10 * time.Second,
		},
	)

	counts := waitOutboxDrained(t, db, 60*time.Second)

	for _, err := range stopWorkersAndCollect(t, cancel, group, errs) {
		if err != nil {
			t.Errorf("worker returned %v, want nil", err)
		}
	}

	if counts.published != normalEvents {
		t.Errorf(
			"published = %d, want %d",
			counts.published,
			normalEvents,
		)
	}

	if counts.closed != poisonEvents {
		t.Errorf(
			"CLOSE = %d, want %d",
			counts.closed,
			poisonEvents,
		)
	}

	if counts.pending != 0 || counts.processing != 0 {
		t.Errorf(
			"leftover pending=%d processing=%d, want 0/0",
			counts.pending,
			counts.processing,
		)
	}

	if got := counting.distinct(); got != normalEvents {
		t.Errorf(
			"distinct published = %d, want %d",
			got,
			normalEvents,
		)
	}

	if got := counting.duplicates(); got != 0 {
		t.Errorf("duplicated publishes = %d, want 0", got)
	}

	// Only poison events may close.
	wrongClose := queryInt(
		t,
		db,
		`SELECT COUNT(*)::int FROM outbox_events
		 WHERE status = 'CLOSE' AND event_type <> $1`,
		testPoisonEventType,
	)

	if wrongClose != 0 {
		t.Errorf(
			"%d non-poison events closed, want 0",
			wrongClose,
		)
	}

	// Every poison event burned its full retry budget before closing:
	// maxRetries MarkFailed rounds, then the final attempt closes it.
	wrongAttempts := queryInt(
		t,
		db,
		`SELECT COUNT(*)::int FROM outbox_events
		 WHERE status = 'CLOSE' AND attempts <> $1`,
		maxRetries,
	)

	if wrongAttempts != 0 {
		t.Errorf(
			"%d closed events did not use the full retry budget, want 0",
			wrongAttempts,
		)
	}

	wantInjected := poisonEvents * (maxRetries + 1)

	if got := injector.InjectedFailures(); got != wantInjected {
		t.Errorf(
			"injected failures = %d, want %d (%d poison × %d attempts)",
			got,
			wantInjected,
			poisonEvents,
			maxRetries+1,
		)
	}
}

// ========================================
// Part C — repository-level claim stamping and reclaim
// ========================================

func TestOutboxRepositoryReclaimStale(t *testing.T) {
	db := newConcurrencyTestDB(t)
	repository := postgres.NewOutboxRepository(db)
	ctx := context.Background()

	const total = 20

	seedPendingOutboxEvents(t, db, total)

	claimed, err := repository.ClaimPending(ctx, total)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}

	if len(claimed) != total {
		t.Fatalf("claimed %d events, want %d", len(claimed), total)
	}

	unstamped := queryInt(
		t,
		db,
		`SELECT COUNT(*)::int FROM outbox_events
		 WHERE status = 'PROCESSING' AND processing_at IS NULL`,
	)

	if unstamped != 0 {
		t.Errorf(
			"%d PROCESSING rows without processing_at, want 0",
			unstamped,
		)
	}

	// Fresh rows are not stale yet.
	reclaimed, err := repository.ReclaimStale(ctx, time.Hour)
	if err != nil {
		t.Fatalf("ReclaimStale(1h): %v", err)
	}

	if reclaimed != 0 {
		t.Errorf(
			"ReclaimStale(1h) reclaimed %d fresh rows, want 0",
			reclaimed,
		)
	}

	time.Sleep(100 * time.Millisecond)

	reclaimed, err = repository.ReclaimStale(
		ctx,
		50*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("ReclaimStale(50ms): %v", err)
	}

	if reclaimed != total {
		t.Errorf(
			"ReclaimStale reclaimed %d rows, want %d",
			reclaimed,
			total,
		)
	}

	counts := countOutboxByStatus(t, db)

	if counts.pending != total || counts.processing != 0 {
		t.Errorf(
			"after reclaim pending=%d processing=%d, want %d/0",
			counts.pending,
			counts.processing,
			total,
		)
	}

	// Reclaim must not touch the retry bookkeeping.
	if sum := queryInt(
		t,
		db,
		"SELECT COALESCE(SUM(attempts), 0)::int FROM outbox_events",
	); sum != 0 {
		t.Errorf("SUM(attempts) = %d after reclaim, want 0", sum)
	}

	// Reclaimed rows are immediately claimable again.
	reclaimedEvents, err := repository.ClaimPending(ctx, total)
	if err != nil {
		t.Fatalf("ClaimPending after reclaim: %v", err)
	}

	if len(reclaimedEvents) != total {
		t.Errorf(
			"re-claimed %d events, want %d",
			len(reclaimedEvents),
			total,
		)
	}
}

// ========================================
// Part C + D — a real crash: force-canceled mid-publish, no cleanup
// ========================================

func TestOutboxWorkerCrashIsRecoveredByReclaim(t *testing.T) {
	db := newConcurrencyTestDB(t)
	repository := postgres.NewOutboxRepository(db)

	const (
		totalEvents = 40
		batchSize   = 10
	)

	seedPendingOutboxEvents(t, db, totalEvents)

	// Phase 1: one worker claims a batch and hangs inside Publish. The
	// short shutdown timeout then force-cancels its in-flight context —
	// the moral equivalent of kill -9 mid-publish. MarkFailed never
	// runs, so the whole batch stays PROCESSING.
	crashCtx, crashCancel := context.WithCancel(context.Background())
	defer crashCancel()

	crashGroup, crashErrs := startOutboxWorkersCfg(
		crashCtx,
		repository,
		blockingPublisher{},
		workerFixture{
			workers:         1,
			batchSize:       batchSize,
			interval:        5 * time.Millisecond,
			shutdownTimeout: 100 * time.Millisecond,
		},
	)

	waitProcessingCount(t, db, batchSize, 30*time.Second)

	crashResults := stopWorkersAndCollect(
		t, crashCancel, crashGroup, crashErrs,
	)

	for _, err := range crashResults {
		if !errors.Is(err, ErrShutdownTimeout) {
			t.Errorf(
				"crashed worker returned %v, want ErrShutdownTimeout "+
					"(anything else means cleanup ran)",
				err,
			)
		}
	}

	counts := countOutboxByStatus(t, db)

	if counts.processing != batchSize {
		t.Fatalf(
			"stuck PROCESSING = %d, want %d",
			counts.processing,
			batchSize,
		)
	}

	if counts.published != 0 {
		t.Fatalf(
			"published = %d before recovery, want 0",
			counts.published,
		)
	}

	// Phase 2: let the rows cross the stale threshold, then start
	// healthy workers. They must reclaim the stuck batch and publish
	// everything exactly once.
	const staleTimeout = 500 * time.Millisecond

	time.Sleep(staleTimeout + 200*time.Millisecond)

	counting := newCountingPublisher()

	recoverCtx, recoverCancel := context.WithCancel(
		context.Background(),
	)
	defer recoverCancel()

	recoverGroup, recoverErrs := startOutboxWorkersCfg(
		recoverCtx,
		repository,
		counting,
		workerFixture{
			workers:         2,
			batchSize:       batchSize,
			interval:        10 * time.Millisecond,
			shutdownTimeout: 10 * time.Second,
			staleTimeout:    staleTimeout,
		},
	)

	final := waitOutboxDrained(t, db, 60*time.Second)

	for _, err := range stopWorkersAndCollect(
		t, recoverCancel, recoverGroup, recoverErrs,
	) {
		if err != nil {
			t.Errorf("recovery worker returned %v, want nil", err)
		}
	}

	if final.published != totalEvents {
		t.Errorf(
			"published = %d, want %d",
			final.published,
			totalEvents,
		)
	}

	if final.pending != 0 || final.processing != 0 {
		t.Errorf(
			"leftover pending=%d processing=%d, want 0/0",
			final.pending,
			final.processing,
		)
	}

	if final.closed != 0 {
		t.Errorf("CLOSE = %d, want 0", final.closed)
	}

	if got := counting.duplicates(); got != 0 {
		t.Errorf("duplicated publishes = %d, want 0", got)
	}

	if got := counting.distinct(); got != totalEvents {
		t.Errorf(
			"distinct published = %d, want %d",
			got,
			totalEvents,
		)
	}
}

// ========================================
// Part E — concurrent reclaim
// ========================================

func TestReclaimStaleConcurrentCallersReclaimEachRowOnce(t *testing.T) {
	db := newConcurrencyTestDB(t)
	repository := postgres.NewOutboxRepository(db)
	ctx := context.Background()

	const (
		total     = 200
		batch     = 50
		callers   = 4
		staleWait = 50 * time.Millisecond
	)

	seedPendingOutboxEvents(t, db, total)

	// Strand every row in PROCESSING, as if the claiming workers died.
	for claimed := 0; claimed < total; {
		events, err := repository.ClaimPending(ctx, batch)
		if err != nil {
			t.Fatalf("ClaimPending: %v", err)
		}

		if len(events) == 0 {
			t.Fatalf(
				"claimed only %d of %d events",
				claimed,
				total,
			)
		}

		claimed += len(events)
	}

	time.Sleep(2 * staleWait)

	reclaimedCounts := make([]int, callers)
	errs := make([]error, callers)

	var group sync.WaitGroup

	for i := 0; i < callers; i++ {
		group.Add(1)

		go func() {
			defer group.Done()

			reclaimedCounts[i], errs[i] = repository.ReclaimStale(
				ctx,
				staleWait,
			)
		}()
	}

	group.Wait()

	sum := 0

	for i := 0; i < callers; i++ {
		if errs[i] != nil {
			t.Errorf("concurrent ReclaimStale: %v", errs[i])
		}

		sum += reclaimedCounts[i]
	}

	// Each stale row must be handed to exactly one reclaimer.
	if sum != total {
		t.Errorf(
			"reclaimers got %d rows total (%v), want exactly %d",
			sum,
			reclaimedCounts,
			total,
		)
	}

	counts := countOutboxByStatus(t, db)

	if counts.pending != total || counts.processing != 0 {
		t.Errorf(
			"after concurrent reclaim pending=%d processing=%d, want %d/0",
			counts.pending,
			counts.processing,
			total,
		)
	}
}

func TestOutboxWorkersReclaimStaleWhileProcessing(t *testing.T) {
	db := newConcurrencyTestDB(t)
	repository := postgres.NewOutboxRepository(db)

	const (
		totalEvents  = 250
		strandedRows = 50
		workers      = 4
		batchSize    = 10
		staleTimeout = 500 * time.Millisecond
	)

	seedPendingOutboxEvents(t, db, totalEvents)

	// Strand one slice of the queue in PROCESSING before the fleet
	// starts, as if a previous worker crashed with it.
	stranded, err := repository.ClaimPending(
		context.Background(),
		strandedRows,
	)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}

	if len(stranded) != strandedRows {
		t.Fatalf(
			"stranded %d rows, want %d",
			len(stranded),
			strandedRows,
		)
	}

	time.Sleep(staleTimeout + 200*time.Millisecond)

	counting := newCountingPublisher()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// All four workers run reclaim on every tick while also claiming
	// and publishing, so reclaim races reclaim AND normal processing.
	group, errs := startOutboxWorkersCfg(
		ctx,
		repository,
		counting,
		workerFixture{
			workers:         workers,
			batchSize:       batchSize,
			interval:        10 * time.Millisecond,
			shutdownTimeout: 10 * time.Second,
			staleTimeout:    staleTimeout,
		},
	)

	counts := waitOutboxDrained(t, db, 60*time.Second)

	for _, err := range stopWorkersAndCollect(t, cancel, group, errs) {
		if err != nil {
			t.Errorf("worker returned %v, want nil", err)
		}
	}

	if counts.published != totalEvents {
		t.Errorf(
			"published = %d, want %d",
			counts.published,
			totalEvents,
		)
	}

	if counts.pending != 0 || counts.processing != 0 {
		t.Errorf(
			"leftover pending=%d processing=%d, want 0/0",
			counts.pending,
			counts.processing,
		)
	}

	if got := counting.duplicates(); got != 0 {
		t.Errorf("duplicated publishes = %d, want 0", got)
	}

	if got := counting.distinct(); got != totalEvents {
		t.Errorf(
			"distinct published = %d, want %d",
			got,
			totalEvents,
		)
	}
}
