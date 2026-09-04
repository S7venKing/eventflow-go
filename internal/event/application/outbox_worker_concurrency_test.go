package application

// Concurrency tests for running several outbox workers in one process.
//
// No event is ever assigned to a worker: every worker calls ClaimPending
// against the same table and Postgres arbitrates through
// FOR UPDATE SKIP LOCKED. These tests assert that arbitration holds under
// load - each event is published exactly once, none is lost, and a
// shutdown mid-flight leaves nothing stuck in PROCESSING.
//
// Connection is resolved in order: TEST_DATABASE_URL, DATABASE_URL
// (including the repo root .env), then the docker-compose default. Each
// test creates and drops its own database, so the dev database is never
// touched. When no Postgres is reachable the tests skip instead of
// failing.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/s7venking/eventflow/internal/config"
	"github.com/s7venking/eventflow/internal/event/domain"
	"github.com/s7venking/eventflow/internal/metrics"
	"github.com/s7venking/eventflow/internal/platform/postgres"
)

const concurrencyDefaultDatabaseURL = "postgres://eventflow:eventflow@localhost:5452/eventflow?sslmode=disable"

// ========================================
// Test database lifecycle
// ========================================

// newConcurrencyTestDB creates a throwaway database for one test and drops
// it on cleanup. It deliberately avoids TestMain so it cannot collide with
// another harness in this package.
func newConcurrencyTestDB(t *testing.T) *postgres.DB {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	baseURL := resolveConcurrencyDatabaseURL()

	admin, err := openConcurrencyPool(ctx, baseURL, 2)
	if err != nil {
		t.Skipf(
			"skipping concurrency test, no reachable Postgres: %v",
			err,
		)
	}

	name := fmt.Sprintf(
		"eventflow_conc_%d",
		time.Now().UnixNano(),
	)

	if _, err := admin.Pool.Exec(
		ctx,
		"CREATE DATABASE "+name,
	); err != nil {
		admin.Close()
		t.Fatalf("create test database: %v", err)
	}

	testURL, err := withConcurrencyDatabaseName(baseURL, name)
	if err != nil {
		admin.Close()
		t.Fatalf("build test database URL: %v", err)
	}

	// One connection per worker plus headroom for the polling helpers, so
	// connection acquisition never masks a claim conflict.
	db, err := openConcurrencyPool(ctx, testURL, 12)
	if err != nil {
		admin.Close()
		t.Fatalf("connect to test database: %v", err)
	}

	if err := applyConcurrencyMigrations(ctx, db); err != nil {
		db.Close()
		admin.Close()
		t.Fatalf("apply migrations: %v", err)
	}

	t.Cleanup(func() {
		db.Close()

		dropCtx, dropCancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer dropCancel()

		if _, err := admin.Pool.Exec(
			dropCtx,
			fmt.Sprintf(
				"DROP DATABASE IF EXISTS %s WITH (FORCE)",
				name,
			),
		); err != nil {
			t.Logf("drop test database %s: %v", name, err)
		}

		admin.Close()
	})

	return db
}

func resolveConcurrencyDatabaseURL() string {
	if v := os.Getenv("TEST_DATABASE_URL"); v != "" {
		return v
	}

	if root, err := concurrencyRepoRoot(); err == nil {
		// Load does not override variables already set in the environment.
		_ = godotenv.Load(filepath.Join(root, ".env"))
	}

	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}

	return concurrencyDefaultDatabaseURL
}

func openConcurrencyPool(
	ctx context.Context,
	databaseURL string,
	maxConns int32,
) (*postgres.DB, error) {
	db, err := postgres.NewDB(ctx, config.DatabaseConfig{
		URL:             databaseURL,
		MaxConns:        maxConns,
		MinConns:        1,
		MaxConnLifetime: time.Hour,
		MaxConnIdleTime: time.Hour,
	})
	if err != nil {
		return nil, err
	}

	if err := db.Ping(ctx); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func withConcurrencyDatabaseName(
	rawURL string,
	name string,
) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse database URL: %w", err)
	}

	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf(
			"database URL must be in postgres://user:pass@host:port/db form",
		)
	}

	u.Path = "/" + name

	return u.String(), nil
}

func concurrencyRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		if _, err := os.Stat(
			filepath.Join(dir, "go.mod"),
		); err == nil {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf(
				"go.mod not found above working directory",
			)
		}

		dir = parent
	}
}

// applyConcurrencyMigrations runs the repo's migrations against the
// throwaway database. db.Migrate cannot be used here: it resolves the
// migration directory relative to the working directory, which is the
// package directory during tests.
func applyConcurrencyMigrations(
	ctx context.Context,
	db *postgres.DB,
) error {
	root, err := concurrencyRepoRoot()
	if err != nil {
		return err
	}

	migrationDir := filepath.Join(root, "migrations")

	entries, err := os.ReadDir(migrationDir)
	if err != nil {
		return fmt.Errorf("read migration directory: %w", err)
	}

	names := make([]string, 0, len(entries))

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		names = append(names, entry.Name())
	}

	sort.Strings(names)

	for _, name := range names {
		content, err := os.ReadFile(
			filepath.Join(migrationDir, name),
		)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}

		if _, err := db.Pool.Exec(
			ctx,
			string(content),
		); err != nil {
			return fmt.Errorf("run migration %s: %w", name, err)
		}
	}

	return nil
}

// ========================================
// Fixtures and helpers
// ========================================

// countingPublisher records every publish so the test can tell an
// exactly-once run from a run where two workers processed the same row.
type countingPublisher struct {
	mu    sync.Mutex
	calls map[uuid.UUID]int
	total int
}

func newCountingPublisher() *countingPublisher {
	return &countingPublisher{
		calls: make(map[uuid.UUID]int),
	}
}

func (p *countingPublisher) Publish(
	_ context.Context,
	event domain.OutboxEvent,
	_ *slog.Logger,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.calls[event.ID]++
	p.total++

	return nil
}

// duplicates returns how many events were handed to the publisher more
// than once.
func (p *countingPublisher) duplicates() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	duplicates := 0

	for _, n := range p.calls {
		if n > 1 {
			duplicates++
		}
	}

	return duplicates
}

func (p *countingPublisher) distinct() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.calls)
}

func (p *countingPublisher) totalPublishes() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.total
}

// discardLogger keeps a thousand published-event lines out of the test
// output without changing what the worker does.
func discardLogger() *slog.Logger {
	return slog.New(
		slog.NewTextHandler(io.Discard, nil),
	)
}

// seedPendingOutboxEvents inserts count ready-to-claim PENDING events in a
// single round trip.
func seedPendingOutboxEvents(
	t *testing.T,
	db *postgres.DB,
	count int,
) {
	t.Helper()

	seedTypedPendingOutboxEvents(t, db, count, "test.event")
}

// seedTypedPendingOutboxEvents is seedPendingOutboxEvents with an explicit
// event type, so failure tests can mark a subset of events poison.
func seedTypedPendingOutboxEvents(
	t *testing.T,
	db *postgres.DB,
	count int,
	eventType string,
) {
	t.Helper()

	const query = `
		WITH seeded_events AS (
			INSERT INTO events (
				id, event_id, type, version, source, timestamp, properties
			)
			SELECT
				gen_random_uuid(),
				gen_random_uuid(),
				$2,
				1,
				'concurrency-test',
				NOW(),
				'{}'
			FROM generate_series(1, $1)
			RETURNING event_id
		)
		INSERT INTO outbox_events (
			id, event_id, event_type, payload,
			status, attempts, available_at, created_at
		)
		SELECT
			gen_random_uuid(),
			event_id,
			$2,
			'{}',
			'PENDING',
			0,
			NOW() - INTERVAL '1 second',
			NOW()
		FROM seeded_events
	`

	if _, err := db.Pool.Exec(
		context.Background(),
		query,
		count,
		eventType,
	); err != nil {
		t.Fatalf(
			"seed %d pending %s events: %v",
			count,
			eventType,
			err,
		)
	}
}

type outboxStatusCounts struct {
	pending    int
	processing int
	published  int
	closed     int
}

func (c outboxStatusCounts) total() int {
	return c.pending + c.processing + c.published + c.closed
}

func countOutboxByStatus(
	t *testing.T,
	db *postgres.DB,
) outboxStatusCounts {
	t.Helper()

	const query = `
		SELECT
			COUNT(*) FILTER (WHERE status = 'PENDING'),
			COUNT(*) FILTER (WHERE status = 'PROCESSING'),
			COUNT(*) FILTER (WHERE status = 'PUBLISHED'),
			COUNT(*) FILTER (WHERE status = 'CLOSE')
		FROM outbox_events
	`

	var counts outboxStatusCounts

	if err := db.Pool.QueryRow(
		context.Background(),
		query,
	).Scan(
		&counts.pending,
		&counts.processing,
		&counts.published,
		&counts.closed,
	); err != nil {
		t.Fatalf("count outbox events: %v", err)
	}

	return counts
}

// workerFixture describes one fleet of workers for a test. Zero values
// fall back to the defaults main.go uses, except staleTimeout, which
// stays 0 (reclaim disabled) unless a test opts in.
type workerFixture struct {
	workers         int
	batchSize       int
	interval        time.Duration
	maxRetries      int
	retryBaseDelay  time.Duration
	retryMaxDelay   time.Duration
	shutdownTimeout time.Duration
	staleTimeout    time.Duration
}

func (f workerFixture) withDefaults() workerFixture {
	if f.maxRetries == 0 {
		f.maxRetries = 3
	}

	if f.retryBaseDelay == 0 {
		f.retryBaseDelay = time.Second
	}

	if f.retryMaxDelay == 0 {
		f.retryMaxDelay = 30 * time.Second
	}

	return f
}

// startOutboxWorkers launches count workers sharing one repository, one
// pool, one publisher and one metrics set, exactly as main.go wires them.
func startOutboxWorkers(
	ctx context.Context,
	repository *postgres.OutboxRepository,
	publisher EventPublisher,
	count int,
	batchSize int,
	interval time.Duration,
	shutdownTimeout time.Duration,
) (*sync.WaitGroup, chan error) {
	return startOutboxWorkersCfg(
		ctx,
		repository,
		publisher,
		workerFixture{
			workers:         count,
			batchSize:       batchSize,
			interval:        interval,
			shutdownTimeout: shutdownTimeout,
		},
	)
}

func startOutboxWorkersCfg(
	ctx context.Context,
	repository *postgres.OutboxRepository,
	publisher EventPublisher,
	fixture workerFixture,
) (*sync.WaitGroup, chan error) {
	fixture = fixture.withDefaults()

	outboxMetrics := metrics.NewOutboxMetrics(
		prometheus.NewRegistry(),
	)

	logger := discardLogger()

	var group sync.WaitGroup

	errs := make(chan error, fixture.workers)

	for i := 1; i <= fixture.workers; i++ {
		worker := NewOutboxWorker(
			repository,
			publisher,
			fixture.interval,
			fixture.batchSize,
			fixture.maxRetries,
			fixture.retryBaseDelay,
			fixture.retryMaxDelay,
			fixture.shutdownTimeout,
			fixture.staleTimeout,
			outboxMetrics,
			logger.With("worker_id", i),
		)

		group.Add(1)

		go func() {
			defer group.Done()

			errs <- worker.Run(ctx)
		}()
	}

	return &group, errs
}

func waitOutboxDrained(
	t *testing.T,
	db *postgres.DB,
	timeout time.Duration,
) outboxStatusCounts {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for {
		counts := countOutboxByStatus(t, db)

		if counts.pending == 0 && counts.processing == 0 {
			return counts
		}

		if time.Now().After(deadline) {
			t.Fatalf(
				"outbox not drained within %s: pending=%d processing=%d published=%d closed=%d",
				timeout,
				counts.pending,
				counts.processing,
				counts.published,
				counts.closed,
			)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func waitSomePublished(
	t *testing.T,
	publisher *countingPublisher,
	timeout time.Duration,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if publisher.totalPublishes() > 0 {
			return
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatalf("no publish started within %s", timeout)
}

func waitWorkerGroupDone(
	t *testing.T,
	group *sync.WaitGroup,
	timeout time.Duration,
) {
	t.Helper()

	done := make(chan struct{})

	go func() {
		group.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("workers did not stop within %s", timeout)
	}
}

// ========================================
// Many workers, exactly-once delivery
// ========================================

func TestOutboxWorkersConcurrentNoDuplicateNoLoss(t *testing.T) {
	db := newConcurrencyTestDB(t)

	const (
		totalEvents = 1000
		workers     = 4
		batchSize   = 10
	)

	seedPendingOutboxEvents(t, db, totalEvents)

	publisher := newCountingPublisher()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	group, errs := startOutboxWorkers(
		ctx,
		postgres.NewOutboxRepository(db),
		publisher,
		workers,
		batchSize,
		5*time.Millisecond,
		10*time.Second,
	)

	counts := waitOutboxDrained(t, db, 120*time.Second)

	cancel()
	waitWorkerGroupDone(t, group, 15*time.Second)
	close(errs)

	for err := range errs {
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

	if counts.pending != 0 {
		t.Errorf("remaining PENDING = %d, want 0", counts.pending)
	}

	if counts.processing != 0 {
		t.Errorf("remaining PROCESSING = %d, want 0", counts.processing)
	}

	if counts.closed != 0 {
		t.Errorf("CLOSE (failed) = %d, want 0", counts.closed)
	}

	if lost := totalEvents - counts.total(); lost != 0 {
		t.Errorf("lost = %d, want 0", lost)
	}

	if got := publisher.duplicates(); got != 0 {
		t.Errorf(
			"%d events were published more than once, want 0",
			got,
		)
	}

	if got := publisher.distinct(); got != totalEvents {
		t.Errorf(
			"distinct events published = %d, want %d",
			got,
			totalEvents,
		)
	}

	if got := publisher.totalPublishes(); got != totalEvents {
		t.Errorf(
			"total publish calls = %d, want %d",
			got,
			totalEvents,
		)
	}
}

// ========================================
// Many workers, graceful shutdown
// ========================================

func TestOutboxWorkersGracefulShutdownLosesNothing(t *testing.T) {
	db := newConcurrencyTestDB(t)

	const (
		totalEvents = 300
		workers     = 4
		batchSize   = 10
	)

	seedPendingOutboxEvents(t, db, totalEvents)

	publisher := newCountingPublisher()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	group, errs := startOutboxWorkers(
		ctx,
		postgres.NewOutboxRepository(db),
		publisher,
		workers,
		batchSize,
		5*time.Millisecond,
		10*time.Second,
	)

	// Cancel while the workers are mid-run, so shutdown overlaps in-flight
	// batches instead of hitting an already-idle worker.
	waitSomePublished(t, publisher, 30*time.Second)

	cancel()

	waitWorkerGroupDone(t, group, 15*time.Second)

	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("worker returned %v, want nil (clean drain)", err)
		}
	}

	counts := countOutboxByStatus(t, db)

	// A claimed batch always runs to completion on the detached context,
	// so a clean shutdown never strands a row in PROCESSING.
	if counts.processing != 0 {
		t.Errorf(
			"PROCESSING after shutdown = %d, want 0",
			counts.processing,
		)
	}

	if counts.total() != totalEvents {
		t.Errorf(
			"total rows = %d, want %d (events were lost)",
			counts.total(),
			totalEvents,
		)
	}

	if counts.published == 0 {
		t.Error("no events were published before shutdown")
	}

	if got := publisher.duplicates(); got != 0 {
		t.Errorf(
			"%d events were published more than once, want 0",
			got,
		)
	}

	if got := publisher.totalPublishes(); got != counts.published {
		t.Errorf(
			"publish calls = %d, PUBLISHED rows = %d, want equal",
			got,
			counts.published,
		)
	}
}
