package application

// Integration tests for the full outbox worker flow against a real Postgres.
//
// Connection is resolved in order: TEST_DATABASE_URL, DATABASE_URL (including
// the repo root .env), then the docker-compose default
// (postgres://eventflow:eventflow@localhost:5452/eventflow). A dedicated
// database is created per test run and dropped afterwards, so the dev
// database is never touched. When no Postgres is reachable, every
// integration test skips instead of failing.

import (
	"context"
	"errors"
	"fmt"
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

	"github.com/s7venking/eventflow/internal/config"
	"github.com/s7venking/eventflow/internal/event/domain"
	"github.com/s7venking/eventflow/internal/platform/postgres"
)

const composeDefaultDatabaseURL = "postgres://eventflow:eventflow@localhost:5452/eventflow?sslmode=disable"

var (
	integrationOnce   sync.Once
	integrationDB     *postgres.DB
	integrationErr    error
	integrationAdmin  *postgres.DB
	integrationDBName string
)

func TestMain(m *testing.M) {
	code := m.Run()
	teardownIntegrationDB()
	os.Exit(code)
}

// ========================================
// Test database lifecycle
// ========================================

func acquireIntegrationDB(t *testing.T) *postgres.DB {
	t.Helper()

	integrationOnce.Do(setupIntegrationDB)

	if integrationErr != nil {
		t.Skipf(
			"skipping integration test, no reachable Postgres: %v",
			integrationErr,
		)
	}

	if _, err := integrationDB.Pool.Exec(
		context.Background(),
		"TRUNCATE TABLE outbox_events, events",
	); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}

	return integrationDB
}

func setupIntegrationDB() {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	baseURL := resolveDatabaseURL()

	admin, err := openPool(ctx, baseURL)
	if err != nil {
		integrationErr = err
		return
	}

	name := fmt.Sprintf(
		"eventflow_it_%d",
		time.Now().UnixNano(),
	)

	if _, err := admin.Pool.Exec(
		ctx,
		"CREATE DATABASE "+name,
	); err != nil {
		admin.Close()
		integrationErr = fmt.Errorf(
			"create test database: %w",
			err,
		)
		return
	}

	testURL, err := withDatabaseName(baseURL, name)
	if err != nil {
		admin.Close()
		integrationErr = err
		return
	}

	db, err := openPool(ctx, testURL)
	if err != nil {
		admin.Close()
		integrationErr = err
		return
	}

	if err := applyMigrations(ctx, db); err != nil {
		db.Close()
		admin.Close()
		integrationErr = err
		return
	}

	integrationAdmin = admin
	integrationDB = db
	integrationDBName = name
}

func teardownIntegrationDB() {
	if integrationDB != nil {
		integrationDB.Close()
	}

	if integrationAdmin == nil {
		return
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	if integrationDBName != "" {
		_, _ = integrationAdmin.Pool.Exec(
			ctx,
			fmt.Sprintf(
				"DROP DATABASE IF EXISTS %s WITH (FORCE)",
				integrationDBName,
			),
		)
	}

	integrationAdmin.Close()
}

func resolveDatabaseURL() string {
	if v := os.Getenv("TEST_DATABASE_URL"); v != "" {
		return v
	}

	if root, err := repoRoot(); err == nil {
		// Load does not override variables already set in the environment.
		_ = godotenv.Load(filepath.Join(root, ".env"))
	}

	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}

	return composeDefaultDatabaseURL
}

func openPool(
	ctx context.Context,
	databaseURL string,
) (*postgres.DB, error) {
	db, err := postgres.NewDB(ctx, config.DatabaseConfig{
		URL:             databaseURL,
		MaxConns:        5,
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

func withDatabaseName(
	rawURL string,
	name string,
) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf(
			"parse database URL: %w",
			err,
		)
	}

	if u.Scheme == "" || u.Host == "" {
		return "", errors.New(
			"database URL must be in postgres://user:pass@host:port/db form",
		)
	}

	u.Path = "/" + name

	return u.String(), nil
}

func repoRoot() (string, error) {
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
			return "", errors.New(
				"go.mod not found above working directory",
			)
		}

		dir = parent
	}
}

func applyMigrations(
	ctx context.Context,
	db *postgres.DB,
) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}

	dir := filepath.Join(root, "migrations")

	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf(
			"read migration directory: %w",
			err,
		)
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
			filepath.Join(dir, name),
		)
		if err != nil {
			return fmt.Errorf(
				"read migration %s: %w",
				name,
				err,
			)
		}

		if _, err := db.Pool.Exec(
			ctx,
			string(content),
		); err != nil {
			return fmt.Errorf(
				"run migration %s: %w",
				name,
				err,
			)
		}
	}

	return nil
}

// ========================================
// Fixtures and helpers
// ========================================

func insertOutboxEvent(
	t *testing.T,
	db *postgres.DB,
	status string,
	attempts int,
	availableAt time.Time,
	createdAt time.Time,
) uuid.UUID {
	t.Helper()

	ctx := context.Background()
	eventID := uuid.New()

	if _, err := db.Pool.Exec(
		ctx,
		`
		INSERT INTO events (
			id, event_id, type, version, source, timestamp, properties
		)
		VALUES ($1, $2, 'test.event', 1, 'integration-test', NOW(), '{}')
		`,
		uuid.New(),
		eventID,
	); err != nil {
		t.Fatalf("insert event fixture: %v", err)
	}

	id := uuid.New()

	if _, err := db.Pool.Exec(
		ctx,
		`
		INSERT INTO outbox_events (
			id, event_id, event_type, payload,
			status, attempts, available_at, created_at
		)
		VALUES ($1, $2, 'test.event', '{}', $3, $4, $5, $6)
		`,
		id,
		eventID,
		status,
		attempts,
		availableAt,
		createdAt,
	); err != nil {
		t.Fatalf("insert outbox fixture: %v", err)
	}

	return id
}

func insertReadyEvent(
	t *testing.T,
	db *postgres.DB,
) uuid.UUID {
	t.Helper()

	return insertOutboxEvent(
		t,
		db,
		"PENDING",
		0,
		time.Now().Add(-time.Minute),
		time.Now(),
	)
}

type outboxRow struct {
	status      string
	attempts    int
	availableAt time.Time
	publishedAt *time.Time
	lastError   *string
}

func readOutboxRow(
	t *testing.T,
	db *postgres.DB,
	id uuid.UUID,
) outboxRow {
	t.Helper()

	var row outboxRow

	if err := db.Pool.QueryRow(
		context.Background(),
		`
		SELECT status, attempts, available_at, published_at, last_error
		FROM outbox_events
		WHERE id = $1
		`,
		id,
	).Scan(
		&row.status,
		&row.attempts,
		&row.availableAt,
		&row.publishedAt,
		&row.lastError,
	); err != nil {
		t.Fatalf("read outbox row %s: %v", id, err)
	}

	return row
}

// forceClaimable rewinds available_at so the next ClaimPending picks the
// event up immediately, instead of the test sleeping through real backoff.
func forceClaimable(
	t *testing.T,
	db *postgres.DB,
	id uuid.UUID,
) {
	t.Helper()

	tag, err := db.Pool.Exec(
		context.Background(),
		`
		UPDATE outbox_events
		SET available_at = NOW() - INTERVAL '1 second'
		WHERE id = $1
		  AND status = 'PENDING'
		`,
		id,
	)
	if err != nil {
		t.Fatalf("force claimable: %v", err)
	}

	if tag.RowsAffected() == 0 {
		t.Fatalf("force claimable: event %s is not PENDING", id)
	}
}

// waitUntilClaimable polls the database clock, the same clock ClaimPending
// compares against, so the wait is immune to app/DB clock skew.
func waitUntilClaimable(
	t *testing.T,
	db *postgres.DB,
	id uuid.UUID,
) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		var ready bool

		if err := db.Pool.QueryRow(
			context.Background(),
			`
			SELECT available_at <= NOW()
			FROM outbox_events
			WHERE id = $1
			`,
			id,
		).Scan(&ready); err != nil {
			t.Fatalf("check claimable: %v", err)
		}

		if ready {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("event %s never became claimable", id)
}

type fakePublisher struct {
	mu       sync.Mutex
	failAll  bool
	failures map[uuid.UUID]int
	calls    map[uuid.UUID]int
}

func (p *fakePublisher) Publish(
	_ context.Context,
	event domain.OutboxEvent,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.calls == nil {
		p.calls = make(map[uuid.UUID]int)
	}

	p.calls[event.ID]++

	if p.failAll {
		return errors.New("publish failed")
	}

	if p.failures[event.ID] > 0 {
		p.failures[event.ID]--
		return errors.New("publish failed")
	}

	return nil
}

func (p *fakePublisher) callCount(id uuid.UUID) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.calls[id]
}

func (p *fakePublisher) totalCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	total := 0

	for _, n := range p.calls {
		total += n
	}

	return total
}

func newIntegrationWorker(
	db *postgres.DB,
	publisher EventPublisher,
	batchSize int,
	maxRetries int,
	retryBaseDelay time.Duration,
	retryMaxDelay time.Duration,
) *OutboxWorker {
	return NewOutboxWorker(
		postgres.NewOutboxRepository(db),
		publisher,
		time.Hour,
		batchSize,
		maxRetries,
		retryBaseDelay,
		retryMaxDelay,
		30*time.Second,
	)
}

// ========================================
// Worker flow: success
// ========================================

func TestOutboxWorkerPublishSuccess(t *testing.T) {
	db := acquireIntegrationDB(t)
	id := insertReadyEvent(t, db)

	pub := &fakePublisher{}
	w := newIntegrationWorker(db, pub, 10, 3, time.Second, 30*time.Second)

	if err := w.process(context.Background()); err != nil {
		t.Fatalf("process: %v", err)
	}

	row := readOutboxRow(t, db, id)

	if row.status != "PUBLISHED" {
		t.Errorf("status = %s, want PUBLISHED", row.status)
	}

	if row.attempts != 0 {
		t.Errorf("attempts = %d, want 0", row.attempts)
	}

	if row.publishedAt == nil {
		t.Error("published_at is NULL, want set")
	}

	if got := pub.callCount(id); got != 1 {
		t.Errorf("publish calls = %d, want 1", got)
	}

	// A published event must never be claimed again.
	if err := w.process(context.Background()); err != nil {
		t.Fatalf("second process: %v", err)
	}

	if got := pub.callCount(id); got != 1 {
		t.Errorf("publish calls after second cycle = %d, want 1", got)
	}
}

// ========================================
// Worker flow: failure schedules retry
// ========================================

func TestOutboxWorkerFailureSchedulesRetryWithBackoff(t *testing.T) {
	db := acquireIntegrationDB(t)
	id := insertReadyEvent(t, db)

	base := time.Second

	pub := &fakePublisher{failAll: true}
	w := newIntegrationWorker(db, pub, 10, 3, base, 30*time.Second)

	before := time.Now()

	if err := w.process(context.Background()); err != nil {
		t.Fatalf("process: %v", err)
	}

	after := time.Now()

	row := readOutboxRow(t, db, id)

	if row.status != "PENDING" {
		t.Errorf("status = %s, want PENDING", row.status)
	}

	if row.attempts != 1 {
		t.Errorf("attempts = %d, want 1", row.attempts)
	}

	if row.publishedAt != nil {
		t.Error("published_at set on failed event")
	}

	if row.lastError == nil ||
		!strings.Contains(*row.lastError, "publish failed") {
		t.Errorf("last_error = %v, want publish failure message", row.lastError)
	}

	// Retry #1 with equal jitter is scheduled within [base/2, base).
	min := before.Add(base / 2).Add(-time.Millisecond)
	max := after.Add(base).Add(time.Millisecond)

	if row.availableAt.Before(min) || row.availableAt.After(max) {
		t.Errorf(
			"available_at = %s, want in [%s, %s]",
			row.availableAt,
			min,
			max,
		)
	}

	// The database itself must consider the event not yet claimable.
	var claimable bool

	if err := db.Pool.QueryRow(
		context.Background(),
		"SELECT available_at <= NOW() FROM outbox_events WHERE id = $1",
		id,
	).Scan(&claimable); err != nil {
		t.Fatalf("check claimable: %v", err)
	}

	if claimable {
		t.Error("event claimable immediately after failure, want backoff")
	}

	// So an immediate next cycle must not touch it.
	if err := w.process(context.Background()); err != nil {
		t.Fatalf("second process: %v", err)
	}

	if got := pub.callCount(id); got != 1 {
		t.Errorf("event claimed before available_at, calls = %d, want 1", got)
	}
}

// ========================================
// Worker flow: retry succeeds after real backoff
// ========================================

func TestOutboxWorkerRetryAfterBackoffSucceeds(t *testing.T) {
	db := acquireIntegrationDB(t)
	id := insertReadyEvent(t, db)

	pub := &fakePublisher{
		failures: map[uuid.UUID]int{id: 1},
	}
	w := newIntegrationWorker(
		db,
		pub,
		10,
		3,
		20*time.Millisecond,
		100*time.Millisecond,
	)

	if err := w.process(context.Background()); err != nil {
		t.Fatalf("process: %v", err)
	}

	if row := readOutboxRow(t, db, id); row.status != "PENDING" || row.attempts != 1 {
		t.Fatalf(
			"after failure: status=%s attempts=%d, want PENDING/1",
			row.status,
			row.attempts,
		)
	}

	waitUntilClaimable(t, db, id)

	if err := w.process(context.Background()); err != nil {
		t.Fatalf("retry process: %v", err)
	}

	row := readOutboxRow(t, db, id)

	if row.status != "PUBLISHED" {
		t.Errorf("status = %s, want PUBLISHED", row.status)
	}

	if row.attempts != 1 {
		t.Errorf("attempts = %d, want 1 (success must not increment)", row.attempts)
	}

	if row.publishedAt == nil {
		t.Error("published_at is NULL, want set")
	}

	if got := pub.callCount(id); got != 2 {
		t.Errorf("publish calls = %d, want 2 (initial + retry #1)", got)
	}
}

// ========================================
// Worker flow: backoff grows exponentially and is capped
// ========================================

func TestOutboxWorkerBackoffGrowsAndIsCapped(t *testing.T) {
	db := acquireIntegrationDB(t)
	id := insertReadyEvent(t, db)

	base := time.Second
	maxDelay := 2 * time.Second

	pub := &fakePublisher{failAll: true}
	w := newIntegrationWorker(db, pub, 10, 10, base, maxDelay)

	windows := []struct {
		min time.Duration
		max time.Duration
	}{
		{min: 500 * time.Millisecond, max: 1 * time.Second}, // retry #1: 1s
		{min: 1 * time.Second, max: 2 * time.Second},        // retry #2: 2s
		{min: 1 * time.Second, max: 2 * time.Second},        // retry #3: 4s capped to 2s
		{min: 1 * time.Second, max: 2 * time.Second},        // retry #4: capped
	}

	for i, window := range windows {
		before := time.Now()

		if err := w.process(context.Background()); err != nil {
			t.Fatalf("process #%d: %v", i+1, err)
		}

		after := time.Now()

		row := readOutboxRow(t, db, id)

		if row.status != "PENDING" {
			t.Fatalf("cycle %d: status = %s, want PENDING", i+1, row.status)
		}

		if row.attempts != i+1 {
			t.Fatalf("cycle %d: attempts = %d, want %d", i+1, row.attempts, i+1)
		}

		min := before.Add(window.min).Add(-time.Millisecond)
		max := after.Add(window.max).Add(time.Millisecond)

		if row.availableAt.Before(min) || row.availableAt.After(max) {
			t.Errorf(
				"retry #%d: available_at = %s, want in [%s, %s]",
				i+1,
				row.availableAt,
				min,
				max,
			)
		}

		forceClaimable(t, db, id)
	}
}

// ========================================
// Worker flow: retries exhausted, event closed
// ========================================

func TestOutboxWorkerExhaustsRetriesThenCloses(t *testing.T) {
	db := acquireIntegrationDB(t)
	id := insertReadyEvent(t, db)

	pub := &fakePublisher{failAll: true}
	w := newIntegrationWorker(db, pub, 10, 3, time.Second, 30*time.Second)

	// Initial attempt + retries #1 and #2 fail, staying PENDING.
	for wantAttempts := 1; wantAttempts <= 3; wantAttempts++ {
		if err := w.process(context.Background()); err != nil {
			t.Fatalf("process: %v", err)
		}

		row := readOutboxRow(t, db, id)

		if row.status != "PENDING" {
			t.Fatalf(
				"attempts=%d: status = %s, want PENDING",
				wantAttempts,
				row.status,
			)
		}

		if row.attempts != wantAttempts {
			t.Fatalf(
				"attempts = %d, want %d",
				row.attempts,
				wantAttempts,
			)
		}

		forceClaimable(t, db, id)
	}

	// attempts == maxRetries: the next failure closes the event.
	if err := w.process(context.Background()); err != nil {
		t.Fatalf("closing process: %v", err)
	}

	row := readOutboxRow(t, db, id)

	if row.status != "CLOSE" {
		t.Errorf("status = %s, want CLOSE", row.status)
	}

	if row.attempts != 3 {
		t.Errorf("attempts = %d, want 3 (close must not increment)", row.attempts)
	}

	if row.publishedAt != nil {
		t.Error("published_at set on closed event")
	}

	if row.lastError == nil {
		t.Error("last_error is NULL on closed event")
	}

	if got := pub.callCount(id); got != 4 {
		t.Errorf("publish calls = %d, want 4 (initial + 3 retries)", got)
	}

	// Closed events must never be claimed again.
	if err := w.process(context.Background()); err != nil {
		t.Fatalf("post-close process: %v", err)
	}

	if got := pub.callCount(id); got != 4 {
		t.Errorf("closed event was claimed again, calls = %d, want 4", got)
	}
}

// ========================================
// Claim semantics
// ========================================

func TestOutboxWorkerClaimsOnlyReadyPendingEvents(t *testing.T) {
	db := acquireIntegrationDB(t)

	now := time.Now()

	ready := insertOutboxEvent(t, db, "PENDING", 0, now.Add(-time.Minute), now)
	future := insertOutboxEvent(t, db, "PENDING", 1, now.Add(time.Hour), now)
	processing := insertOutboxEvent(t, db, "PROCESSING", 1, now.Add(-time.Minute), now)
	published := insertOutboxEvent(t, db, "PUBLISHED", 0, now.Add(-time.Minute), now)
	closed := insertOutboxEvent(t, db, "CLOSE", 3, now.Add(-time.Minute), now)

	pub := &fakePublisher{}
	w := newIntegrationWorker(db, pub, 10, 3, time.Second, 30*time.Second)

	if err := w.process(context.Background()); err != nil {
		t.Fatalf("process: %v", err)
	}

	if got := pub.totalCalls(); got != 1 {
		t.Errorf("total publish calls = %d, want 1", got)
	}

	if got := pub.callCount(ready); got != 1 {
		t.Errorf("ready event calls = %d, want 1", got)
	}

	if row := readOutboxRow(t, db, ready); row.status != "PUBLISHED" {
		t.Errorf("ready event status = %s, want PUBLISHED", row.status)
	}

	untouched := map[string]struct {
		id     uuid.UUID
		status string
	}{
		"future available_at": {id: future, status: "PENDING"},
		// A stale PROCESSING event (e.g. after a crash) must not be
		// reclaimed here; crash recovery is a separate concern.
		"processing": {id: processing, status: "PROCESSING"},
		"published":  {id: published, status: "PUBLISHED"},
		"closed":     {id: closed, status: "CLOSE"},
	}

	for name, want := range untouched {
		if got := pub.callCount(want.id); got != 0 {
			t.Errorf("%s event was published, calls = %d, want 0", name, got)
		}

		if row := readOutboxRow(t, db, want.id); row.status != want.status {
			t.Errorf(
				"%s event status = %s, want %s",
				name,
				row.status,
				want.status,
			)
		}
	}
}

func TestOutboxWorkerRespectsBatchSizeAndOrder(t *testing.T) {
	db := acquireIntegrationDB(t)

	availableAt := time.Now().Add(-time.Hour)
	createdBase := time.Now().Add(-time.Hour)

	ids := make([]uuid.UUID, 5)

	for i := range ids {
		ids[i] = insertOutboxEvent(
			t,
			db,
			"PENDING",
			0,
			availableAt,
			createdBase.Add(time.Duration(i)*time.Second),
		)
	}

	pub := &fakePublisher{}
	w := newIntegrationWorker(db, pub, 2, 3, time.Second, 30*time.Second)

	if err := w.process(context.Background()); err != nil {
		t.Fatalf("process: %v", err)
	}

	if got := pub.totalCalls(); got != 2 {
		t.Errorf("total publish calls = %d, want 2 (batch size)", got)
	}

	// ClaimPending orders by created_at, so the two oldest go first.
	for i, id := range ids {
		row := readOutboxRow(t, db, id)

		want := "PENDING"
		if i < 2 {
			want = "PUBLISHED"
		}

		if row.status != want {
			t.Errorf(
				"event #%d status = %s, want %s",
				i,
				row.status,
				want,
			)
		}
	}
}

func TestOutboxRepositoryConcurrentClaimNoOverlap(t *testing.T) {
	db := acquireIntegrationDB(t)
	repo := postgres.NewOutboxRepository(db)

	const total = 20
	const workers = 4
	const limit = 5

	for i := 0; i < total; i++ {
		insertReadyEvent(t, db)
	}

	results := make([][]domain.OutboxEvent, workers)
	errs := make([]error, workers)

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			results[i], errs[i] = repo.ClaimPending(
				context.Background(),
				limit,
			)
		}(i)
	}

	wg.Wait()

	seen := make(map[uuid.UUID]bool)
	claimed := 0

	for i := 0; i < workers; i++ {
		if errs[i] != nil {
			t.Fatalf("worker %d claim: %v", i, errs[i])
		}

		for _, event := range results[i] {
			if seen[event.ID] {
				t.Errorf("event %s claimed by two workers", event.ID)
			}

			seen[event.ID] = true
			claimed++

			if event.Status != "PROCESSING" {
				t.Errorf(
					"claimed event status = %s, want PROCESSING",
					event.Status,
				)
			}
		}
	}

	if claimed != total {
		t.Errorf("claimed %d events, want %d", claimed, total)
	}

	var pending int

	if err := db.Pool.QueryRow(
		context.Background(),
		"SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING'",
	).Scan(&pending); err != nil {
		t.Fatalf("count pending: %v", err)
	}

	if pending != 0 {
		t.Errorf("%d events still PENDING after claims, want 0", pending)
	}
}

// ========================================
// Repository guards
// ========================================

func TestOutboxRepositoryMarkFailedRequiresProcessing(t *testing.T) {
	db := acquireIntegrationDB(t)
	repo := postgres.NewOutboxRepository(db)

	id := insertReadyEvent(t, db)

	err := repo.MarkFailed(
		context.Background(),
		id,
		"boom",
		time.Now().Add(time.Second),
	)
	if err == nil {
		t.Fatal("MarkFailed on PENDING event succeeded, want error")
	}

	row := readOutboxRow(t, db, id)

	if row.status != "PENDING" || row.attempts != 0 || row.lastError != nil {
		t.Errorf(
			"PENDING event mutated: status=%s attempts=%d last_error=%v",
			row.status,
			row.attempts,
			row.lastError,
		)
	}
}

func TestOutboxRepositoryMarkCloseRequiresProcessing(t *testing.T) {
	db := acquireIntegrationDB(t)
	repo := postgres.NewOutboxRepository(db)

	id := insertReadyEvent(t, db)

	err := repo.MarkClose(
		context.Background(),
		id,
		"boom",
	)
	if err == nil {
		t.Fatal("MarkClose on PENDING event succeeded, want error")
	}

	row := readOutboxRow(t, db, id)

	if row.status != "PENDING" || row.lastError != nil {
		t.Errorf(
			"PENDING event mutated: status=%s last_error=%v",
			row.status,
			row.lastError,
		)
	}
}

func TestOutboxRepositoryMarkPublishedUnknownEvent(t *testing.T) {
	db := acquireIntegrationDB(t)
	repo := postgres.NewOutboxRepository(db)

	if err := repo.MarkPublished(
		context.Background(),
		uuid.New(),
	); err == nil {
		t.Fatal("MarkPublished on unknown id succeeded, want error")
	}
}

// ========================================
// Worker error propagation
// ========================================

func TestOutboxWorkerProcessPropagatesClaimError(t *testing.T) {
	db := acquireIntegrationDB(t)

	w := newIntegrationWorker(
		db,
		&fakePublisher{},
		10,
		3,
		time.Second,
		30*time.Second,
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := w.process(ctx); err == nil {
		t.Fatal("process with canceled context succeeded, want error")
	}
}
