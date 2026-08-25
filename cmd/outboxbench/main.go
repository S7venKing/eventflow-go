// Command outboxbench measures outbox worker throughput at a given
// concurrency level.
//
// It drives the production application.OutboxWorker against a real
// Postgres: no event is assigned to a worker, every worker calls
// ClaimPending and the database arbitrates through
// FOR UPDATE SKIP LOCKED. Two things differ from the running API, both
// held identical across every concurrency level so the runs stay
// comparable:
//
//   - the publisher is in-process, so the numbers reflect the
//     claim/publish/mark pipeline rather than stdout throughput;
//   - the worker logger discards output, for the same reason.
//
// Duration is measured by polling the outbox until it drains, because the
// metrics package exposes no publish-duration histogram.
//
// Each run truncates and reseeds the outbox, so the four benchmark levels
// never share a batch of events.
//
//	go run ./cmd/outboxbench -workers 4 -batch 10 -events 1000
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/s7venking/eventflow/internal/config"
	"github.com/s7venking/eventflow/internal/event/application"
	"github.com/s7venking/eventflow/internal/event/domain"
	"github.com/s7venking/eventflow/internal/metrics"
	"github.com/s7venking/eventflow/internal/platform/postgres"
)

const composeDefaultDatabaseURL = "postgres://eventflow:eventflow@localhost:5452/eventflow?sslmode=disable"

func main() {
	workers := flag.Int(
		"workers",
		1,
		"number of outbox workers competing for the same events",
	)

	batchSize := flag.Int(
		"batch",
		10,
		"events claimed per ClaimPending call",
	)

	interval := flag.Duration(
		"interval",
		50*time.Millisecond,
		"poll interval of each worker",
	)

	totalEvents := flag.Int(
		"events",
		1000,
		"number of PENDING events to seed before the run",
	)

	publishLatency := flag.Duration(
		"publish-latency",
		0,
		"artificial per-event publish latency, to model a real broker",
	)

	maxConns := flag.Int(
		"max-conns",
		0,
		"pool size (default: workers + 4, so the pool is never the limit)",
	)

	timeout := flag.Duration(
		"timeout",
		10*time.Minute,
		"give up if the outbox has not drained within this window",
	)

	databaseURL := flag.String(
		"database-url",
		"",
		"Postgres URL (default: DATABASE_URL, then the compose default)",
	)

	out := flag.String(
		"out",
		"",
		"append the result row to this markdown file",
	)

	flag.Parse()

	if *workers <= 0 || *batchSize <= 0 || *totalEvents <= 0 {
		log.Fatal("workers, batch and events must all be greater than 0")
	}

	if *interval <= 0 {
		log.Fatal("interval must be greater than 0")
	}

	poolSize := *maxConns

	if poolSize <= 0 {
		// One connection per worker plus headroom for the progress
		// poller, so connection acquisition never masks the real
		// bottleneck.
		poolSize = *workers + 4
	}

	if err := run(runConfig{
		workers:        *workers,
		batchSize:      *batchSize,
		interval:       *interval,
		totalEvents:    *totalEvents,
		publishLatency: *publishLatency,
		poolSize:       poolSize,
		timeout:        *timeout,
		databaseURL:    resolveDatabaseURL(*databaseURL),
		out:            *out,
	}); err != nil {
		log.Fatal(err)
	}
}

type runConfig struct {
	workers        int
	batchSize      int
	interval       time.Duration
	totalEvents    int
	publishLatency time.Duration
	poolSize       int
	timeout        time.Duration
	databaseURL    string
	out            string
}

func resolveDatabaseURL(override string) string {
	if override != "" {
		return override
	}

	// Load does not override variables already set in the environment.
	_ = godotenv.Load()

	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}

	return composeDefaultDatabaseURL
}

// ========================================
// Publisher
// ========================================

// benchPublisher is deliberately cheap so the measurement reflects the
// claim/mark pipeline. It also records every call, which is how the run
// detects an event published twice.
type benchPublisher struct {
	latency time.Duration

	mu    sync.Mutex
	calls map[uuid.UUID]int
	total int
	first time.Time
}

func newBenchPublisher(latency time.Duration) *benchPublisher {
	return &benchPublisher{
		latency: latency,
		calls:   make(map[uuid.UUID]int),
	}
}

func (p *benchPublisher) Publish(
	ctx context.Context,
	event domain.OutboxEvent,
	_ *slog.Logger,
) error {
	p.mu.Lock()

	p.calls[event.ID]++
	p.total++

	if p.first.IsZero() {
		p.first = time.Now()
	}

	p.mu.Unlock()

	if p.latency > 0 {
		select {
		case <-time.After(p.latency):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (p *benchPublisher) stats() (distinct, duplicates, total int, first time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, n := range p.calls {
		if n > 1 {
			duplicates++
		}
	}

	return len(p.calls), duplicates, p.total, p.first
}

// ========================================
// Run
// ========================================

func run(cfg runConfig) error {
	rootCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	setupCtx, cancelSetup := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancelSetup()

	db, err := postgres.NewDB(setupCtx, config.DatabaseConfig{
		URL:             cfg.databaseURL,
		MaxConns:        int32(cfg.poolSize),
		MinConns:        1,
		MaxConnLifetime: time.Hour,
		MaxConnIdleTime: time.Hour,
	})
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	defer db.Close()

	if err := db.Ping(setupCtx); err != nil {
		return fmt.Errorf("ping: %w", err)
	}

	if err := db.Migrate(setupCtx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	log.Printf(
		"seeding %d PENDING events (workers=%d batch=%d interval=%s pool=%d)",
		cfg.totalEvents,
		cfg.workers,
		cfg.batchSize,
		cfg.interval,
		cfg.poolSize,
	)

	if err := reseed(setupCtx, db, cfg.totalEvents); err != nil {
		return err
	}

	publisher := newBenchPublisher(cfg.publishLatency)
	repository := postgres.NewOutboxRepository(db)

	// A fresh registry per run: the counters start at zero, and nothing is
	// scraped, so this only satisfies the worker's dependency.
	outboxMetrics := metrics.NewOutboxMetrics(
		prometheus.NewRegistry(),
	)

	workerLogger := slog.New(
		slog.NewTextHandler(io.Discard, nil),
	)

	workerCtx, stopWorkers := context.WithCancel(rootCtx)
	defer stopWorkers()

	var group sync.WaitGroup

	errs := make(chan error, cfg.workers)

	start := time.Now()

	for i := 1; i <= cfg.workers; i++ {
		worker := application.NewOutboxWorker(
			repository,
			publisher,
			cfg.interval,
			cfg.batchSize,
			3,
			time.Second,
			30*time.Second,
			30*time.Second,
			outboxMetrics,
			workerLogger.With("worker_id", i),
		)

		group.Add(1)

		go func() {
			defer group.Done()

			errs <- worker.Run(workerCtx)
		}()
	}

	counts, drainErr := waitDrained(
		rootCtx,
		db,
		cfg.timeout,
	)

	duration := time.Since(start)

	stopWorkers()
	group.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			log.Printf("worker stopped with error: %v", err)
		}
	}

	if drainErr != nil {
		log.Printf("WARNING: %v", drainErr)
	}

	result := buildResult(cfg, counts, publisher, duration, start)

	fmt.Print(result.report())

	if cfg.out != "" {
		if err := appendRow(cfg.out, result); err != nil {
			return err
		}

		log.Printf("result row appended to %s", cfg.out)
	}

	return nil
}

// reseed clears the outbox and inserts a fresh batch, so every concurrency
// level starts from the same state instead of sharing one pool of events.
func reseed(
	ctx context.Context,
	db *postgres.DB,
	count int,
) error {
	if _, err := db.Pool.Exec(
		ctx,
		"TRUNCATE TABLE outbox_events, events",
	); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}

	const query = `
		WITH seeded_events AS (
			INSERT INTO events (
				id, event_id, type, version, source, timestamp, properties
			)
			SELECT
				gen_random_uuid(),
				gen_random_uuid(),
				'benchmark.event',
				1,
				'outboxbench',
				NOW(),
				'{"benchmark": true}'
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
			'benchmark.event',
			'{"benchmark": true}',
			'PENDING',
			0,
			NOW() - INTERVAL '1 second',
			NOW()
		FROM seeded_events
	`

	if _, err := db.Pool.Exec(ctx, query, count); err != nil {
		return fmt.Errorf("seed events: %w", err)
	}

	return nil
}

type outboxCounts struct {
	pending    int
	processing int
	published  int
	closed     int
}

func countByStatus(
	ctx context.Context,
	db *postgres.DB,
) (outboxCounts, error) {
	const query = `
		SELECT
			COUNT(*) FILTER (WHERE status = 'PENDING'),
			COUNT(*) FILTER (WHERE status = 'PROCESSING'),
			COUNT(*) FILTER (WHERE status = 'PUBLISHED'),
			COUNT(*) FILTER (WHERE status = 'CLOSE')
		FROM outbox_events
	`

	var counts outboxCounts

	err := db.Pool.QueryRow(ctx, query).Scan(
		&counts.pending,
		&counts.processing,
		&counts.published,
		&counts.closed,
	)

	return counts, err
}

func waitDrained(
	ctx context.Context,
	db *postgres.DB,
	timeout time.Duration,
) (outboxCounts, error) {
	deadline := time.Now().Add(timeout)

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	progress := time.NewTicker(5 * time.Second)
	defer progress.Stop()

	var counts outboxCounts

	for {
		fresh, err := countByStatus(ctx, db)
		if err != nil {
			return counts, fmt.Errorf("count: %w", err)
		}

		counts = fresh

		if counts.pending == 0 && counts.processing == 0 {
			return counts, nil
		}

		if time.Now().After(deadline) {
			return counts, fmt.Errorf(
				"outbox not drained within %s",
				timeout,
			)
		}

		select {
		case <-ctx.Done():
			return counts, fmt.Errorf("interrupted")

		case <-progress.C:
			log.Printf(
				"progress: published=%d pending=%d processing=%d closed=%d",
				counts.published,
				counts.pending,
				counts.processing,
				counts.closed,
			)

		case <-ticker.C:
		}
	}
}

// ========================================
// Result
// ========================================

type result struct {
	cfg                runConfig
	duration           time.Duration
	timeToFirstPublish time.Duration
	published          int
	failed             int
	pending            int
	processing         int
	duplicate          int
	lost               int
	publishCalls       int
	distinct           int
}

func buildResult(
	cfg runConfig,
	counts outboxCounts,
	publisher *benchPublisher,
	duration time.Duration,
	start time.Time,
) result {
	distinct, duplicates, total, first := publisher.stats()

	accounted := counts.published +
		counts.closed +
		counts.pending +
		counts.processing

	var timeToFirst time.Duration

	if !first.IsZero() {
		timeToFirst = first.Sub(start)
	}

	return result{
		cfg:                cfg,
		duration:           duration,
		timeToFirstPublish: timeToFirst,
		published:          counts.published,
		failed:             counts.closed,
		pending:            counts.pending,
		processing:         counts.processing,
		duplicate:          duplicates,
		lost:               cfg.totalEvents - accounted,
		publishCalls:       total,
		distinct:           distinct,
	}
}

func (r result) throughput() float64 {
	if r.duration <= 0 {
		return 0
	}

	return float64(r.published) / r.duration.Seconds()
}

func (r result) report() string {
	return fmt.Sprintf(`
========================================
outbox worker benchmark
========================================
workers               %d
batch_size            %d
interval              %s
publish_latency       %s
pool_size             %d
total_events          %d
----------------------------------------
duration              %s
time_to_first_publish %s
throughput            %.1f events/sec
----------------------------------------
published             %d
failed (CLOSE)        %d
remaining_pending     %d
remaining_processing  %d
duplicate             %d
lost                  %d
----------------------------------------
publish_calls         %d (distinct %d)
========================================

%s
%s
`,
		r.cfg.workers,
		r.cfg.batchSize,
		r.cfg.interval,
		r.cfg.publishLatency,
		r.cfg.poolSize,
		r.cfg.totalEvents,
		r.duration.Round(time.Millisecond),
		r.timeToFirstPublish.Round(time.Millisecond),
		r.throughput(),
		r.published,
		r.failed,
		r.pending,
		r.processing,
		r.duplicate,
		r.lost,
		r.publishCalls,
		r.distinct,
		markdownHeader,
		r.markdownRow(),
	)
}

const markdownHeader = "| Workers | Batch | Events | Duration | Throughput | Published | Failed | Pending | Processing | Duplicate | Lost |\n" +
	"|---------|-------|--------|----------|------------|-----------|--------|---------|------------|-----------|------|"

func (r result) markdownRow() string {
	return fmt.Sprintf(
		"| %d | %d | %d | %s | %.1f/s | %d | %d | %d | %d | %d | %d |",
		r.cfg.workers,
		r.cfg.batchSize,
		r.cfg.totalEvents,
		r.duration.Round(time.Millisecond),
		r.throughput(),
		r.published,
		r.failed,
		r.pending,
		r.processing,
		r.duplicate,
		r.lost,
	)
}

func appendRow(path string, r result) error {
	file, err := os.OpenFile(
		path,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0o644,
	)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}

	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}

	if info.Size() == 0 {
		if _, err := fmt.Fprintln(file, markdownHeader); err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
	}

	if _, err := fmt.Fprintln(file, r.markdownRow()); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}

	return nil
}
