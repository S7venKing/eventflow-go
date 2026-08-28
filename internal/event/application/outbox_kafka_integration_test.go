package application

// Outbox -> Kafka end to end, through the production worker and the
// production Kafka publisher. Two flavours:
//
//   - a real broker (skipped when none is reachable): 100 PENDING rows
//     must become 100 PUBLISHED rows and exactly 100 Kafka messages;
//   - a simulated outage: the Kafka writer is faked so the broker can be
//     "stopped" and "started" from the test, proving the worker never
//     marks PUBLISHED while Kafka is down and drains once it is back.
//
// Postgres comes from the shared harness in
// outbox_worker_concurrency_test.go.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/s7venking/eventflow/internal/config"
	"github.com/s7venking/eventflow/internal/metrics"
	kafkaplatform "github.com/s7venking/eventflow/internal/platform/kafka"
	"github.com/s7venking/eventflow/internal/platform/postgres"
)

// ========================================
// Kafka test harness
// ========================================

func kafkaTestBrokers() []string {
	value := os.Getenv("KAFKA_TEST_BROKERS")

	if value == "" {
		if root, err := concurrencyRepoRoot(); err == nil {
			_ = godotenv.Load(filepath.Join(root, ".env"))
		}

		value = os.Getenv("KAFKA_BROKERS")
	}

	if value == "" {
		value = "localhost:9092"
	}

	parts := strings.Split(value, ",")

	brokers := make([]string, 0, len(parts))

	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			brokers = append(brokers, part)
		}
	}

	return brokers
}

func requireKafka(t *testing.T) []string {
	t.Helper()

	brokers := kafkaTestBrokers()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Skipf(
			"skipping kafka test, no reachable broker at %v: %v",
			brokers,
			err,
		)
	}

	conn.Close()

	return brokers
}

func newKafkaTestTopic(t *testing.T, brokers []string) string {
	t.Helper()

	topic := fmt.Sprintf("eventflow.outbox-test.%d", time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	if err := kafkaplatform.EnsureTopic(
		ctx,
		brokers,
		topic,
		1,
		10*time.Second,
	); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()

		if err := kafkaplatform.DeleteTopic(
			cleanupCtx,
			brokers,
			topic,
			10*time.Second,
		); err != nil {
			t.Logf("delete topic %s: %v", topic, err)
		}
	})

	if err := kafkaplatform.WaitTopicReady(
		ctx,
		brokers,
		topic,
		20*time.Second,
	); err != nil {
		t.Fatalf("topic not ready: %v", err)
	}

	return topic
}

// readAllKeys drains the single-partition topic until want messages
// arrived or the deadline passed, and returns the keys seen.
func readAllKeys(
	t *testing.T,
	brokers []string,
	topic string,
	want int,
	timeout time.Duration,
) []string {
	t.Helper()

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		Partition:   0,
		MinBytes:    1,
		MaxBytes:    1 << 20,
		StartOffset: kafkago.FirstOffset,
	})
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	keys := make([]string, 0, want)

	for len(keys) < want {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf(
				"read message %d/%d: %v",
				len(keys)+1,
				want,
				err,
			)
		}

		keys = append(keys, string(msg.Key))
	}

	// Anything beyond want is a duplicate delivery.
	extraCtx, extraCancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer extraCancel()

	if msg, err := reader.ReadMessage(extraCtx); err == nil {
		t.Errorf(
			"topic holds more than %d messages; extra key %s",
			want,
			msg.Key,
		)
	}

	return keys
}

func waitUntil(
	t *testing.T,
	timeout time.Duration,
	what string,
	condition func() bool,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for {
		if condition() {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %s", what, timeout)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// ========================================
// C. 100 PENDING rows -> 100 Kafka messages
// ========================================

func TestOutboxWorkersPublishEveryEventToKafkaOnce(t *testing.T) {
	db := newConcurrencyTestDB(t)
	brokers := requireKafka(t)
	topic := newKafkaTestTopic(t, brokers)

	const (
		totalEvents = 100
		workers     = 4
		batchSize   = 10
	)

	seedPendingOutboxEvents(t, db, totalEvents)

	publisher := kafkaplatform.NewPublisher(
		config.KafkaConfig{
			Brokers:      brokers,
			Topic:        topic,
			ClientID:     "eventflow-outbox-test",
			WriteTimeout: 10 * time.Second,
			MaxAttempts:  5,
			BatchTimeout: 10 * time.Millisecond,
		},
		metrics.NewKafkaMetrics(prometheus.NewRegistry()),
	)
	defer publisher.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	group, errs := startOutboxWorkersCfg(
		ctx,
		postgres.NewOutboxRepository(db),
		publisher,
		workerFixture{
			workers:         workers,
			batchSize:       batchSize,
			interval:        20 * time.Millisecond,
			shutdownTimeout: 10 * time.Second,
		},
	)

	counts := waitOutboxDrained(t, db, 120*time.Second)

	for _, err := range stopWorkersAndCollect(t, cancel, group, errs) {
		if err != nil {
			t.Errorf("worker returned %v, want nil", err)
		}
	}

	if counts.published != totalEvents {
		t.Errorf("PUBLISHED = %d, want %d", counts.published, totalEvents)
	}

	if counts.pending != 0 || counts.processing != 0 || counts.closed != 0 {
		t.Errorf(
			"leftover pending=%d processing=%d closed=%d, want 0/0/0",
			counts.pending,
			counts.processing,
			counts.closed,
		)
	}

	if lost := totalEvents - counts.total(); lost != 0 {
		t.Errorf("lost = %d, want 0", lost)
	}

	keys := readAllKeys(t, brokers, topic, totalEvents, 60*time.Second)

	seen := make(map[string]int, len(keys))

	for _, key := range keys {
		seen[key]++
	}

	if len(seen) != totalEvents {
		t.Errorf(
			"distinct message keys = %d, want %d",
			len(seen),
			totalEvents,
		)
	}

	for key, n := range seen {
		if n > 1 {
			t.Errorf("event %s delivered %d times, want 1", key, n)
		}
	}

	// Every key is one of the seeded event ids.
	rows, err := db.Pool.Query(
		context.Background(),
		"SELECT event_id::text FROM outbox_events",
	)
	if err != nil {
		t.Fatalf("list event ids: %v", err)
	}

	defer rows.Close()

	for rows.Next() {
		var eventID string

		if err := rows.Scan(&eventID); err != nil {
			t.Fatalf("scan event id: %v", err)
		}

		if seen[eventID] != 1 {
			t.Errorf(
				"event %s reached Kafka %d times, want 1",
				eventID,
				seen[eventID],
			)
		}
	}
}

// ========================================
// Critical failure: Kafka down, then back
// ========================================

// outageWriter is the Kafka writer with a switch: while down, every write
// fails the way a dead broker does; once up, writes are recorded.
type outageWriter struct {
	down atomic.Bool

	mu     sync.Mutex
	keys   map[string]int
	writes int
}

func newOutageWriter() *outageWriter {
	return &outageWriter{keys: make(map[string]int)}
}

func (w *outageWriter) WriteMessages(
	ctx context.Context,
	msgs ...kafkago.Message,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if w.down.Load() {
		return errors.New(
			"dial tcp 127.0.0.1:9092: connect: connection refused (simulated kafka outage)",
		)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	for _, msg := range msgs {
		w.keys[string(msg.Key)]++
		w.writes++
	}

	return nil
}

func (w *outageWriter) Close() error {
	return nil
}

func (w *outageWriter) stats() (writes int, distinct int, duplicates int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, n := range w.keys {
		if n > 1 {
			duplicates++
		}
	}

	return w.writes, len(w.keys), duplicates
}

func TestOutboxWorkerKafkaOutageKeepsEventsPendingUntilRecovery(t *testing.T) {
	db := newConcurrencyTestDB(t)

	const (
		totalEvents = 20
		workers     = 2
		batchSize   = 10
	)

	seedPendingOutboxEvents(t, db, totalEvents)

	writer := newOutageWriter()
	writer.down.Store(true)

	publisher := kafkaplatform.NewPublisherWithWriter(
		writer,
		"eventflow.events",
		metrics.NewKafkaMetrics(prometheus.NewRegistry()),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A retry budget far above what the outage needs, so no event can
	// fall into CLOSE while the broker is away.
	group, errs := startOutboxWorkersCfg(
		ctx,
		postgres.NewOutboxRepository(db),
		publisher,
		workerFixture{
			workers:         workers,
			batchSize:       batchSize,
			interval:        20 * time.Millisecond,
			maxRetries:      1000,
			retryBaseDelay:  50 * time.Millisecond,
			retryMaxDelay:   200 * time.Millisecond,
			shutdownTimeout: 10 * time.Second,
		},
	)

	// Phase 1: Kafka is down. Every event gets claimed, fails to publish,
	// and returns to PENDING with the failure recorded.
	waitUntil(t, 30*time.Second, "every event failing once", func() bool {
		return queryInt(
			t,
			db,
			"SELECT COUNT(*)::int FROM outbox_events WHERE attempts >= 1",
		) == totalEvents
	})

	counts := countOutboxByStatus(t, db)

	if counts.published != 0 {
		t.Fatalf(
			"PUBLISHED = %d while Kafka is down, want 0",
			counts.published,
		)
	}

	if counts.closed != 0 {
		t.Fatalf("CLOSE = %d while Kafka is down, want 0", counts.closed)
	}

	if counts.pending+counts.processing != totalEvents {
		t.Fatalf(
			"pending+processing = %d while Kafka is down, want %d",
			counts.pending+counts.processing,
			totalEvents,
		)
	}

	if missing := queryInt(
		t,
		db,
		`SELECT COUNT(*)::int FROM outbox_events
		 WHERE attempts >= 1
		   AND (last_error IS NULL OR last_error NOT LIKE '%kafka%')`,
	); missing != 0 {
		t.Errorf(
			"%d failed events lack a kafka last_error, want 0",
			missing,
		)
	}

	if writes, _, _ := writer.stats(); writes != 0 {
		t.Fatalf("writer recorded %d writes while down, want 0", writes)
	}

	// Phase 2: Kafka is back. The same workers drain the backlog through
	// their normal retry path.
	writer.down.Store(false)

	counts = waitOutboxDrained(t, db, 60*time.Second)

	for _, err := range stopWorkersAndCollect(t, cancel, group, errs) {
		if err != nil {
			t.Errorf("worker returned %v, want nil", err)
		}
	}

	if counts.published != totalEvents {
		t.Errorf("PUBLISHED = %d, want %d", counts.published, totalEvents)
	}

	if counts.pending != 0 || counts.processing != 0 || counts.closed != 0 {
		t.Errorf(
			"leftover pending=%d processing=%d closed=%d, want 0/0/0",
			counts.pending,
			counts.processing,
			counts.closed,
		)
	}

	writes, distinct, duplicates := writer.stats()

	if writes != totalEvents || distinct != totalEvents {
		t.Errorf(
			"kafka writes = %d (distinct %d), want %d/%d",
			writes,
			distinct,
			totalEvents,
			totalEvents,
		)
	}

	if duplicates != 0 {
		t.Errorf("%d events written to Kafka twice, want 0", duplicates)
	}
}
