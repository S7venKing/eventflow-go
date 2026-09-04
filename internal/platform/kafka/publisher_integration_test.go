package kafka

// Real-broker coverage. Needs a reachable Kafka (docker compose up -d
// kafka); resolved from KAFKA_TEST_BROKERS, then KAFKA_BROKERS, then the
// compose default localhost:9092. Skips when no broker answers.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/s7venking/eventflow/internal/config"
	"github.com/s7venking/eventflow/internal/event/domain"
	"github.com/s7venking/eventflow/internal/metrics"
)

func testBrokers() []string {
	value := os.Getenv("KAFKA_TEST_BROKERS")

	if value == "" {
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

	brokers := testBrokers()

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

// newTestTopic creates a single-partition throwaway topic and removes it
// on cleanup. One partition keeps reading deterministic.
func newTestTopic(t *testing.T, brokers []string) string {
	t.Helper()

	topic := fmt.Sprintf("eventflow.test.%d", time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	if err := EnsureTopic(ctx, brokers, topic, 1, 10*time.Second); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()

		if err := DeleteTopic(
			cleanupCtx,
			brokers,
			topic,
			10*time.Second,
		); err != nil {
			t.Logf("delete topic %s: %v", topic, err)
		}
	})

	if err := WaitTopicReady(ctx, brokers, topic, 20*time.Second); err != nil {
		t.Fatalf("topic not ready: %v", err)
	}

	return topic
}

func TestPublisherDeliversEventToKafka(t *testing.T) {
	brokers := requireKafka(t)
	topic := newTestTopic(t, brokers)

	m := metrics.NewKafkaMetrics(prometheus.NewRegistry())

	publisher := NewPublisher(
		config.KafkaConfig{
			Brokers:      brokers,
			Topic:        topic,
			ClientID:     "eventflow-test",
			WriteTimeout: 10 * time.Second,
			MaxAttempts:  5,
			BatchTimeout: 10 * time.Millisecond,
		},
		m,
	)
	defer publisher.Close()

	eventID := uuid.New()
	timestamp := time.Now().UTC().Truncate(time.Millisecond)

	// Same shape the ingestion transaction writes to outbox_events.payload
	// (see postgres.EventRepository.Save).
	payload, err := json.Marshal(map[string]any{
		"event_id":     eventID,
		"type":         "purchase",
		"version":      1,
		"source":       "web",
		"user_id":      "user_123",
		"anonymous_id": "anon_456",
		"session_id":   "session_789",
		"timestamp":    timestamp,
		"properties": map[string]any{
			"order_id": "ORD-123",
			"amount":   99.99,
			"currency": "USD",
		},
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	if err := publisher.Publish(
		ctx,
		domain.OutboxEvent{
			ID:        uuid.New(),
			EventID:   eventID,
			EventType: "purchase",
			Payload:   payload,
		},
		discardLogger(),
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		Partition:   0,
		MinBytes:    1,
		MaxBytes:    1 << 20,
		StartOffset: kafkago.FirstOffset,
	})
	defer reader.Close()

	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("read message: %v", err)
	}

	if got := string(msg.Key); got != eventID.String() {
		t.Errorf("key = %q, want %q", got, eventID)
	}

	if got := headerValue(msg, "event_type"); got != "purchase" {
		t.Errorf("event_type header = %q, want purchase", got)
	}

	var got struct {
		EventID     string         `json:"event_id"`
		Type        string         `json:"type"`
		Version     int            `json:"version"`
		Source      string         `json:"source"`
		UserID      string         `json:"user_id"`
		AnonymousID string         `json:"anonymous_id"`
		SessionID   string         `json:"session_id"`
		Timestamp   time.Time      `json:"timestamp"`
		Properties  map[string]any `json:"properties"`
	}

	if err := json.Unmarshal(msg.Value, &got); err != nil {
		t.Fatalf("unmarshal message value %s: %v", msg.Value, err)
	}

	if got.EventID != eventID.String() {
		t.Errorf("event_id = %q, want %q", got.EventID, eventID)
	}

	if got.Type != "purchase" {
		t.Errorf("type = %q, want purchase", got.Type)
	}

	if got.Version != 1 {
		t.Errorf("version = %d, want 1", got.Version)
	}

	if got.Source != "web" {
		t.Errorf("source = %q, want web", got.Source)
	}

	if got.UserID != "user_123" {
		t.Errorf("user_id = %q, want user_123", got.UserID)
	}

	if got.AnonymousID != "anon_456" {
		t.Errorf("anonymous_id = %q, want anon_456", got.AnonymousID)
	}

	if got.SessionID != "session_789" {
		t.Errorf("session_id = %q, want session_789", got.SessionID)
	}

	if !got.Timestamp.Equal(timestamp) {
		t.Errorf("timestamp = %s, want %s", got.Timestamp, timestamp)
	}

	if got.Properties["order_id"] != "ORD-123" {
		t.Errorf("properties.order_id = %v, want ORD-123", got.Properties["order_id"])
	}

	if got.Properties["amount"] != 99.99 {
		t.Errorf("properties.amount = %v, want 99.99", got.Properties["amount"])
	}

	if got.Properties["currency"] != "USD" {
		t.Errorf("properties.currency = %v, want USD", got.Properties["currency"])
	}

	if published := testutil.ToFloat64(
		m.Published.WithLabelValues(topic),
	); published != 1 {
		t.Errorf("kafka_publish_total = %v, want 1", published)
	}
}
