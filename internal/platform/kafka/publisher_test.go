package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/s7venking/eventflow/internal/event/domain"
	"github.com/s7venking/eventflow/internal/metrics"
)

const testTopic = "eventflow.test"

// fakeWriter stands in for kafka-go's Writer. It honours context
// cancellation the way the real writer does and records what it was
// asked to write.
type fakeWriter struct {
	err      error
	messages []kafkago.Message
	closed   bool
}

func (w *fakeWriter) WriteMessages(
	ctx context.Context,
	msgs ...kafkago.Message,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if w.err != nil {
		return w.err
	}

	w.messages = append(w.messages, msgs...)

	return nil
}

func (w *fakeWriter) Close() error {
	w.closed = true

	return nil
}

func newTestPublisher(
	writer MessageWriter,
) (*Publisher, *metrics.KafkaMetrics) {
	m := metrics.NewKafkaMetrics(prometheus.NewRegistry())

	return NewPublisherWithWriter(writer, testTopic, m), m
}

func testEvent(payload string) domain.OutboxEvent {
	return domain.OutboxEvent{
		ID:        uuid.New(),
		EventID:   uuid.New(),
		EventType: "purchase",
		Payload:   []byte(payload),
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func headerValue(msg kafkago.Message, key string) string {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}

	return ""
}

func TestPublishWritesEventKeyedByEventID(t *testing.T) {
	writer := &fakeWriter{}
	publisher, m := newTestPublisher(writer)

	event := testEvent(`{"event_id":"x","type":"purchase"}`)

	if err := publisher.Publish(
		context.Background(),
		event,
		discardLogger(),
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if len(writer.messages) != 1 {
		t.Fatalf("wrote %d messages, want 1", len(writer.messages))
	}

	msg := writer.messages[0]

	if got := string(msg.Key); got != event.EventID.String() {
		t.Errorf("key = %q, want event_id %q", got, event.EventID)
	}

	if string(msg.Value) != string(event.Payload) {
		t.Errorf(
			"value = %s, want the outbox payload verbatim %s",
			msg.Value,
			event.Payload,
		)
	}

	if msg.Topic != "" {
		t.Errorf(
			"message topic = %q, want empty (the writer owns the topic)",
			msg.Topic,
		)
	}

	if got := headerValue(msg, "event_id"); got != event.EventID.String() {
		t.Errorf("event_id header = %q, want %q", got, event.EventID)
	}

	if got := headerValue(msg, "event_type"); got != "purchase" {
		t.Errorf("event_type header = %q, want purchase", got)
	}

	if got := headerValue(msg, "content_type"); got != "application/json" {
		t.Errorf("content_type header = %q, want application/json", got)
	}

	if got := testutil.ToFloat64(
		m.Published.WithLabelValues(testTopic),
	); got != 1 {
		t.Errorf("kafka_publish_total = %v, want 1", got)
	}

	if got := testutil.ToFloat64(
		m.Failed.WithLabelValues(testTopic),
	); got != 0 {
		t.Errorf("kafka_publish_failed_total = %v, want 0", got)
	}
}

func TestPublishReturnsWriterErrorAndCountsFailure(t *testing.T) {
	brokerDown := errors.New("dial tcp: connection refused")

	writer := &fakeWriter{err: brokerDown}
	publisher, m := newTestPublisher(writer)

	err := publisher.Publish(
		context.Background(),
		testEvent(`{}`),
		discardLogger(),
	)

	if !errors.Is(err, brokerDown) {
		t.Fatalf("Publish error = %v, want to wrap %v", err, brokerDown)
	}

	if got := testutil.ToFloat64(
		m.Failed.WithLabelValues(testTopic),
	); got != 1 {
		t.Errorf("kafka_publish_failed_total = %v, want 1", got)
	}

	if got := testutil.ToFloat64(
		m.Published.WithLabelValues(testTopic),
	); got != 0 {
		t.Errorf("kafka_publish_total = %v, want 0", got)
	}
}

func TestPublishRespectsContextCancellation(t *testing.T) {
	writer := &fakeWriter{}
	publisher, m := newTestPublisher(writer)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := publisher.Publish(ctx, testEvent(`{}`), discardLogger())

	// The worker relies on errors.Is to tell shutdown from a broker
	// failure, so the context error must surface unwrapped-compatible.
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish error = %v, want context.Canceled", err)
	}

	if len(writer.messages) != 0 {
		t.Errorf("wrote %d messages after cancel, want 0", len(writer.messages))
	}

	// Shutdown is not a Kafka failure.
	if got := testutil.ToFloat64(
		m.Failed.WithLabelValues(testTopic),
	); got != 0 {
		t.Errorf("kafka_publish_failed_total = %v, want 0", got)
	}
}

func TestPublishRejectsUnusablePayloadWithoutWriting(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    error
	}{
		{"empty", "", ErrEmptyPayload},
		{"invalid json", `{"event_id":`, ErrInvalidPayload},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writer := &fakeWriter{}
			publisher, _ := newTestPublisher(writer)

			err := publisher.Publish(
				context.Background(),
				testEvent(tc.payload),
				discardLogger(),
			)

			if !errors.Is(err, tc.want) {
				t.Fatalf("Publish error = %v, want %v", err, tc.want)
			}

			if len(writer.messages) != 0 {
				t.Errorf("wrote %d messages, want 0", len(writer.messages))
			}
		})
	}
}

func TestCloseClosesWriter(t *testing.T) {
	writer := &fakeWriter{}
	publisher, _ := newTestPublisher(writer)

	if err := publisher.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if !writer.closed {
		t.Error("writer was not closed")
	}
}
