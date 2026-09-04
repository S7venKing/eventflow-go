// Package kafka is the Kafka side of the outbox pipeline. It implements
// the application's EventPublisher so the outbox worker never touches a
// Kafka client: the worker calls Publish, and only a nil return means the
// broker acknowledged the write.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/s7venking/eventflow/internal/config"
	"github.com/s7venking/eventflow/internal/event/domain"
	"github.com/s7venking/eventflow/internal/metrics"
)

// ErrEmptyPayload is returned for an outbox row with no payload. Retrying
// cannot fix it, so the worker's retry budget eventually moves the row to
// CLOSE instead of it circulating forever.
var ErrEmptyPayload = errors.New("kafka publisher: outbox payload is empty")

// ErrInvalidPayload is returned when the payload is not valid JSON.
var ErrInvalidPayload = errors.New("kafka publisher: outbox payload is not valid JSON")

// MessageWriter is the slice of kafka-go's Writer the publisher relies on,
// so tests can stand in a fake and keep the rest of the code path real.
type MessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

// Publisher writes outbox events to one Kafka topic.
type Publisher struct {
	writer  MessageWriter
	topic   string
	metrics *metrics.KafkaMetrics
}

// NewPublisher builds a synchronous producer. Every Publish blocks until
// the broker acknowledges (RequireAll) or the attempts run out, which is
// what lets the outbox worker mark PUBLISHED only after a durable write.
func NewPublisher(
	cfg config.KafkaConfig,
	m *metrics.KafkaMetrics,
) *Publisher {
	writer := &kafkago.Writer{
		Addr:  kafkago.TCP(cfg.Brokers...),
		Topic: cfg.Topic,

		// Hash on the key so one event_id always lands on the same
		// partition. Switching the key to user_id later would give
		// per-user ordering with no other change.
		Balancer: &kafkago.Hash{},

		// Wait for the full ISR: a PUBLISHED outbox row must mean the
		// message survives a broker restart.
		RequiredAcks: kafkago.RequireAll,

		// Synchronous by design. Async would return before the ack and
		// break the outbox contract.
		Async: false,

		MaxAttempts:  cfg.MaxAttempts,
		WriteTimeout: cfg.WriteTimeout,
		ReadTimeout:  cfg.WriteTimeout,

		// kafka-go coalesces concurrent WriteMessages calls into one
		// batch per partition. The default BatchTimeout of 1s would
		// stall every single-message write; keep it short so the
		// workers' sequential publishes are not throttled.
		BatchTimeout: cfg.BatchTimeout,

		// Topics come from docker compose (kafka-init) or the admin
		// helper, never as a side effect of a produce.
		AllowAutoTopicCreation: false,

		Transport: &kafkago.Transport{
			ClientID:    cfg.ClientID,
			DialTimeout: cfg.WriteTimeout,
		},
	}

	return NewPublisherWithWriter(writer, cfg.Topic, m)
}

// NewPublisherWithWriter wires an arbitrary writer; production code goes
// through NewPublisher, tests inject a fake here.
func NewPublisherWithWriter(
	writer MessageWriter,
	topic string,
	m *metrics.KafkaMetrics,
) *Publisher {
	return &Publisher{
		writer:  writer,
		topic:   topic,
		metrics: m,
	}
}

// Topic reports the destination topic.
func (p *Publisher) Topic() string {
	return p.topic
}

// Publish sends one outbox event. The message value is the JSON payload
// the ingestion transaction wrote to the outbox, so what reaches Kafka is
// exactly what was durably committed: no second serialization step and
// no drift between the two.
func (p *Publisher) Publish(
	ctx context.Context,
	event domain.OutboxEvent,
	logger *slog.Logger,
) error {
	if len(event.Payload) == 0 {
		return ErrEmptyPayload
	}

	if !json.Valid(event.Payload) {
		return ErrInvalidPayload
	}

	message := kafkago.Message{
		// event_id keys the message: it identifies the event and keeps
		// redeliveries of the same event on one partition.
		Key:   []byte(event.EventID.String()),
		Value: event.Payload,
		Headers: []kafkago.Header{
			{Key: "event_id", Value: []byte(event.EventID.String())},
			{Key: "event_type", Value: []byte(event.EventType)},
			{Key: "content_type", Value: []byte("application/json")},
		},
	}

	start := time.Now()

	err := p.writer.WriteMessages(ctx, message)

	duration := time.Since(start)

	if err != nil {
		// A canceled context is the worker shutting down, not Kafka
		// failing; the row stays PROCESSING and reclaim frees it.
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			logger.Warn(
				"kafka_publish_canceled",
				"event_id", event.EventID,
				"topic", p.topic,
				"duration", duration,
				"error", err,
			)

			return err
		}

		p.metrics.Failed.WithLabelValues(p.topic).Inc()
		p.metrics.Duration.WithLabelValues(
			p.topic,
			metrics.KafkaStatusFailure,
		).Observe(duration.Seconds())

		logger.Error(
			"kafka_publish_failed",
			"event_id", event.EventID,
			"event_type", event.EventType,
			"topic", p.topic,
			"duration", duration,
			"error", err,
		)

		return fmt.Errorf(
			"kafka publish to %s: %w",
			p.topic,
			err,
		)
	}

	p.metrics.Published.WithLabelValues(p.topic).Inc()
	p.metrics.Duration.WithLabelValues(
		p.topic,
		metrics.KafkaStatusSuccess,
	).Observe(duration.Seconds())

	logger.Info(
		"event_published_to_kafka",
		"event_id", event.EventID,
		"event_type", event.EventType,
		"topic", p.topic,
		"duration", duration,
	)

	return nil
}

// Close flushes and stops the producer. Call it after the workers have
// stopped so no publish is in flight.
func (p *Publisher) Close() error {
	return p.writer.Close()
}
