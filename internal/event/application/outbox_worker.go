package application

import (
	"context"
	"log"
	"time"

	"github.com/s7venking/eventflow/internal/event/domain"
	"github.com/s7venking/eventflow/internal/platform/postgres"
)

type OutboxWorker struct {
	repository *postgres.OutboxRepository
	publisher  EventPublisher
	interval   time.Duration
	batchSize  int
}

type EventPublisher interface {
	Publish(
		ctx context.Context,
		event domain.OutboxEvent,
	) error
}

func NewOutboxWorker(
	repository *postgres.OutboxRepository,
	publisher EventPublisher,
	interval time.Duration,
	batchSize int,
) *OutboxWorker {
	return &OutboxWorker{
		repository: repository,
		publisher:  publisher,
		interval:   interval,
		batchSize:  batchSize,
	}
}

func (w *OutboxWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	log.Println("outbox worker started")

	for {
		select {
		case <-ctx.Done():
			log.Println("outbox worker stopped")
			return

		case <-ticker.C:
			if err := w.process(ctx); err != nil {
				log.Printf(
					"outbox worker error: %v",
					err,
				)
			}
		}
	}
}

const retryDelay = 10 * time.Second

func (w *OutboxWorker) process(ctx context.Context) error {
	events, err := w.repository.ClaimPending(
		ctx,
		w.batchSize,
	)
	if err != nil {
		return err
	}

	for _, event := range events {
		if err := w.publisher.Publish(ctx, event); err != nil {
			nextAttemptAt := time.Now().Add(retryDelay)

			if markErr := w.repository.MarkFailed(
				ctx,
				event.ID,
				err.Error(),
				nextAttemptAt,
			); markErr != nil {
				log.Printf(
					"mark outbox event failed: event_id=%s error=%v",
					event.EventID,
					markErr,
				)
			}

			log.Printf(
				"publish failed: event_id=%s retry_at=%s error=%v",
				event.EventID,
				nextAttemptAt,
				err,
			)

			continue
		}

		if err := w.repository.MarkPublished(
			ctx,
			event.ID,
		); err != nil {
			log.Printf(
				"mark published failed: event_id=%s error=%v",
				event.EventID,
				err,
			)

			continue
		}

		log.Printf(
			"published: event_id=%s type=%s",
			event.EventID,
			event.EventType,
		)
	}

	return nil
}

type LogPublisher struct{}

func NewLogPublisher() *LogPublisher {
	return &LogPublisher{}
}

func (p *LogPublisher) Publish(
	ctx context.Context,
	event domain.OutboxEvent,
) error {
	log.Printf(
		"publish event: id=%s type=%s payload=%s",
		event.EventID,
		event.EventType,
		string(event.Payload),
	)

	return nil
}
