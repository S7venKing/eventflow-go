package application

import (
	"context"
	"log"
	"time"

	"github.com/s7venking/eventflow/internal/event/domain"
	"github.com/s7venking/eventflow/internal/platform/postgres"
)

type OutboxWorker struct {
	repository     *postgres.OutboxRepository
	publisher      EventPublisher
	interval       time.Duration
	batchSize      int
	maxRetries     int
	retryBaseDelay time.Duration
	retryMaxDelay  time.Duration
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
	maxRetries int,
	retryBaseDelay time.Duration,
	retryMaxDelay time.Duration,
) *OutboxWorker {
	return &OutboxWorker{
		repository:     repository,
		publisher:      publisher,
		interval:       interval,
		batchSize:      batchSize,
		maxRetries:     maxRetries,
		retryBaseDelay: retryBaseDelay,
		retryMaxDelay:  retryMaxDelay,
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
			if event.Attempts >= w.maxRetries {
				if closeErr := w.repository.MarkClose(
					ctx,
					event.ID,
					err.Error(),
				); closeErr != nil {
					log.Printf(
						"mark outbox event closed: event_id=%s error=%v",
						event.EventID,
						closeErr,
					)
				}

				log.Printf(
					"publish failed permanently: event_id=%s attempts=%d error=%v",
					event.EventID,
					event.Attempts,
					err,
				)

				continue
			}

			retryNumber := event.Attempts + 1

			delay := RetryDelay(
				retryNumber,
				w.retryBaseDelay,
				w.retryMaxDelay,
			)

			retryAt := time.Now().Add(delay)

			if markErr := w.repository.MarkFailed(
				ctx,
				event.ID,
				err.Error(),
				retryAt,
			); markErr != nil {
				log.Printf(
					"mark outbox event failed: event_id=%s error=%v",
					event.EventID,
					markErr,
				)
			}

			log.Printf(
				"publish failed: event_id=%s retry=%d retry_at=%s error=%v",
				event.EventID,
				retryNumber,
				retryAt,
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
