package application

import (
	"context"
	"log"
	"time"

	"github.com/s7venking/eventflow/internal/platform/postgres"
)

type OutboxWorker struct {
	repository *postgres.OutboxRepository
	interval   time.Duration
	batchSize  int
}

func NewOutboxWorker(
	repository *postgres.OutboxRepository,
	interval time.Duration,
	batchSize int,
) *OutboxWorker {
	return &OutboxWorker{
		repository: repository,
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

func (w *OutboxWorker) process(
	ctx context.Context,
) error {
	events, err := w.repository.GetPendingOutboxEvents(
		ctx,
		w.batchSize,
	)
	if err != nil {
		return err
	}

	for _, event := range events {
		log.Printf(
			"processing outbox event: id=%s event_id=%s type=%s",
			event.ID,
			event.EventID,
			event.EventType,
		)

		// Kafka sẽ được thêm ở bước sau.
	}

	return nil
}
