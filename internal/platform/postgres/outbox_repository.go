package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type OutboxRepository struct {
	db *DB
}

type OutboxEvent struct {
	ID          uuid.UUID
	EventID     uuid.UUID
	EventType   string
	Payload     []byte
	Status      string
	Attempts    int
	AvailableAt time.Time
	CreatedAt   time.Time
	PublishedAt *time.Time
	LastError   *string
}

func NewOutboxRepository(db *DB) *OutboxRepository {
	return &OutboxRepository{
		db: db,
	}
}

func (r *OutboxRepository) GetPendingOutboxEvents(
	ctx context.Context,
	limit int,
) ([]OutboxEvent, error) {
	const query = `
		SELECT
			id,
			event_id,
			event_type,
			payload,
			status,
			attempts,
			available_at,
			created_at,
			published_at,
			last_error
		FROM outbox_events
		WHERE status = 'PENDING'
		  AND available_at <= NOW()
		ORDER BY created_at
		LIMIT $1
	`

	rows, err := r.db.Pool.Query(
		ctx,
		query,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get pending outbox events: %w",
			err,
		)
	}

	defer rows.Close()

	var events []OutboxEvent

	for rows.Next() {
		var event OutboxEvent

		err := rows.Scan(
			&event.ID,
			&event.EventID,
			&event.EventType,
			&event.Payload,
			&event.Status,
			&event.Attempts,
			&event.AvailableAt,
			&event.CreatedAt,
			&event.PublishedAt,
			&event.LastError,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"scan outbox event: %w",
				err,
			)
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate outbox events: %w",
			err,
		)
	}

	return events, nil
}
