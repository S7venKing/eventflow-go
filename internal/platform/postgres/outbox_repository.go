package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/s7venking/eventflow/internal/event/domain"
)

type OutboxRepository struct {
	db *DB
}

func NewOutboxRepository(db *DB) *OutboxRepository {
	return &OutboxRepository{
		db: db,
	}
}

func (r *OutboxRepository) GetPendingOutboxEvents(
	ctx context.Context,
	limit int,
) ([]domain.OutboxEvent, error) {
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

	var events []domain.OutboxEvent

	for rows.Next() {
		var event domain.OutboxEvent

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

func (r *OutboxRepository) MarkPublished(
	ctx context.Context,
	id uuid.UUID,
) error {
	const query = `
		UPDATE outbox_events
		SET
			status = 'PUBLISHED',
			published_at = NOW()
		WHERE id = $1
	`

	tag, err := r.db.Pool.Exec(
		ctx,
		query,
		id,
	)
	if err != nil {
		return fmt.Errorf(
			"mark outbox event as published: %w",
			err,
		)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf(
			"outbox event not found: %s",
			id,
		)
	}

	return nil
}
