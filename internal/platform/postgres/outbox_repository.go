package postgres

import (
	"context"
	"fmt"
	"time"

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

func (r *OutboxRepository) ClaimPending(
	ctx context.Context,
	limit int,
) ([]domain.OutboxEvent, error) {
	tx, err := r.db.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"begin claim transaction: %w",
			err,
		)
	}

	defer tx.Rollback(ctx)

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
		FOR UPDATE SKIP LOCKED
	`

	rows, err := tx.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf(
			"query pending outbox events: %w",
			err,
		)
	}

	defer rows.Close()

	events := make([]domain.OutboxEvent, 0, limit)

	for rows.Next() {
		var event domain.OutboxEvent

		if err := rows.Scan(
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
		); err != nil {
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

	if len(events) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf(
				"commit empty claim transaction: %w",
				err,
			)
		}

		return events, nil
	}

	ids := make([]uuid.UUID, 0, len(events))

	for _, event := range events {
		ids = append(ids, event.ID)
	}

	_, err = tx.Exec(
		ctx,
		`
		UPDATE outbox_events
		SET status = 'PROCESSING'
		WHERE id = ANY($1)
		`,
		ids,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"mark outbox events processing: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf(
			"commit claim transaction: %w",
			err,
		)
	}

	for i := range events {
		events[i].Status = "PROCESSING"
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

func (r *OutboxRepository) MarkFailed(
	ctx context.Context,
	id uuid.UUID,
	errMsg string,
	nextAttemptAt time.Time,
) error {
	const query = `
		UPDATE outbox_events
		SET
			status = 'PENDING'
			attempts = attempts + 1,
			available_at = $2,
			last_error = $3
		WHERE id = $1
		  AND status = 'PROCESSING'
	`

	tag, err := r.db.Pool.Exec(
		ctx,
		query,
		id,
		nextAttemptAt,
		errMsg,
	)
	if err != nil {
		return fmt.Errorf(
			"mark outbox event as failed: %w",
			err,
		)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf(
			"outbox event not found or not pending: %s",
			id,
		)
	}

	return nil
}
