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
		SET
			status = 'PROCESSING',
			processing_at = NOW()
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

// ReclaimStale returns to PENDING every event that has sat in PROCESSING
// longer than staleAfter, measured against the processing_at timestamp
// that ClaimPending stamps. A crashed worker leaves its claimed batch in
// PROCESSING with no cleanup; this is the only path that frees those rows.
//
// Rows in PROCESSING with a NULL processing_at were claimed by a build
// that predates the timestamp and can only be leftovers, so they are
// reclaimed unconditionally.
//
// The comparison happens entirely on the database clock and the rows are
// taken FOR UPDATE SKIP LOCKED inside a single statement, so any number
// of workers may call this concurrently: each stale row is reclaimed by
// exactly one of them.
func (r *OutboxRepository) ReclaimStale(
	ctx context.Context,
	staleAfter time.Duration,
) (int, error) {
	const query = `
		WITH stale AS (
			SELECT id
			FROM outbox_events
			WHERE status = 'PROCESSING'
			  AND (
				processing_at IS NULL
				OR processing_at <= NOW() - make_interval(secs => $1)
			  )
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox_events AS o
		SET
			status = 'PENDING',
			available_at = NOW(),
			processing_at = NULL
		FROM stale
		WHERE o.id = stale.id
	`

	tag, err := r.db.Pool.Exec(
		ctx,
		query,
		staleAfter.Seconds(),
	)
	if err != nil {
		return 0, fmt.Errorf(
			"reclaim stale outbox events: %w",
			err,
		)
	}

	return int(tag.RowsAffected()), nil
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
			status = 'PENDING',
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

func (r *OutboxRepository) MarkClose(
	ctx context.Context,
	id uuid.UUID,
	errMsg string,
) error {
	const query = `
		UPDATE outbox_events
		SET
			status = 'CLOSE',
			last_error = $2
		WHERE id = $1
		  AND status = 'PROCESSING'
	`

	tag, err := r.db.Pool.Exec(
		ctx,
		query,
		id,
		errMsg,
	)
	if err != nil {
		return fmt.Errorf(
			"mark outbox event as closed: %w",
			err,
		)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf(
			"outbox event not found or not processing: %s",
			id,
		)
	}

	return nil
}
