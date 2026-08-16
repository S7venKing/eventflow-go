package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/s7venking/eventflow/internal/event/domain"
)

type EventRepository struct {
	db *DB
}

func NewEventRepository(db *DB) *EventRepository {
	return &EventRepository{
		db: db,
	}
}

func (r *EventRepository) Save(
	ctx context.Context,
	event domain.Event,
) error {
	tx, err := r.db.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf(
			"begin transaction: %w",
			err,
		)
	}

	defer tx.Rollback(ctx)

	const insertEventQuery = `
		INSERT INTO events (
			id,
			event_id,
			type,
			version,
			source,
			user_id,
			anonymous_id,
			session_id,
			timestamp,
			properties,
			created_at
		)
		VALUES (
			$1,
			$2,
			$3,
			$4,
			$5,
			$6,
			$7,
			$8,
			$9,
			$10,
			NOW()
		)
		ON CONFLICT (event_id)
		DO NOTHING
		RETURNING id
	`

	var id uuid.UUID

	err = tx.QueryRow(
		ctx,
		insertEventQuery,
		event.ID,
		event.EventID,
		event.Type,
		event.Version,
		event.Source,
		event.UserID,
		event.AnonymousID,
		event.SessionID,
		event.Timestamp,
		event.Properties,
	).Scan(&id)

	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrEventAlreadyExists
	}

	if err != nil {
		return fmt.Errorf(
			"insert event: %w",
			err,
		)
	}

	const insertOutboxQuery = `
		INSERT INTO outbox_events (
			id,
			event_id,
			event_type,
			payload
		)
		VALUES (
			$1,
			$2,
			$3,
			$4
		)
	`

	payload := map[string]any{
		"event_id":     event.EventID,
		"type":         event.Type,
		"version":      event.Version,
		"source":       event.Source,
		"user_id":      event.UserID,
		"anonymous_id": event.AnonymousID,
		"session_id":   event.SessionID,
		"timestamp":    event.Timestamp,
		"properties":   event.Properties,
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf(
			"marshal outbox payload: %w",
			err,
		)
	}

	_, err = tx.Exec(
		ctx,
		insertOutboxQuery,
		uuid.New(),
		event.EventID,
		event.Type,
		payloadJSON,
	)

	if err != nil {
		return fmt.Errorf(
			"insert outbox event: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf(
			"commit transaction: %w",
			err,
		)
	}

	return nil
}

func (r *EventRepository) FindByEventID(
	ctx context.Context,
	eventID uuid.UUID,
) (*domain.Event, error) {
	const query = `
		SELECT
			id,
			event_id,
			type,
			version,
			source,
			user_id,
			anonymous_id,
			session_id,
			timestamp,
			properties,
			created_at
		FROM events
		WHERE event_id = $1
	`

	var event domain.Event

	err := r.db.Pool.QueryRow(
		ctx,
		query,
		eventID,
	).Scan(
		&event.ID,
		&event.EventID,
		&event.Type,
		&event.Version,
		&event.Source,
		&event.UserID,
		&event.AnonymousID,
		&event.SessionID,
		&event.Timestamp,
		&event.Properties,
		&event.CreatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrEventNotFound
	}

	if err != nil {
		return nil, fmt.Errorf(
			"find event by event id: %w",
			err,
		)
	}

	return &event, nil
}
