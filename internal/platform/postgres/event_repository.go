package postgres

import (
	"context"
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
	const query = `
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
			now()
		)
		ON CONFLICT (event_id)
		DO NOTHING
		RETURNING id
	`

	var id uuid.UUID

	err := r.db.Pool.QueryRow(
		ctx,
		query,
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
			"save event: %w",
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
