package postgres

import (
	"context"
	"encoding/json"
	"fmt"

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
	properties, err := json.Marshal(
		event.Properties,
	)

	if err != nil {
		return fmt.Errorf(
			"marshal event properties: %w",
			err,
		)
	}

	_, err = r.db.Pool.Exec(
		ctx,
		`
		INSERT INTO events (
			id,
			type,
			version,
			source,
			user_id,
			anonymous_id,
			session_id,
			timestamp,
			properties
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
			$9
		)
		`,
		event.ID,
		event.Type,
		event.Version,
		event.Source,
		event.UserID,
		event.AnonymousID,
		event.SessionID,
		event.Timestamp,
		properties,
	)

	if err != nil {
		return fmt.Errorf(
			"save event: %w",
			err,
		)
	}

	return nil
}
