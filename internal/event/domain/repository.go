package domain

import (
	"context"

	"github.com/google/uuid"
)

type EventRepository interface {
	Save(
		ctx context.Context,
		event Event,
	) error

	FindByEventID(
		ctx context.Context,
		eventID uuid.UUID,
	) (*Event, error)
}


