package domain

import (
	"time"

	"github.com/google/uuid"
)

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
