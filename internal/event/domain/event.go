package domain

import (
	"time"

	"github.com/google/uuid"
)

type Event struct {
	ID          uuid.UUID
	EventID     uuid.UUID
	Type        string
	Version     int
	Source      string
	UserID      string
	AnonymousID string
	SessionID   string
	Timestamp   time.Time
	Properties  map[string]any
	CreatedAt   time.Time
}
