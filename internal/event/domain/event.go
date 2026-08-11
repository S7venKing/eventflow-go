package domain

import (
	"time"
)

type Event struct {
	ID          string
	EventID     string
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
