package http

import (
	"time"

	"github.com/google/uuid"
)

type IngestEventRequest struct {
	Type        string         `json:"type" binding:"required"`
	EventID     uuid.UUID      `json:"event_id" binding:"required"`
	Version     int            `json:"version" binding:"required,gt=0"`
	Source      string         `json:"source" binding:"required"`
	UserID      string         `json:"user_id,omitempty"`
	AnonymousID string         `json:"anonymous_id,omitempty"`
	SessionID   string         `json:"session_id,omitempty"`
	Timestamp   time.Time      `json:"timestamp" binding:"required"`
	Properties  map[string]any `json:"properties" binding:"required"`
}
