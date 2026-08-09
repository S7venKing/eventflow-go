package http

import "time"

type IngestEventRequest struct {
	Type        string         `json:"type"`
	Version     int            `json:"version"`
	Source      string         `json:"source"`
	UserID      string         `json:"user_id,omitempty"`
	AnonymousID string         `json:"anonymous_id,omitempty"`
	SessionID   string         `json:"session_id,omitempty"`
	Timestamp   time.Time      `json:"timestamp"`
	Properties  map[string]any `json:"properties"`
}
