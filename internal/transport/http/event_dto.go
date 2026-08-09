package http

import "time"

type IngestEventRequest struct {
	Type        string         `json:"type"`
	Version     int            `json:"version"`
	Source      string         `json:"source"`
	UserID      string         `json:"user_id"`
	AnonymousID string         `json:"anonymous_id"`
	SessionID   string         `json:"session_id"`
	Timestamp   time.Time      `json:"timestamp"`
	Properties  map[string]any `json:"properties"`
}

type IngestEventResponse struct {
	EventID string `json:"event_id"`
	Status  string `json:"status"`
}
