package httptransport

import (
	"github.com/google/uuid"
	"github.com/s7venking/eventflow/internal/event/domain"
)

type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type ErrorResponse struct {
	Error APIError `json:"error"`
}

type EventResponse struct {
	ID          uuid.UUID      `json:"id"`
	EventID     uuid.UUID      `json:"event_id"`
	Type        string         `json:"type"`
	Version     int            `json:"version"`
	Source      string         `json:"source"`
	UserID      string         `json:"user_id,omitempty"`
	AnonymousID string         `json:"anonymous_id,omitempty"`
	SessionID   string         `json:"session_id,omitempty"`
	Timestamp   string         `json:"timestamp"`
	Properties  map[string]any `json:"properties"`
	CreatedAt   string         `json:"created_at"`
}

func fromEvent(event domain.Event) EventResponse {
	return EventResponse{
		ID:          event.ID,
		EventID:     event.EventID,
		Type:        event.Type,
		Version:     event.Version,
		Source:      event.Source,
		UserID:      event.UserID,
		AnonymousID: event.AnonymousID,
		SessionID:   event.SessionID,
		Timestamp:   event.Timestamp.UTC().Format("2006-01-02T15:04:05Z07:00"),
		Properties:  event.Properties,
		CreatedAt:   event.CreatedAt.UTC().Format("2006-01-02T15:04:05Z07:00"),
	}
}
