package application

import (
	"time"

	"github.com/s7venking/eventflow/internal/event/domain"
	"github.com/s7venking/eventflow/internal/event/validation"
)

type IngestEventCommand struct {
	Type        string
	Version     int
	Source      string
	UserID      string
	AnonymousID string
	SessionID   string
	Timestamp   time.Time
	Properties  map[string]any
}

type EventIngestor struct {
	registry  domain.SchemaRegistry
	validator *validation.Validator
}

func NewEventIngestor(
	registry domain.SchemaRegistry,
	validator *validation.Validator,
) *EventIngestor {
	return &EventIngestor{
		registry:  registry,
		validator: validator,
	}
}

func (i *EventIngestor) Handle(
	cmd IngestEventCommand,
) (domain.Event, error) {

	schema, ok := i.registry.Get(
		cmd.Type,
		cmd.Version,
	)

	if !ok {
		return domain.Event{}, ErrInvalidEventType
	}

	if err := i.validator.Validate(
		schema,
		cmd.Properties,
	); err != nil {
		return domain.Event{}, err
	}

	event := domain.Event{
		ID:          generateEventID(),
		Type:        cmd.Type,
		Version:     cmd.Version,
		Source:      cmd.Source,
		UserID:      cmd.UserID,
		AnonymousID: cmd.AnonymousID,
		SessionID:   cmd.SessionID,
		Timestamp:   cmd.Timestamp,
		Properties:  cmd.Properties,
	}

	return event, nil
}
