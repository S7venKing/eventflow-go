package application

import (
	"context"
	"fmt"
	"time"

	"github.com/s7venking/eventflow/internal/event/domain"
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
	registry   domain.SchemaRegistry
	validator  *Validator
	repository domain.EventRepository
}

func NewEventIngestor(
	registry domain.SchemaRegistry,
	validator *Validator,
	repository domain.EventRepository,
) *EventIngestor {
	return &EventIngestor{
		registry:   registry,
		validator:  validator,
		repository: repository,
	}
}

func (i *EventIngestor) Handle(
	ctx context.Context,
	cmd IngestEventCommand,
) (domain.Event, error) {
	if err := validateCommand(cmd); err != nil {
		return domain.Event{}, err
	}

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

	if err := i.repository.Save(
		ctx,
		event,
	); err != nil {
		return domain.Event{}, fmt.Errorf(
			"save event: %w",
			err,
		)
	}

	return event, nil
}

func validateCommand(
	cmd IngestEventCommand,
) error {

	if cmd.Type == "" {
		return ErrInvalidEventType
	}

	if cmd.Version <= 0 {
		return ErrInvalidEventVersion
	}

	if cmd.Source == "" {
		return ErrInvalidSource
	}

	if cmd.Timestamp.IsZero() {
		return ErrTimestampRequired
	}

	if cmd.Properties == nil {
		return ErrPropertiesRequired
	}

	return nil
}
