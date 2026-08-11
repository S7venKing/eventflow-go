package application

import (
	"context"
	"errors"
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

type IngestResult struct {
	Event   domain.Event
	Created bool
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
) (IngestResult, error) {
	if err := validateCommand(cmd); err != nil {
		return IngestResult{}, err
	}

	schema, ok := i.registry.Get(
		cmd.Type,
		cmd.Version,
	)

	if !ok {
		return IngestResult{}, ErrInvalidEventType
	}

	if err := i.validator.Validate(
		schema,
		cmd.Properties,
	); err != nil {
		return IngestResult{}, err
	}

	event := domain.Event{
		ID:          generateID(),
		EventID:     generateID(),
		Type:        cmd.Type,
		Version:     cmd.Version,
		Source:      cmd.Source,
		UserID:      cmd.UserID,
		AnonymousID: cmd.AnonymousID,
		SessionID:   cmd.SessionID,
		Timestamp:   cmd.Timestamp,
		Properties:  cmd.Properties,
	}

	err := i.repository.Save(ctx, event)

	if err == nil {
		return IngestResult{
			Event:   event,
			Created: true,
		}, nil
	}

	if errors.Is(
		err,
		domain.ErrEventAlreadyExists,
	) {
		existing, findErr := i.repository.FindByEventID(
			ctx,
			event.EventID,
		)

		if findErr != nil {
			return IngestResult{}, fmt.Errorf(
				"find existing event: %w",
				findErr,
			)
		}

		return IngestResult{
			Event:   *existing,
			Created: false,
		}, nil
	}

	return IngestResult{}, fmt.Errorf(
		"save event: %w",
		err,
	)
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
