package application

import "errors"

var (
	ErrInvalidEventType = errors.New(
		"unsupported event type",
	)

	ErrInvalidVersion = errors.New(
		"unsupported event version",
	)

	ErrInvalidSource = errors.New(
		"unsupported event source",
	)

	ErrPropertiesRequired = errors.New(
		"properties is required",
	)
)

func validateCommand(
	cmd IngestEventCommand,
) error {

	if cmd.Type == "" {
		return ErrInvalidEventType
	}

	if cmd.Version <= 0 {
		return ErrInvalidVersion
	}

	if cmd.Source == "" {
		return ErrInvalidSource
	}

	if cmd.Timestamp.IsZero() {
		return errors.New(
			"timestamp is required",
		)
	}

	if cmd.Properties == nil {
		return errors.New(
			"properties is required",
		)
	}

	return nil
}
