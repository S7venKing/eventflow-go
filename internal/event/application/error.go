package application

import "errors"

var (
	ErrInvalidEventType = errors.New(
		"unsupported event type",
	)

	ErrInvalidEventVersion = errors.New(
		"unsupported event version",
	)

	ErrInvalidSource = errors.New(
		"unsupported event source",
	)

	ErrInvalidEventSchema = errors.New(
		"invalid event schema",
	)

	ErrInvalidRequest = errors.New(
		"invalid request",
	)

	ErrPropertiesRequired = errors.New(
		"properties is required",
	)

	ErrTimestampRequired = errors.New(
		"timestamp is required",
	)
)
