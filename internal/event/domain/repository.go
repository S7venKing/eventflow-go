package domain

import "context"

type EventRepository interface {
	Save(
		ctx context.Context,
		event Event,
	) error
}
