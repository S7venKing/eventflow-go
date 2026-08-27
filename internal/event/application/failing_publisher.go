package application

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"

	"github.com/google/uuid"

	"github.com/s7venking/eventflow/internal/event/domain"
)

// FailingPublisher wraps another EventPublisher and injects failures, so
// retry, permanent-failure and recovery paths can be exercised without a
// broker that actually breaks. The worker cannot tell an injected failure
// from a real one: it sees a plain error and runs its usual
// MarkFailed/MarkClose handling.
//
// Two independent failure modes:
//
//   - transient: each publish attempt fails with probability rate. When
//     maxFailuresPerEvent > 0 an event stops being failed once it has
//     been failed that many times, which pins the final outcome (every
//     non-permanent event eventually publishes) while the failure rate
//     still shapes the run.
//   - permanent: events whose type is in permanentTypes always fail, so
//     they exhaust the worker's retry budget and end in CLOSE.
type FailingPublisher struct {
	inner               EventPublisher
	rate                float64
	maxFailuresPerEvent int
	permanentTypes      map[string]struct{}

	mu       sync.Mutex
	failures map[uuid.UUID]int
	attempts int
	injected int
}

// NewFailingPublisher wraps inner. rate is the per-attempt transient
// failure probability in [0, 1). maxFailuresPerEvent caps how many times
// one event may be transiently failed (0 = no cap). Every event whose
// type appears in permanentTypes fails on every attempt.
func NewFailingPublisher(
	inner EventPublisher,
	rate float64,
	maxFailuresPerEvent int,
	permanentTypes ...string,
) *FailingPublisher {
	permanent := make(map[string]struct{}, len(permanentTypes))

	for _, t := range permanentTypes {
		permanent[t] = struct{}{}
	}

	return &FailingPublisher{
		inner:               inner,
		rate:                rate,
		maxFailuresPerEvent: maxFailuresPerEvent,
		permanentTypes:      permanent,
		failures:            make(map[uuid.UUID]int),
	}
}

func (p *FailingPublisher) Publish(
	ctx context.Context,
	event domain.OutboxEvent,
	logger *slog.Logger,
) error {
	if err := p.injectFailure(event); err != nil {
		return err
	}

	return p.inner.Publish(ctx, event, logger)
}

func (p *FailingPublisher) injectFailure(
	event domain.OutboxEvent,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.attempts++

	if _, ok := p.permanentTypes[event.EventType]; ok {
		p.injected++

		return fmt.Errorf(
			"injected permanent failure for event type %s",
			event.EventType,
		)
	}

	if p.rate <= 0 {
		return nil
	}

	if p.maxFailuresPerEvent > 0 &&
		p.failures[event.ID] >= p.maxFailuresPerEvent {
		return nil
	}

	if rand.Float64() >= p.rate {
		return nil
	}

	p.failures[event.ID]++
	p.injected++

	return fmt.Errorf(
		"injected transient failure (event failure %d)",
		p.failures[event.ID],
	)
}

// Attempts reports how many publish attempts reached this publisher.
func (p *FailingPublisher) Attempts() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.attempts
}

// InjectedFailures reports how many attempts were failed by injection.
func (p *FailingPublisher) InjectedFailures() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.injected
}
