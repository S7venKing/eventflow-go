package application

import (
	"context"
	"errors"
	"log"
	"sync/atomic"
	"time"

	"github.com/s7venking/eventflow/internal/event/domain"
	"github.com/s7venking/eventflow/internal/platform/postgres"
)

// ErrShutdownTimeout is returned by Run when in-flight work did not finish
// within the shutdown timeout and had to be force-canceled.
var ErrShutdownTimeout = errors.New(
	"outbox worker: shutdown timeout exceeded, in-flight work canceled",
)

const (
	WorkerStateStarting = "STARTING"
	WorkerStateRunning  = "RUNNING"
	WorkerStateStopping = "STOPPING"
	WorkerStateStopped  = "STOPPED"
)

type workerState int32

const (
	workerStarting workerState = iota
	workerRunning
	workerStopping
	workerStopped
)

func (s workerState) String() string {
	switch s {
	case workerStarting:
		return WorkerStateStarting
	case workerRunning:
		return WorkerStateRunning
	case workerStopping:
		return WorkerStateStopping
	case workerStopped:
		return WorkerStateStopped
	default:
		return "UNKNOWN"
	}
}

type OutboxWorker struct {
	repository      *postgres.OutboxRepository
	publisher       EventPublisher
	interval        time.Duration
	batchSize       int
	maxRetries      int
	retryBaseDelay  time.Duration
	retryMaxDelay   time.Duration
	shutdownTimeout time.Duration
	state           atomic.Int32
}

type EventPublisher interface {
	Publish(
		ctx context.Context,
		event domain.OutboxEvent,
	) error
}

func NewOutboxWorker(
	repository *postgres.OutboxRepository,
	publisher EventPublisher,
	interval time.Duration,
	batchSize int,
	maxRetries int,
	retryBaseDelay time.Duration,
	retryMaxDelay time.Duration,
	shutdownTimeout time.Duration,
) *OutboxWorker {
	return &OutboxWorker{
		repository:      repository,
		publisher:       publisher,
		interval:        interval,
		batchSize:       batchSize,
		maxRetries:      maxRetries,
		retryBaseDelay:  retryBaseDelay,
		retryMaxDelay:   retryMaxDelay,
		shutdownTimeout: shutdownTimeout,
	}
}

func (w *OutboxWorker) State() string {
	return workerState(w.state.Load()).String()
}

// advanceState only moves the lifecycle forward, so a late transition from
// a concurrent goroutine can never move it backwards.
func (w *OutboxWorker) advanceState(s workerState) {
	for {
		current := w.state.Load()

		if int32(s) <= current {
			return
		}

		if w.state.CompareAndSwap(current, int32(s)) {
			return
		}
	}
}

// Run drives the worker until ctx is canceled. Cancellation stops new
// batches from being claimed; in-flight work keeps running on a separate
// context and is force-canceled only after shutdownTimeout. Run returns
// nil on a clean stop and ErrShutdownTimeout when the timeout forced
// cancellation.
func (w *OutboxWorker) Run(ctx context.Context) error {
	w.advanceState(workerStarting)

	// In-flight work must survive the shutdown signal, so it runs on a
	// context detached from ctx's cancellation.
	procCtx, procCancel := context.WithCancel(
		context.WithoutCancel(ctx),
	)
	defer procCancel()

	stopped := make(chan struct{})
	defer close(stopped)

	var timedOut atomic.Bool

	go func() {
		select {
		case <-ctx.Done():
		case <-stopped:
			return
		}

		w.advanceState(workerStopping)
		log.Println("worker_stopping")

		timer := time.NewTimer(w.shutdownTimeout)
		defer timer.Stop()

		select {
		case <-timer.C:
			timedOut.Store(true)
			log.Println("worker_shutdown_timeout")
			procCancel()

		case <-stopped:
		}
	}()

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	w.advanceState(workerRunning)
	log.Println("worker_started")

	for {
		select {
		case <-ctx.Done():
			w.advanceState(workerStopped)
			log.Println("worker_stopped")

			if timedOut.Load() {
				return ErrShutdownTimeout
			}

			return nil

		case <-ticker.C:
			// Never claim a new batch once shutdown has begun.
			if ctx.Err() != nil {
				continue
			}

			if err := w.process(procCtx); err != nil {
				log.Printf(
					"outbox worker error: %v",
					err,
				)
			}
		}
	}
}

func (w *OutboxWorker) process(ctx context.Context) error {
	events, err := w.repository.ClaimPending(
		ctx,
		w.batchSize,
	)
	if err != nil {
		return err
	}

	for _, event := range events {
		if err := w.publisher.Publish(ctx, event); err != nil {
			if event.Attempts >= w.maxRetries {
				if closeErr := w.repository.MarkClose(
					ctx,
					event.ID,
					err.Error(),
				); closeErr != nil {
					log.Printf(
						"mark outbox event closed: event_id=%s error=%v",
						event.EventID,
						closeErr,
					)
				}

				log.Printf(
					"publish failed permanently: event_id=%s attempts=%d error=%v",
					event.EventID,
					event.Attempts,
					err,
				)

				continue
			}

			retryNumber := event.Attempts + 1

			delay := RetryDelay(
				retryNumber,
				w.retryBaseDelay,
				w.retryMaxDelay,
			)

			retryAt := time.Now().Add(delay)

			if markErr := w.repository.MarkFailed(
				ctx,
				event.ID,
				err.Error(),
				retryAt,
			); markErr != nil {
				log.Printf(
					"mark outbox event failed: event_id=%s error=%v",
					event.EventID,
					markErr,
				)
			}

			log.Printf(
				"publish failed: event_id=%s retry=%d retry_at=%s error=%v",
				event.EventID,
				retryNumber,
				retryAt,
				err,
			)

			continue
		}

		if err := w.repository.MarkPublished(
			ctx,
			event.ID,
		); err != nil {
			log.Printf(
				"mark published failed: event_id=%s error=%v",
				event.EventID,
				err,
			)

			continue
		}

		log.Printf(
			"published: event_id=%s type=%s",
			event.EventID,
			event.EventType,
		)
	}

	return nil
}

type LogPublisher struct{}

func NewLogPublisher() *LogPublisher {
	return &LogPublisher{}
}

func (p *LogPublisher) Publish(
	ctx context.Context,
	event domain.OutboxEvent,
) error {
	log.Printf(
		"publish event: id=%s type=%s payload=%s",
		event.EventID,
		event.EventType,
		string(event.Payload),
	)

	return nil
}
