package application

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/s7venking/eventflow/internal/event/domain"
	"github.com/s7venking/eventflow/internal/metrics"
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
	metrics         *metrics.OutboxMetrics
	logger          *slog.Logger
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
	metrics *metrics.OutboxMetrics,
	logger *slog.Logger,
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
		metrics:         metrics,
		logger:          logger,
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
		w.logger.Info(
			"worker_stopping",
		)

		timer := time.NewTimer(w.shutdownTimeout)
		defer timer.Stop()

		select {
		case <-timer.C:
			timedOut.Store(true)
			w.logger.Error(
				"worker_shutdown_timeout",
				"timeout",
				w.shutdownTimeout,
			)
			procCancel()

		case <-stopped:
		}
	}()

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	w.advanceState(workerRunning)
	w.logger.Info(
		"worker_started",
		"interval",
		w.interval,
		"batch_size",
		w.batchSize,
	)

	for {
		select {
		case <-ctx.Done():
			w.advanceState(workerStopped)
			w.logger.Info("worker_stopped")

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
				w.logger.Error(
					"outbox_worker_error",
					"error",
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

		w.logger.Error(
			"claim_pending_failed",
			"error",
			err,
		)

		return err
	}

	for _, event := range events {
		if err := w.publisher.Publish(ctx, event); err != nil {

			// Context cancellation is part of worker lifecycle,
			// not a publish failure.
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				return err
			}

			// Real publish failure.
			w.metrics.Failed.Inc()

			// Max retries reached -> CLOSE.
			if event.Attempts >= w.maxRetries {
				if closeErr := w.repository.MarkClose(
					ctx,
					event.ID,
					err.Error(),
				); closeErr != nil {
					w.logger.Error(
						"mark_outbox_event_closed",
						"event_id",
						event.EventID,
						"error",
						closeErr,
					)
				} else {
					// Only count CLOSE when DB update succeeds.
					w.metrics.Closed.Inc()
				}

				w.logger.Error(
					"publish_failed_permanently",
					"event_id",
					event.EventID,
					"event_type",
					event.EventType,
					"attempts",
					event.Attempts,
					"error",
					err,
				)

				continue
			}

			// Retry number is based on the current Attempts.
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
				w.metrics.Failed.Inc()

				w.logger.Error(
					"mark_outbox_event_failed",
					"event_id",
					event.EventID,
					"error",
					markErr,
				)
			}

			w.logger.Warn(
				"publish_failed",
				"event_id",
				event.EventID,
				"event_type",
				event.EventType,
				"retry",
				retryNumber,
				"retry_at",
				retryAt,
				"error",
				err,
			)

			continue
		}

		// Publish succeeded.
		// Only consider the event successfully published
		// after the database state is updated.
		if err := w.repository.MarkPublished(
			ctx,
			event.ID,
		); err != nil {
			w.logger.Error(
				"mark_published_failed",
				"event_id", event.EventID,
				"event_type", event.EventType,
				"error", err,
			)

			continue
		}

		// DB state successfully changed to PUBLISHED.
		w.metrics.Published.Inc()

		w.logger.Info(
			"event_published",
			"event_id",
			event.EventID,
			"event_type",
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
