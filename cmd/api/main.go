package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/s7venking/eventflow/internal/config"
	"github.com/s7venking/eventflow/internal/event/application"
	"github.com/s7venking/eventflow/internal/event/domain"
	"github.com/s7venking/eventflow/internal/metrics"
	kafkaplatform "github.com/s7venking/eventflow/internal/platform/kafka"
	"github.com/s7venking/eventflow/internal/platform/logger"
	"github.com/s7venking/eventflow/internal/platform/postgres"
	httptransport "github.com/s7venking/eventflow/internal/transport/http"
)

func main() {
	// ========================================
	// Logger
	// ========================================

	appLogger := logger.New()

	appLogger.Info(
		"application_starting",
		"service", "eventflow",
	)

	workerLogger := appLogger.With(
		"service", "eventflow",
		"component", "outbox-worker",
	)

	// ========================================
	// Environment
	// ========================================

	if err := godotenv.Load(); err != nil {
		appLogger.Warn(
			"environment_file_not_loaded",
			"error", err,
		)
	}

	// ========================================
	// Config
	// ========================================

	cfg, err := config.Load()
	if err != nil {
		appLogger.Error(
			"config_load_failed",
			"error", err,
		)

		return
	}

	if err := cfg.Database.Validate(); err != nil {
		appLogger.Error(
			"database_config_invalid",
			"error", err,
		)

		return
	}

	if err := cfg.Outbox.Validate(); err != nil {
		appLogger.Error(
			"outbox_config_invalid",
			"error", err,
		)

		return
	}

	if err := cfg.Kafka.Validate(); err != nil {
		appLogger.Error(
			"kafka_config_invalid",
			"error", err,
		)

		return
	}

	appLogger.Info(
		"database_configured",
		"min_connections", cfg.Database.MinConns,
		"max_connections", cfg.Database.MaxConns,
		"max_connection_lifetime", cfg.Database.MaxConnLifetime,
		"max_connection_idle_time", cfg.Database.MaxConnIdleTime,
	)

	appLogger.Info(
		"outbox_configured",
		"workers", cfg.Outbox.Workers,
		"batch_size", cfg.Outbox.BatchSize,
		"interval", cfg.Outbox.Interval,
		"stale_timeout", cfg.Outbox.StaleTimeout,
		"publish_failure_rate", cfg.Outbox.PublishFailureRate,
	)

	// Each worker holds a pooled connection for the length of its claim
	// transaction, so a pool smaller than the worker count makes the
	// workers queue on connection acquisition instead of on Postgres.
	if int(cfg.Database.MaxConns) < cfg.Outbox.Workers {
		appLogger.Warn(
			"outbox_workers_exceed_pool_size",
			"workers", cfg.Outbox.Workers,
			"max_connections", cfg.Database.MaxConns,
		)
	}

	// ========================================
	// Database
	// ========================================

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	db, err := postgres.NewDB(
		ctx,
		cfg.Database,
	)
	if err != nil {
		appLogger.Error(
			"database_connection_create_failed",
			"error", err,
		)

		return
	}

	defer db.Close()

	// ========================================
	// Database Ping
	// ========================================

	if err := db.Ping(ctx); err != nil {
		appLogger.Error(
			"database_ping_failed",
			"error", err,
		)

		return
	}

	appLogger.Info(
		"database_connection_established",
	)

	// ========================================
	// Migration
	// ========================================

	if err := db.Migrate(ctx); err != nil {
		appLogger.Error(
			"database_migration_failed",
			"error", err,
		)

		return
	}

	appLogger.Info(
		"database_migration_completed",
	)

	// ========================================
	// Repository
	// ========================================

	repository := postgres.NewEventRepository(db)
	outboxRepository := postgres.NewOutboxRepository(db)

	// ========================================
	// Schema Registry
	// ========================================

	registry := domain.NewInMemorySchemaRegistry()

	schemas := []domain.EventSchema{
		application.NewPageViewSchema(),
		application.NewPurchaseSchema(),
		application.NewSearchSchema(),
	}

	for _, schema := range schemas {
		registry.RegisterSchema(schema)
	}

	appLogger.Info(
		"schema_registry_initialized",
		"schema_count", len(schemas),
	)

	// ========================================
	// Application
	// ========================================

	validator := application.NewValidator()

	ingestor := application.NewEventIngestor(
		registry,
		validator,
		repository,
	)

	// ========================================
	// Prometheus Metrics
	// ========================================

	metricsRegistry := prometheus.NewRegistry()

	outboxMetrics := metrics.NewOutboxMetrics(
		metricsRegistry,
	)

	kafkaMetrics := metrics.NewKafkaMetrics(
		metricsRegistry,
	)

	appLogger.Info(
		"metrics_initialized",
	)

	// ========================================
	// HTTP
	// ========================================

	eventHandler := httptransport.NewEventHandler(
		ingestor,
	)

	healthHandler := httptransport.NewHealthHandler(
		db,
	)

	router := httptransport.NewRouter(
		eventHandler,
		healthHandler,
		metricsRegistry,
		appLogger,
	)

	// ========================================
	// HTTP Server
	// ========================================

	server := &http.Server{
		Addr:    ":4053",
		Handler: router,
	}

	// ========================================
	// Shutdown Signal Context
	// ========================================

	rootCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// ========================================
	// WORKER
	// ========================================

	// Every worker shares one repository, pool, publisher, metrics set and
	// context. No event is assigned to a worker: they all call
	// ClaimPending, and Postgres decides who gets what through
	// FOR UPDATE SKIP LOCKED. worker_id only labels the log lines.
	//
	// The workers only ever see the EventPublisher interface. Kafka lives
	// behind it in internal/platform/kafka; a nil Publish means the broker
	// acknowledged the write, which is what lets the worker mark PUBLISHED.
	var publisher application.EventPublisher

	var kafkaPublisher *kafkaplatform.Publisher

	switch cfg.Outbox.Publisher {
	case config.OutboxPublisherKafka:
		kafkaPublisher = kafkaplatform.NewPublisher(
			cfg.Kafka,
			kafkaMetrics,
		)

		publisher = kafkaPublisher

		appLogger.Info(
			"kafka_publisher_configured",
			"brokers", cfg.Kafka.Brokers,
			"topic", cfg.Kafka.Topic,
			"client_id", cfg.Kafka.ClientID,
			"write_timeout", cfg.Kafka.WriteTimeout,
			"max_attempts", cfg.Kafka.MaxAttempts,
		)

	default:
		publisher = application.NewLogPublisher()

		appLogger.Warn(
			"log_publisher_configured",
			"reason", "OUTBOX_PUBLISHER=log, events are logged, not sent to Kafka",
		)
	}

	if cfg.Outbox.PublishFailureRate > 0 {
		appLogger.Warn(
			"publish_failure_injection_enabled",
			"rate", cfg.Outbox.PublishFailureRate,
		)

		publisher = application.NewFailingPublisher(
			publisher,
			cfg.Outbox.PublishFailureRate,
			0,
		)
	}

	// A stale timeout below the shutdown drain window can reclaim rows a
	// slow-but-alive worker is still publishing, which double-publishes.
	if cfg.Outbox.StaleTimeout <= cfg.ShutdownTimeout {
		appLogger.Warn(
			"outbox_stale_timeout_below_shutdown_timeout",
			"stale_timeout", cfg.Outbox.StaleTimeout,
			"shutdown_timeout", cfg.ShutdownTimeout,
		)
	}

	var workerGroup sync.WaitGroup

	workerErrors := make(chan error, cfg.Outbox.Workers)

	for i := 1; i <= cfg.Outbox.Workers; i++ {
		outboxWorker := application.NewOutboxWorker(
			outboxRepository,
			publisher,
			cfg.Outbox.Interval,
			cfg.Outbox.BatchSize,
			3,
			1*time.Second,
			30*time.Second,
			cfg.ShutdownTimeout,
			cfg.Outbox.StaleTimeout,
			outboxMetrics,
			workerLogger.With("worker_id", i),
		)

		workerGroup.Add(1)

		go func() {
			defer workerGroup.Done()

			workerErrors <- outboxWorker.Run(rootCtx)
		}()
	}

	workersStopped := make(chan struct{})

	go func() {
		workerGroup.Wait()
		close(workersStopped)
	}()

	appLogger.Info(
		"outbox_workers_started",
		"workers", cfg.Outbox.Workers,
	)

	// ========================================
	// Start Server
	// ========================================

	serverErrors := make(chan error, 1)

	go func() {
		appLogger.Info(
			"http_server_started",
			"address", server.Addr,
		)

		if err := server.ListenAndServe(); err != nil &&
			err != http.ErrServerClosed {
			serverErrors <- err
		}
	}()

	// ========================================
	// Signal
	// ========================================

	select {
	case err := <-serverErrors:
		appLogger.Error(
			"http_server_failed",
			"error", err,
		)

	case <-rootCtx.Done():
		appLogger.Info(
			"shutdown_signal_received",
		)
	}

	// Restore default signal handling so a second
	// SIGINT/SIGTERM kills the process immediately.
	stop()

	// ========================================
	// Graceful Shutdown
	// ========================================

	appLogger.Info(
		"application_shutdown_started",
		"timeout", cfg.ShutdownTimeout,
	)

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		cfg.ShutdownTimeout,
	)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		appLogger.Error(
			"http_server_shutdown_failed",
			"error", err,
		)
	}

	appLogger.Info(
		"http_server_stopped",
	)

	// ========================================
	// Wait Worker
	// ========================================

	// Every worker has been draining since the signal arrived and each
	// bounds its own exit with cfg.ShutdownTimeout; the extra grace here
	// only guards against a publisher that ignores context cancellation.
	select {
	case <-workersStopped:
		close(workerErrors)

		for err := range workerErrors {
			if err != nil {
				appLogger.Error(
					"outbox_worker_stopped_with_error",
					"error", err,
				)
			}
		}

		appLogger.Info(
			"outbox_workers_stopped",
			"workers", cfg.Outbox.Workers,
		)

	case <-time.After(cfg.ShutdownTimeout + 5*time.Second):
		appLogger.Error(
			"outbox_worker_shutdown_timeout",
			"timeout", cfg.ShutdownTimeout+5*time.Second,
		)
	}

	// ========================================
	// Close Publisher
	// ========================================

	// Only after the workers are done, so no publish is in flight.
	if kafkaPublisher != nil {
		if err := kafkaPublisher.Close(); err != nil {
			appLogger.Error(
				"kafka_publisher_close_failed",
				"error", err,
			)
		} else {
			appLogger.Info(
				"kafka_publisher_closed",
			)
		}
	}

	// ========================================
	// Application Stopped
	// ========================================

	appLogger.Info(
		"application_stopped",
	)
}
