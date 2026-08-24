package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/s7venking/eventflow/internal/config"
	"github.com/s7venking/eventflow/internal/event/application"
	"github.com/s7venking/eventflow/internal/event/domain"
	"github.com/s7venking/eventflow/internal/metrics"
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

	appLogger.Info(
		"database_configured",
		"min_connections", cfg.Database.MinConns,
		"max_connections", cfg.Database.MaxConns,
		"max_connection_lifetime", cfg.Database.MaxConnLifetime,
		"max_connection_idle_time", cfg.Database.MaxConnIdleTime,
	)

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

	publisher := application.NewLogPublisher()

	outboxWorker := application.NewOutboxWorker(
		outboxRepository,
		publisher,
		5*time.Second,
		100,
		3,
		1*time.Second,
		30*time.Second,
		cfg.ShutdownTimeout,
		outboxMetrics,
		workerLogger,
	)

	workerErrors := make(chan error, 1)

	go func() {
		workerErrors <- outboxWorker.Run(rootCtx)
	}()

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

	select {
	case err := <-workerErrors:
		if err != nil {
			appLogger.Error(
				"outbox_worker_stopped_with_error",
				"error", err,
			)
		} else {
			appLogger.Info(
				"outbox_worker_stopped",
			)
		}

	case <-time.After(cfg.ShutdownTimeout + 5*time.Second):
		appLogger.Error(
			"outbox_worker_shutdown_timeout",
			"timeout", cfg.ShutdownTimeout+5*time.Second,
		)
	}

	// ========================================
	// Application Stopped
	// ========================================

	appLogger.Info(
		"application_stopped",
	)
}
