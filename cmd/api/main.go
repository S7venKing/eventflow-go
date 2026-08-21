package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"github.com/s7venking/eventflow/internal/config"
	"github.com/s7venking/eventflow/internal/event/application"
	"github.com/s7venking/eventflow/internal/event/domain"
	"github.com/s7venking/eventflow/internal/platform/postgres"
	httptransport "github.com/s7venking/eventflow/internal/transport/http"
)

func main() {
	// ========================================
	// Environment
	// ========================================

	if err := godotenv.Load(); err != nil {
		log.Printf("warning: .env not loaded")
	}

	// ========================================
	// Config
	// ========================================

	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	if err := cfg.Database.Validate(); err != nil {
		log.Fatal(err)
	}

	log.Printf(
		"database configured: %s",
		cfg.Database.URL,
	)

	log.Printf(
		"database pool: min=%d max=%d lifetime=%s idle=%s",
		cfg.Database.MinConns,
		cfg.Database.MaxConns,
		cfg.Database.MaxConnLifetime,
		cfg.Database.MaxConnIdleTime,
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
		log.Fatalf(
			"create database pool: %v",
			err,
		)
	}

	defer db.Close()

	// ========================================
	// Database Ping
	// ========================================

	if err := db.Ping(ctx); err != nil {
		log.Fatalf(
			"database ping failed: %v",
			err,
		)
	}

	log.Println("database connection established")

	// ========================================
	// Migration
	// ========================================

	if err := db.Migrate(ctx); err != nil {
		log.Fatalf(
			"database migration failed: %v",
			err,
		)
	}

	log.Println("database migration completed")

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

	log.Printf(
		"schema registry initialized: %d schemas",
		len(schemas),
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

	//WORKER
	// Outbox Publisher
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
		log.Println("eventflow API running on :4053")

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
		log.Fatalf(
			"server failed: %v",
			err,
		)

	case <-rootCtx.Done():
		log.Println("shutdown_signal_received")
	}

	// Restore default signal handling so a second SIGINT/SIGTERM kills
	// the process immediately instead of being swallowed.
	stop()

	// ========================================
	// Graceful Shutdown
	// ========================================

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		cfg.ShutdownTimeout,
	)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf(
			"HTTP server shutdown failed: %v",
			err,
		)
	}

	log.Println("HTTP server stopped")

	// The worker has been draining since the signal arrived and bounds
	// its own exit with cfg.ShutdownTimeout; the extra grace here only
	// guards against a publisher that ignores context cancellation.
	select {
	case err := <-workerErrors:
		if err != nil {
			log.Printf(
				"outbox worker stopped with error: %v",
				err,
			)
		}

	case <-time.After(cfg.ShutdownTimeout + 5*time.Second):
		log.Println("outbox worker did not stop in time")
	}

	log.Println("application_stopped")
}
