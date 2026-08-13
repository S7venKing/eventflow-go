package main

import (
	"context"
	"log"

	"github.com/joho/godotenv"
	"github.com/s7venking/eventflow/internal/config"
	"github.com/s7venking/eventflow/internal/event/application"
	"github.com/s7venking/eventflow/internal/event/domain"
	"github.com/s7venking/eventflow/internal/platform/postgres"
	httptransport "github.com/s7venking/eventflow/internal/transport/http"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Printf("warning: .env not loaded")
	}

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
		"database url: %s",
		cfg.Database.URL,
	)

	log.Printf(
		"max conns: %d",
		cfg.Database.MaxConns,
	)

	log.Printf(
		"min conns: %d",
		cfg.Database.MinConns,
	)

	log.Printf(
		"max lifetime: %s",
		cfg.Database.MaxConnLifetime,
	)

	log.Printf(
		"max idle time: %s",
		cfg.Database.MaxConnIdleTime,
	)

	ctx := context.Background()

	db, err := postgres.New(
		ctx,
		cfg.Database.URL,
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := db.Migrate(ctx); err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	// Schema Registry
	repository := postgres.NewEventRepository(db)

	registry := domain.NewInMemorySchemaRegistry()

	schemas := []domain.EventSchema{
		application.NewPageViewSchema(),
		application.NewPurchaseSchema(),
		application.NewSearchSchema(),
	}

	for _, schema := range schemas {
		registry.RegisterSchema(schema)
	}

	// Application
	validator := application.NewValidator()

	ingestor := application.NewEventIngestor(
		registry,
		validator,
		repository,
	)

	// HTTP Handler
	eventHandler := httptransport.NewEventHandler(ingestor)

	// Router
	router := httptransport.NewRouter(eventHandler)

	log.Println("eventflow API running on :4053")

	if err := router.Run(":4053"); err != nil {
		log.Fatal(err)
	}
}
