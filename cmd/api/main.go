package main

import (
	"context"
	"log"

	"github.com/s7venking/eventflow/internal/event/application"
	"github.com/s7venking/eventflow/internal/event/domain"
	"github.com/s7venking/eventflow/internal/platform/postgres"
	httptransport "github.com/s7venking/eventflow/internal/transport/http"
)

func main() {

	ctx := context.Background()

	db, err := postgres.New(
		ctx,
		"postgres://eventflow:eventflow@postgres:5432/eventflow",
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
