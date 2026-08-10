package main

import (
	"log"

	"github.com/s7venking/eventflow/internal/event/application"
	"github.com/s7venking/eventflow/internal/event/domain"
	httptransport "github.com/s7venking/eventflow/internal/transport/http"
)

func main() {
	// Schema Registry
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
