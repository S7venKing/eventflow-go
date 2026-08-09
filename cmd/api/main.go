package main

import (
	"log"

	"github.com/s7venking/pulse/internal/event/application"
	"github.com/s7venking/pulse/internal/event/domain"
	"github.com/s7venking/pulse/internal/event/validation"
	httptransport "github.com/s7venking/pulse/internal/transport/http"
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
	validator := validation.NewValidator()

	ingestor := application.NewEventIngestor(
		registry,
		validator,
	)

	// HTTP Handler
	eventHandler := httptransport.NewEventHandler(ingestor)

	// Router
	router := httptransport.NewRouter(eventHandler)

	log.Println("Pulse API running on :8080")

	if err := router.Run(":8080"); err != nil {
		log.Fatal(err)
	}
}
