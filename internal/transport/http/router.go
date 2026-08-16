package httptransport

import "github.com/gin-gonic/gin"

func NewRouter(
	eventHandler *EventHandler,
	healthHandler *HealthHandler,
) *gin.Engine {
	router := gin.Default()

	router.POST(
		"/api/v1/events",
		eventHandler.Ingest,
	)

	router.GET(
		"/health",
		healthHandler.Health,
	)

	router.GET(
		"/ready",
		healthHandler.Ready,
	)

	return router
}
