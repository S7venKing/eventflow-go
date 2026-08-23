package httptransport

import (
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewRouter(
	eventHandler *EventHandler,
	healthHandler *HealthHandler,
	metricsRegistry *prometheus.Registry,
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

	router.GET(
		"/metrics",
		gin.WrapH(
			promhttp.HandlerFor(
				metricsRegistry,
				promhttp.HandlerOpts{},
			),
		),
	)

	return router
}
