package httptransport

import (
	"log/slog"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	eventmiddleware "github.com/s7venking/eventflow/internal/transport/http/middleware"
)

func NewRouter(
	eventHandler *EventHandler,
	healthHandler *HealthHandler,
	metricsRegistry *prometheus.Registry,
	logger *slog.Logger,
) *gin.Engine {
	router := gin.New()

	router.Use(
		gin.Recovery(),
		eventmiddleware.RequestID(),
		eventmiddleware.Logging(logger),
	)

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
