package http

import "github.com/gin-gonic/gin"

func NewRouter(
	eventHandler *EventHandler,
) *gin.Engine {
	r := gin.New()

	r.Use(gin.Logger())
	r.Use(gin.Recovery())

	v1 := r.Group("/api/v1")
	{
		events := v1.Group("/events")
		events.POST("", eventHandler.Ingest)
	}

	return r
}
