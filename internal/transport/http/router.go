package http

import "github.com/gin-gonic/gin"

func NewRouter(
	eventHandler *EventHandler,
) *gin.Engine {

	r := gin.New()

	r.Use(gin.Logger())
	r.Use(gin.Recovery())

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status": "ok",
		})
	})

	v1 := r.Group("/v1")
	{
		v1.POST("/events", eventHandler.Ingest)
	}

	return r
}
