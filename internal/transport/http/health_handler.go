package httptransport

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/s7venking/eventflow/internal/platform/postgres"
)

type HealthHandler struct {
	db *postgres.DB
}

func NewHealthHandler(db *postgres.DB) *HealthHandler {
	return &HealthHandler{
		db: db,
	}
}

func (h *HealthHandler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
	})
}

func (h *HealthHandler) Ready(c *gin.Context) {
	ctx := c.Request.Context()

	if err := h.db.Ping(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "not_ready",
		})

		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "ready",
	})
}
