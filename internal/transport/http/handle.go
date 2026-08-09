package http

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/s7venking/pulse/internal/event/application"
)

type EventHandler struct {
	ingestor *application.EventIngestor
}

func NewEventHandler(
	ingestor *application.EventIngestor,
) *EventHandler {
	return &EventHandler{
		ingestor: ingestor,
	}
}

func (h *EventHandler) Ingest(c *gin.Context) {
	var req IngestEventRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"code":    "INVALID_JSON",
				"message": "invalid request body",
			},
		})
		return
	}

	cmd := application.IngestEventCommand{
		Type:        req.Type,
		Version:     req.Version,
		Source:      req.Source,
		UserID:      req.UserID,
		AnonymousID: req.AnonymousID,
		SessionID:   req.SessionID,
		Timestamp:   req.Timestamp,
		Properties:  req.Properties,
	}

	result, err := h.ingestor.Handle(cmd)

	if err != nil {
		switch {
		case errors.Is(err, application.ErrInvalidEventType):
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{
					"code":    "UNSUPPORTED_EVENT_TYPE",
					"message": err.Error(),
				},
			})

		case errors.Is(err, application.ErrInvalidVersion):
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{
					"code":    "UNSUPPORTED_EVENT_VERSION",
					"message": err.Error(),
				},
			})

		default:
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{
					"code":    "INVALID_EVENT",
					"message": err.Error(),
				},
			})
		}

		return
	}

	response := IngestEventResponse{
		EventID: result.ID,
		Status:  "accepted",
	}

	c.JSON(
		http.StatusAccepted,
		response,
	)
}
