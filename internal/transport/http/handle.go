package http

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/s7venking/eventflow/internal/event/application"
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
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrorBody{
				Code:    "INVALID_JSON",
				Message: "invalid request body",
			},
		})
		return
	}

	event, err := h.ingestor.Handle(toCommand(req))
	if err != nil {
		handleHTTPError(c, err)
		return
	}

	c.JSON(http.StatusCreated, fromEvent(event))
}

func toCommand(
	req IngestEventRequest,
) application.IngestEventCommand {
	var cmd application.IngestEventCommand

	// Use the local Map util to copy matching exported fields
	if err := Map(&cmd, req); err != nil {
		// mapping should not fail for correct DTOs; in case it does, return zero-value command
		return application.IngestEventCommand{}
	}

	return cmd
}

func handleHTTPError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, application.ErrInvalidEventType):
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrorBody{
				Code:    "UNSUPPORTED_EVENT_TYPE",
				Message: err.Error(),
			},
		})

	case errors.Is(err, application.ErrInvalidVersion):
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrorBody{
				Code:    "UNSUPPORTED_EVENT_VERSION",
				Message: err.Error(),
			},
		})

	case errors.Is(err, application.ErrInvalidSource):
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrorBody{
				Code:    "INVALID_EVENT_SOURCE",
				Message: err.Error(),
			},
		})

	case errors.Is(err, application.ErrPropertiesRequired), errors.Is(err, application.ErrInvalidEventSchema):
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrorBody{
				Code:    "INVALID_EVENT_SCHEMA",
				Message: err.Error(),
			},
		})

	case errors.Is(err, application.ErrTimestampRequired):
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrorBody{
				Code:    "TIMESTAMP_REQUIRED",
				Message: err.Error(),
			},
		})

	default:
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrorBody{
				Code:    "INTERNAL_ERROR",
				Message: "internal server error",
			},
		})
	}
}
