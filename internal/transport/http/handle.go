package http

import (
	"errors"
	"fmt"
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
		writeError(
			c,
			fmt.Errorf(
				"%w: %v",
				application.ErrInvalidRequest,
				err,
			),
		)
		return
	}
	ctx := c.Request.Context()
	result, err := h.ingestor.Handle(ctx, toCommand(req))
	if err != nil {
		writeError(c, err)
		return
	}

	status := http.StatusCreated
	if !result.Created {
		status = http.StatusOK
	}

	c.JSON(status, fromEvent(result.Event))
}

func (h *EventHandler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
	})
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

func writeError(
	c *gin.Context,
	err error,
) {
	status, code := mapError(err)

	c.JSON(
		status,
		ErrorResponse{
			Error: APIError{
				Code:    code,
				Message: err.Error(),
			},
		},
	)
}

func mapError(err error) (int, string) {
	switch {
	case errors.Is(
		err,
		application.ErrInvalidEventType,
	):
		return http.StatusBadRequest, "INVALID_EVENT_TYPE"

	case errors.Is(
		err,
		application.ErrInvalidEventVersion,
	):
		return http.StatusBadRequest, "INVALID_EVENT_VERSION"

	case errors.Is(
		err,
		application.ErrInvalidSource,
	):
		return http.StatusBadRequest, "INVALID_EVENT_SOURCE"

	case errors.Is(
		err,
		application.ErrInvalidEventSchema,
	):
		return http.StatusBadRequest, "INVALID_EVENT_SCHEMA"

	case errors.Is(
		err,
		application.ErrPropertiesRequired,
	):
		return http.StatusBadRequest, "INVALID_PROPERTIES"

	case errors.Is(
		err,
		application.ErrInvalidRequest,
	):
		return http.StatusBadRequest, "INVALID_REQUEST"

	default:
		return http.StatusInternalServerError, "INTERNAL_ERROR"
	}
}
