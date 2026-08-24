package middleware

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type contextKey string

const requestIDKey contextKey = "request_id"

const RequestIDHeader = "X-Request-ID"

func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader(RequestIDHeader)

		if requestID == "" {
			requestID = uuid.NewString()
		}

		c.Header(
			RequestIDHeader,
			requestID,
		)

		ctx := context.WithValue(
			c.Request.Context(),
			requestIDKey,
			requestID,
		)

		c.Request = c.Request.WithContext(ctx)

		c.Set(
			string(requestIDKey),
			requestID,
		)

		c.Next()
	}
}

func GetRequestID(ctx context.Context) string {
	requestID, ok := ctx.Value(requestIDKey).(string)
	if !ok {
		return ""
	}

	return requestID
}
