package http_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/s7venking/eventflow/internal/event/application"
	"github.com/s7venking/eventflow/internal/event/domain"
	"github.com/s7venking/eventflow/internal/event/validation"
	httptransport "github.com/s7venking/eventflow/internal/transport/http"
)

func setupRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)

	registry := domain.NewInMemorySchemaRegistry()
	registry.RegisterSchema(application.NewPageViewSchema())
	registry.RegisterSchema(application.NewPurchaseSchema())
	registry.RegisterSchema(application.NewSearchSchema())

	validator := validation.NewValidator()
	ingestor := application.NewEventIngestor(registry, validator)
	handler := httptransport.NewEventHandler(ingestor)

	return httptransport.NewRouter(handler)
}

func TestEventHandler_Ingest(t *testing.T) {
	router := setupRouter()

	body := `{
		"type": "page_view",
		"version": 1,
		"source": "web",
		"anonymous_id": "anon_123",
		"session_id": "session_456",
		"timestamp": "2026-08-09T08:30:00Z",
		"properties": {
			"page": "/home",
			"device": "mobile"
		}
	}`

	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d", http.StatusCreated, rec.Code)
	}
}

func TestEventHandler_InvalidJSON(t *testing.T) {
	router := setupRouter()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", strings.NewReader(`{"type":`))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestEventHandler_InvalidSchema(t *testing.T) {
	router := setupRouter()

	body := `{
		"type": "page_view",
		"version": 1,
		"source": "web",
		"properties": {
			"device": "mobile"
		}
	}`

	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestEventHandler_UnknownEvent(t *testing.T) {
	router := setupRouter()

	body := `{
		"type": "abc",
		"version": 1,
		"source": "web",
		"properties": {}
	}`

	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}
