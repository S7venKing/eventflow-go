# Event Flow API - Code Summary & Architecture

## Project Overview

**Project Name:** EvenFlow API (formerly called EventFlow)  
**Language:** Go 1.26.1  
**Module:** `github.com/s7venking/eventflow`  
**Purpose:** Real-time event ingestion and processing system for tracking user events (page views, purchases, searches, etc.)

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     HTTP CLIENT / POSTMAN                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                    POST /v1/events
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      HTTP TRANSPORT LAYER                        │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Gin Web Framework (Router)                             │   │
│  │  ├── GET /health (Health Check)                         │   │
│  │  └── POST /v1/events (Event Ingestion Handler)          │   │
│  │      └── EventHandler.Ingest()                          │   │
│  └─────────────────────────────────────────────────────────┘   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                  APPLICATION LAYER                              │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  EventIngestor                                          │   │
│  │  ├── Schema Registry Lookup                             │   │
│  │  ├── Properties Validation                              │   │
│  │  └── Event Object Creation                              │   │
│  └─────────────────────────────────────────────────────────┘   │
└────────────────────────────┬────────────────────────────────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
              ▼                             ▼
    ┌──────────────────────┐    ┌──────────────────────┐
    │  Schema Registry     │    │   Validator          │
    │                      │    │                      │
    │ InMemory Storage     │    │ String/Number        │
    │ Type/Version Lookup  │    │ Type Checking        │
    └──────────────────────┘    └──────────────────────┘
              │                             │
              └──────────────┬──────────────┘
                             │
                             ▼
                   ┌──────────────────────┐
                   │    Domain Layer      │
                   │                      │
                   │ - Event Domain Model │
                   │ - Field Definitions  │
                   │ - Schema Interface   │
                   └──────────────────────┘
```

---

## Directory Structure

```
pulse-go/
├── cmd/
│   └── api/
│       └── main.go                           # Application Entry Point
│
├── internal/
│   ├── event/
│   │   ├── application/
│   │   │   ├── ingest.go                     # EventIngestor (Business Logic)
│   │   │   ├── id.go                         # Event ID Generation
│   │   │   ├── error.go                      # Custom Errors
│   │   │   ├── page_view_schema.go           # Page View Event Schema
│   │   │   ├── purchase_schema.go            # Purchase Event Schema
│   │   │   └── search_schema.go              # Search Event Schema
│   │   │
│   │   ├── domain/
│   │   │   ├── event.go                      # Event Domain Model
│   │   │   ├── registry.go                   # Schema Registry (In-Memory)
│   │   │   ├── schema.go                     # EventSchema Interface
│   │   │   └── field.go                      # Field Type Definitions
│   │   │
│   │   └── validation/
│   │       ├── validator.go                  # Main Validator
│   │       ├── string.go                     # String Type Validation
│   │       └── number.go                     # Number Type Validation
│   │
│   └── transport/
│       └── http/
│           ├── handle.go                     # HTTP Handler
│           ├── router.go                     # Gin Router Setup
│           └── event_dto.go                  # Request/Response DTOs
│
├── api-tests/
│   ├── README.md                             # Testing Guide
│   ├── pulse-event-api.openapi.yaml          # OpenAPI Specification
│   ├── pulse-event-api.postman.json          # Postman Collection
│   └── pulse-event-api.test.sh               # Bash Test Script
│
├── go.mod                                    # Go Module Dependencies
├── go.sum                                    # Go Module Lock File
└── README.md                                 # Project README
```

---

## Component Details

### 1. **HTTP Transport Layer** (`internal/transport/http/`)

**Responsibility:** Handle HTTP requests and map them to application logic

#### Files:
- **router.go** - Gin web framework setup
  - Routes: `GET /health`, `POST /v1/events`
  - Middleware: Logger, Recovery
  
- **handle.go** - EventHandler
  - Binds JSON request body to `IngestEventRequest`
  - Calls `EventIngestor.Handle()` 
  - Returns JSON response with event_id and status
  - Error handling with proper HTTP status codes

- **event_dto.go** - Data Transfer Objects
  ```go
  type IngestEventRequest struct {
    Type        string         // page_view, purchase, search
    Version     int            // Event schema version
    Source      string         // web, mobile, api, etc.
    UserID      string         // Authenticated user ID
    AnonymousID string         // Anonymous visitor ID
    SessionID   string         // Session identifier
    Timestamp   time.Time      // ISO 8601 timestamp
    Properties  map[string]any // Event-specific data
  }
  
  type IngestEventResponse struct {
    EventID string // Unique event ID
    Status  string // accepted, queued, processed
  }
  ```

### 2. **Application Layer** (`internal/event/application/`)

**Responsibility:** Implement business logic for event processing

#### Files:
- **ingest.go** - EventIngestor
  - Validates event type exists in registry
  - Validates event properties against schema
  - Creates domain Event object with unique ID
  - Returns error if validation fails

- **page_view_schema.go** - Page View Event Schema
  - Type: `page_view`
  - Version: `1`
  - Required fields: `page` (string)
  - Optional fields: `device` (string)

- **purchase_schema.go** - Purchase Event Schema
  - Type: `purchase`
  - Version: `1`
  - Required fields: `order_id` (string), `amount` (number), `currency` (string)

- **search_schema.go** - Search Event Schema
  - Type: `search`
  - Version: `1`
  - Required fields: `keyword` (string), `result_count` (number)

- **id.go** - Event ID Generation
  - Uses ULID format for unique event identifiers

- **error.go** - Custom error types
  - `ErrInvalidEventType` - Event type not found in registry

### 3. **Domain Layer** (`internal/event/domain/`)

**Responsibility:** Core business entities and interfaces

#### Files:
- **event.go** - Event Domain Model
  ```go
  type Event struct {
    ID          string
    Type        string
    Version     int
    Source      string
    UserID      string
    AnonymousID string
    SessionID   string
    Timestamp   time.Time
    Properties  map[string]any
  }
  ```

- **schema.go** - EventSchema Interface
  ```go
  type EventSchema interface {
    Type() string
    Version() int
    Fields() []FieldDefinition
  }
  ```

- **registry.go** - InMemorySchemaRegistry
  - Stores schemas in map with key format: `{eventType}:v{version}`
  - `RegisterSchema(schema)` - Register new event schema
  - `Get(eventType, version)` - Retrieve schema by type and version

- **field.go** - Field Type Definitions
  ```go
  type FieldType string
  - FieldTypeString = "string"
  - FieldTypeNumber = "number"
  - FieldTypeBool   = "bool"
  
  type FieldDefinition struct {
    Name     string
    Type     FieldType
    Required bool
  }
  ```

### 4. **Validation Layer** (`internal/event/validation/`)

**Responsibility:** Validate event properties against schema definitions

#### Files:
- **validator.go** - Main Validator
  - Checks if all required fields exist
  - Validates field types against schema definitions
  - Returns error message if validation fails

- **string.go** - String Type Validation
  - Validates if value is string type
  - Can include length constraints (future enhancement)

- **number.go** - Number Type Validation
  - Validates if value is number (int or float)
  - Can include range constraints (future enhancement)

---

## Data Flow

### Event Ingestion Flow:

```
1. CLIENT REQUEST
   └─> HTTP: POST /v1/events
       Content: { type, version, source, user_id, ..., properties }

2. HTTP HANDLER (handle.go)
   └─> Parse JSON to IngestEventRequest
   └─> Convert to IngestEventCommand

3. EVENT INGESTOR (ingest.go)
   └─> Look up EventSchema in Registry
       └─> NOT FOUND? → Return ErrInvalidEventType
       └─> FOUND? → Continue
   
   └─> Validate properties using Validator
       └─> VALIDATION FAILED? → Return validation error
       └─> VALIDATION PASSED? → Continue
   
   └─> Create Event domain object
       └─> Generate unique ID (ULID)
       └─> Set all fields from command
   
   └─> Return Event (or error)

4. HTTP HANDLER (handle.go)
   └─> Check if error occurred
       └─> ERROR? → Return HTTP 400/422 with error message
       └─> SUCCESS? → Continue
   
   └─> Create IngestEventResponse
       └─> EventID: from Event object
       └─> Status: "accepted"
   
   └─> Return HTTP 200 with response JSON

5. CLIENT RESPONSE
   └─> { event_id: "evt_...", status: "accepted" }
```

---

## API Endpoints

### 1. Health Check
```
GET /health
Response: { "status": "ok" }
Status Code: 200
```

### 2. Ingest Event
```
POST /v1/events
Content-Type: application/json

Request Body:
{
  "type": "page_view|purchase|search",
  "version": 1,
  "source": "web|mobile|api",
  "user_id": "string",
  "anonymous_id": "string",
  "session_id": "string",
  "timestamp": "2026-08-09T10:30:00Z",
  "properties": { ... event-specific ... }
}

Success Response (200):
{
  "event_id": "evt_550e8400e29b41d4a716446655440000",
  "status": "accepted"
}

Error Responses:
- 400: Invalid JSON or missing required fields
- 422: Validation error (properties don't match schema)
- 500: Internal server error
```

---

## Event Types & Schemas

### Page View Event
```json
{
  "type": "page_view",
  "version": 1,
  "source": "web",
  "user_id": "user_123",
  "anonymous_id": "anon_456",
  "session_id": "session_789",
  "timestamp": "2026-08-09T10:30:00Z",
  "properties": {
    "page": "/products",        // REQUIRED: string
    "device": "desktop"         // OPTIONAL: string
  }
}
```

### Purchase Event
```json
{
  "type": "purchase",
  "version": 1,
  "source": "web",
  "user_id": "user_123",
  "anonymous_id": "anon_456",
  "session_id": "session_789",
  "timestamp": "2026-08-09T10:35:00Z",
  "properties": {
    "order_id": "ORD-2026-001",  // REQUIRED: string
    "amount": 99.99,             // REQUIRED: number
    "currency": "USD"            // REQUIRED: string
  }
}
```

### Search Event
```json
{
  "type": "search",
  "version": 1,
  "source": "web",
  "user_id": "user_123",
  "anonymous_id": "anon_456",
  "session_id": "session_789",
  "timestamp": "2026-08-09T10:40:00Z",
  "properties": {
    "keyword": "golang event tracking",  // REQUIRED: string
    "result_count": 42                   // REQUIRED: number
  }
}
```

---

## Technologies & Dependencies

### Core Framework
- **Gin Web Framework** (`github.com/gin-gonic/gin`) - HTTP routing and middleware
- **ULID** (`github.com/oklog/ulid/v2`) - Event ID generation
- **YAML** (`github.com/goccy/go-yaml`) - Configuration (future use)
- **MongoDB Driver** (`go.mongodb.org/mongo-driver/v2`) - Database (future use)
- **Protocol Buffers** (`google.golang.org/protobuf`) - Serialization (future use)

### Validation
- **Validator** (`github.com/go-playground/validator/v10`) - Struct validation
- **Custom Validators** - String/Number type checking

---

## Build & Run

### Prerequisites
- Go 1.26.1 or later
- git

### Installation
```bash
git clone https://github.com/s7venking/pulse-go.git
cd pulse-go
go mod download
```

### Build
```bash
go build -o eventflow ./cmd/api
```

### Run
```bash
go run ./cmd/api
# or
./eventflow
```

The API will start on `http://localhost:8080`

---

## Testing

### Using Postman
1. Import `api-tests/pulse-event-api.postman.json`
2. Pre-configured requests with test scripts
3. Environment variables for easy configuration

### Using OpenAPI/Swagger
1. Use `api-tests/pulse-event-api.openapi.yaml`
2. Upload to https://editor.swagger.io
3. Generate client SDKs if needed

### Using curl
1. Use commands in `api-tests/pulse-event-api.test.sh`
2. Or use curl directly from command line

### Example curl command:
```bash
curl -X POST http://localhost:8080/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "type": "page_view",
    "version": 1,
    "source": "web",
    "user_id": "user_123",
    "anonymous_id": "anon_456",
    "session_id": "session_789",
    "timestamp": "2026-08-09T10:30:00Z",
    "properties": {
      "page": "/products",
      "device": "desktop"
    }
  }'
```

---

## Design Patterns Used

### 1. **Dependency Injection**
- Components receive dependencies via constructor
- Easy to test and mock
- Decoupled components

### 2. **Repository Pattern**
- Schema Registry abstracts schema storage
- Can be replaced with database implementation

### 3. **Command Pattern**
- `IngestEventCommand` encapsulates request
- Separates HTTP request from business logic

### 4. **Factory Pattern**
- Schema constructors create schema instances
- Event constructor creates domain objects

### 5. **Strategy Pattern**
- Validator can be swapped with different implementations
- Field validators can be extended with custom logic

---

## Current Implementation Status

### ✅ Completed Features
- HTTP API with Gin framework
- Event ingestion endpoint
- 3 event types (page_view, purchase, search)
- Schema registry (in-memory)
- Event validation (required fields, type checking)
- Event ID generation (ULID)
- Health check endpoint
- Error handling and responses
- API documentation (OpenAPI/Postman)
- Testing files and guides

### 🚀 Future Enhancements
- Database persistence (MongoDB)
- Event streaming/queuing (Kafka, RabbitMQ)
- Advanced validation (regex, range constraints)
- Event transformation and enrichment
- Rate limiting and authentication
- Metrics and monitoring
- Event archival and retention policies
- Multi-tenant support
- Event replay capabilities
- WebSocket support for real-time events

---

## Key Metrics

- **Current Lines of Code:** ~1000 LOC (excluding tests)
- **Supported Event Types:** 3 (page_view, purchase, search)
- **API Endpoints:** 2 (health check, event ingestion)
- **Layer Architecture:** 4 layers (Transport, Application, Domain, Validation)
- **Database:** None (in-memory currently)
- **Message Queue:** None (synchronous processing currently)

---

## Git Commit History

1. **Initial commit** - Basic project structure
2. **Commit 804c47f** - Enhanced event system with field definitions
3. **Commit 11aaf08** - Added API test files

---

## Notes for Development

1. All schemas must implement the `EventSchema` interface
2. Properties validation happens in the Validator
3. Event IDs are generated using ULID format
4. Schema lookup is done by type and version
5. The in-memory registry is suitable for development; use persistent storage for production
6. Add new event types by creating new schema structs implementing EventSchema
7. Follow the existing package structure for new features
8. All HTTP responses are JSON formatted
9. Error messages are descriptive and match event type validation

---

## Contact & Support

- Repository: https://github.com/s7venking/pulse-go
- Issues: GitHub Issues
- Documentation: See api-tests/README.md for API testing guide
