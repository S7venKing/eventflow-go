# Pulse Event API - Test Files

This directory contains all the necessary files for testing the Pulse Event API.

## Files

### 1. **pulse-event-api.openapi.yaml**
OpenAPI 3.0 specification for the Pulse Event API.

**How to use:**
- Import into Postman:
  - Open Postman → Click "Import" → Select this file
  - This will create a collection with all endpoints
- Use with Swagger UI:
  - Upload to Swagger Editor at https://editor.swagger.io
- Use with API documentation tools

### 2. **pulse-event-api.postman.json**
Postman Collection with pre-configured requests and tests for all API endpoints.

**How to use:**
- Import into Postman:
  - Open Postman → Click "Import" → Select this file
  - This creates a ready-to-use collection with:
    - All event endpoints
    - Pre-configured environment variables
    - Built-in test scripts
    - Example requests for each event type

### 3. **pulse-event-api.test.sh**
Bash script with curl commands for testing the API from the terminal.

**How to use:**
```bash
# Make executable
chmod +x pulse-event-api.test.sh

# Run to see all curl commands
./pulse-event-api.test.sh

# Or run individual curl commands shown in the script
curl -X GET http://localhost:8080/health
```

## Quick Start

### Using Postman (Recommended)
1. Open Postman
2. Click **Import** button
3. Select `pulse-event-api.postman.json`
4. The collection will be imported with:
   - Configured base URL: `http://localhost:8080`
   - Default variables: userId, anonymousId, sessionId
   - Test scripts for validation
5. Start making requests from the collection

### Using curl (Linux/Mac)
```bash
# Health check
curl -X GET http://localhost:8080/health

# Page View Event
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

### Using PowerShell (Windows)
```powershell
# Health check
curl.exe -X GET http://localhost:8080/health

# Page View Event
curl.exe -X POST http://localhost:8080/v1/events `
  -H "Content-Type: application/json" `
  -d '{"type":"page_view","version":1,"source":"web","user_id":"user_123","anonymous_id":"anon_456","session_id":"session_789","timestamp":"2026-08-09T10:30:00Z","properties":{"page":"/products","device":"desktop"}}'
```

## API Endpoints

### Health Check
- **Endpoint:** `GET /health`
- **Description:** Check if the API is running

### Ingest Event
- **Endpoint:** `POST /v1/events`
- **Description:** Submit events to the API
- **Event Types Supported:**
  - `page_view` - Track page views
  - `purchase` - Track purchases
  - `search` - Track searches

## Event Types

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
    "page": "/products",
    "device": "desktop"
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
    "order_id": "ORD-2026-001",
    "amount": 99.99,
    "currency": "USD"
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
    "keyword": "golang event tracking",
    "result_count": 42
  }
}
```

## Environment Variables (Postman)

When importing the Postman collection, the following variables are pre-configured:

| Variable | Default Value | Description |
|----------|---------------|-------------|
| baseUrl | http://localhost:8080 | API base URL |
| userId | user_123 | Authenticated user ID |
| anonymousId | anon_456 | Anonymous visitor ID |
| sessionId | session_789 | Session identifier |
| timestamp | (auto-generated) | Event timestamp (ISO 8601) |

You can override these in Postman's environment settings.

## Notes

- Ensure the API is running on `http://localhost:8080` (or update the base URL)
- All timestamps should be in ISO 8601 format
- The API returns an `event_id` for each successfully processed event
- Status code 200 indicates successful event ingestion
