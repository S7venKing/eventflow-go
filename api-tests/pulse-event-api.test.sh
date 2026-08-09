#!/bin/bash

# Event Flow API - Test Commands
# API Base URL (change if running on different host/port)
BASE_URL="http://localhost:8080"

echo "=== Event Flow API Test Commands ==="
echo ""

# 1. Health Check
echo "1. Health Check:"
echo "curl -X GET $BASE_URL/health"
echo ""

# 2. Page View Event
echo "2. Page View Event:"
echo 'curl -X POST '"$BASE_URL"'/v1/events \'
echo '  -H "Content-Type: application/json" \'
echo '  -d '"'"'{
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
}'"'"
echo ""

# 3. Purchase Event
echo "3. Purchase Event:"
echo 'curl -X POST '"$BASE_URL"'/v1/events \'
echo '  -H "Content-Type: application/json" \'
echo '  -d '"'"'{
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
}'"'"
echo ""

# 4. Search Event
echo "4. Search Event:"
echo 'curl -X POST '"$BASE_URL"'/v1/events \'
echo '  -H "Content-Type: application/json" \'
echo '  -d '"'"'{
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
}'"'"
echo ""

echo "=== How to Run These Commands ==="
echo ""
echo "Option 1: Run individual curl commands:"
echo "  curl -X GET http://localhost:8080/health"
echo ""
echo "Option 2: Make this file executable and source it:"
echo "  chmod +x eventflow-event-api.test.sh"
echo "  source eventflow-event-api.test.sh"
echo ""
echo "Option 3: Use the curl commands directly from PowerShell:"
echo ""

# For PowerShell users
echo "=== PowerShell curl Commands (Windows) ==="
echo ""

echo "1. Health Check (PowerShell):"
echo 'curl.exe -X GET http://localhost:8080/health'
echo ""

echo "2. Page View Event (PowerShell):"
echo '@"'
echo 'curl.exe -X POST http://localhost:8080/v1/events \'
echo '  -H "Content-Type: application/json" \'
echo '  -d @{'
echo '    type="page_view"; version=1; source="web"; user_id="user_123"'
echo '    anonymous_id="anon_456"; session_id="session_789"'
echo '    timestamp="2026-08-09T10:30:00Z"; properties=@{page="/products"; device="desktop"}'
echo '  } | ConvertTo-Json'
echo '"@ | Invoke-Expression'
echo ""

echo "3. Purchase Event (PowerShell):"
echo 'curl.exe -X POST http://localhost:8080/v1/events \'
echo '  -H "Content-Type: application/json" \'
echo '  -d ''{"type":"purchase","version":1,"source":"web","user_id":"user_123","anonymous_id":"anon_456","session_id":"session_789","timestamp":"2026-08-09T10:35:00Z","properties":{"order_id":"ORD-2026-001","amount":99.99,"currency":"USD"}}'''
echo ""

echo "4. Search Event (PowerShell):"
echo 'curl.exe -X POST http://localhost:8080/v1/events \'
echo '  -H "Content-Type: application/json" \'
echo '  -d ''{"type":"search","version":1,"source":"web","user_id":"user_123","anonymous_id":"anon_456","session_id":"session_789","timestamp":"2026-08-09T10:40:00Z","properties":{"keyword":"golang event tracking","result_count":42}}'''
echo ""
