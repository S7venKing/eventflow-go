# Multi-stage Dockerfile for building the eventflow API

FROM golang:1.26 AS builder
WORKDIR /src

# Download dependencies first for caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the sources
COPY . .

# Build a statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" -o /app/eventflow ./cmd/api

# Final minimal image
FROM scratch
COPY --from=builder /app/eventflow /eventflow
EXPOSE 8080
ENTRYPOINT ["/eventflow"]
