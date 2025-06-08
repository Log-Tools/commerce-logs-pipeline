# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache gcc musl-dev librdkafka-dev pkgconf

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -o blob-monitor .

# Runtime stage
FROM alpine:latest

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache ca-certificates librdkafka-dev

# Copy the binary from builder stage
COPY --from=builder /app/blob-monitor .

# Create non-root user
RUN addgroup -g 1001 app && \
    adduser -D -s /bin/sh -u 1001 -G app app

# Change ownership
RUN chown -R app:app /app
USER app

# Expose health check port (if implemented)
EXPOSE 8080

# Command to run
ENTRYPOINT ["./blob-monitor"] 