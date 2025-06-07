FROM golang:1.21-alpine AS builder

# Set working directory
WORKDIR /workspace

# Copy go mod files
COPY go.work go.work.sum ./
COPY pipeline/ingest/go.mod pipeline/ingest/go.sum ./pipeline/ingest/
COPY pipeline/config/go.mod pipeline/config/go.sum ./pipeline/config/

# Download dependencies
RUN cd pipeline/ingest && go mod download

# Copy source code
COPY pipeline/ ./pipeline/

# Build the application
RUN cd pipeline/ingest && CGO_ENABLED=0 GOOS=linux go build -o /ingestion .

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS calls
RUN apk --no-cache add ca-certificates

# Create non-root user
RUN adduser -D -s /bin/sh commerce

WORKDIR /home/commerce

# Copy the binary
COPY --from=builder /ingestion .

# Change ownership
RUN chown commerce:commerce /home/commerce/ingestion

USER commerce

# Run the application
CMD ["./ingestion"] 