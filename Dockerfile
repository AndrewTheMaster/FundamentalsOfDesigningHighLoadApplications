# Build stage
FROM golang:1.24-alpine AS builder

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main cmd/main.go

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS
RUN apk --no-cache add ca-certificates

# Create app directory
WORKDIR /root/

# Copy binary from builder stage
COPY --from=builder /app/main .

# Create data directory
RUN mkdir -p /data

# Expose port for gRPC
EXPOSE 8080

# Set environment variables
ENV DATA_DIR=/data
ENV PORT=8080

# Run the application
CMD ["./main"]
