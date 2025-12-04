# Stage 1: Build the application
FROM golang:1.24-alpine AS builder

# Install build dependencies for CGO (if needed) and libheif
RUN apk add --no-cache build-base libheif-dev libde265-dev

# Set the working director
WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Copy the source code
COPY . .

# Update dependencies (since local go.mod might be outdated)
RUN go mod tidy

# Build the application
RUN go build -o main .

# Stage 2: Run the application
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache libheif libde265

# Set the working directory
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/main .

# Copy the templates directory
COPY --from=builder /app/templates ./templates

# Create directories for uploads and thumbnails
RUN mkdir -p uploads thumbnails

# Expose the port
EXPOSE 8080

# Run the application
CMD ["./main"]