# Build stage
FROM golang:1.24.3-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
ARG TARGETOS=linux
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -a -installsuffix cgo -o geulgyeol-html-storage .

# Final stage
FROM alpine:latest


# Create data directory
RUN mkdir -p /data

WORKDIR /root

# Copy the binary from builder
COPY --from=builder /app/geulgyeol-html-storage .

# Expose the default port
EXPOSE 8080

# Set the data path as a volume
VOLUME ["/data"]

# Run the application
ENTRYPOINT ["./geulgyeol-html-storage"]