# Build stage
FROM --platform=$BUILDPLATFORM ubuntu/go:1.24-25.04_edge AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    gcc \
    libc6-dev \
    gcc-aarch64-linux-gnu \
    libc6-dev-arm64-cross \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

ARG TARGETOS=linux
ARG TARGETARCH
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        CGO_ENABLED=1 CC=arm-linux-gnueabi-gcc GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -a -o geulgyeol-html-storage . ; \
    else \
        CGO_ENABLED=1 CC=gcc GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -a -o geulgyeol-html-storage . ; \
    fi

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