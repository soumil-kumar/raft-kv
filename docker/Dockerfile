# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy dependency files first for better Docker caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code and build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /raft-kv .

# Runtime stage — minimal image
FROM alpine:3.19

RUN apk add --no-cache ca-certificates curl

COPY --from=builder /raft-kv /usr/local/bin/raft-kv

ENTRYPOINT ["raft-kv"]
