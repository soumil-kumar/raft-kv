FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ . ./cmd/raft-bench

FROM alpine:3.20
RUN adduser -D -u 10001 raft && mkdir /data && chown raft /data
COPY --from=builder /out/ /usr/local/bin/
USER raft
VOLUME /data
ENTRYPOINT ["raft-kv"]
