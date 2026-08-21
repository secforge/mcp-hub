# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/

# Only mcp-hub-server is deployed as a service; mcp-hub-client runs locally
# via Claude Code over stdio, not in a container.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o mcp-hub-server ./cmd/mcp-hub-server

# Runtime stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates

RUN adduser -D -u 1000 mcphub
RUN mkdir -p /logs && chown -R mcphub:mcphub /logs

WORKDIR /app
COPY --from=builder /build/mcp-hub-server .

USER mcphub

ENV MCP_HUB_LOG_DIR=/logs

EXPOSE 8765

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD nc -z localhost 8765 || exit 1

ENTRYPOINT ["./mcp-hub-server"]
CMD ["-addr", ":8765"]
