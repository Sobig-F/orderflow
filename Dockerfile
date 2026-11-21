FROM golang:1.27.3-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/orderflow ./cmd/api

FROM alpine:3.22

RUN apk add --no-cache ca-certificates curl \
    && addgroup -S app \
    && adduser -S -G app app
COPY --from=builder /out/orderflow /usr/local/bin/orderflow

USER app
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
    CMD curl --fail --silent http://127.0.0.1:8080/ready >/dev/null || exit 1
ENTRYPOINT ["/usr/local/bin/orderflow"]
