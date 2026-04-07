FROM docker.io/library/golang:1.26.1-bookworm AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# docs/ must exist before swag init runs; the .gitkeep ensures the directory
# is tracked in git but swag will overwrite the generated files at build time.
RUN go install github.com/swaggo/swag/cmd/swag@latest && \
    swag init --parseDependency --parseInternal \
              -g ./internal/orchestrator/api_server.go \
              --output docs && \
    CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w" \
    -o retina-orchestrator .

# ---- runtime ----------------------------------------------------------------
FROM docker.io/library/debian:bookworm-slim

LABEL maintainer="Dioptra <contact@dioptra.io>"

RUN apt-get update \
    && apt-get install --no-install-recommends --yes ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 1001 retina

WORKDIR /app
COPY --from=builder /build/retina-orchestrator .

# 8080 = HTTP API / Swagger UI
# 50050 = agent TCP listener
EXPOSE 8080 50050

USER retina

ENTRYPOINT ["./retina-orchestrator"]
