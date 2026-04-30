FROM docker.io/library/golang:1.26.1-bookworm AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# docs/ must exist before swag init runs; the .gitkeep ensures the directory
# is tracked in git but swag will overwrite the generated files at build time.
RUN go install github.com/swaggo/swag/cmd/swag@v1.16.6 && \
    swag init --parseDependency --parseInternal \
              -g main.go \
              --output docs && \
    CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w" \
    -o retina-orchestrator .

# ---- runtime ----------------------------------------------------------------
FROM docker.io/library/debian:bookworm-slim

LABEL  org.opencontainers.image.authors="Dioptra <contact@dioptra.io>"

RUN apt-get update \
    && apt-get install --no-install-recommends --yes ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/retina-orchestrator .

USER retina

ENTRYPOINT ["./retina-orchestrator"]
