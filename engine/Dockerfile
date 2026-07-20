# LakeSense engine image. Build context is the repo root so the engine module
# is available at engine/. Produces a static, CGo-free lsengine on a distroless
# base (no shell, minimal attack surface).
FROM golang:1.26-alpine AS build
WORKDIR /src/engine
COPY engine/go.mod engine/go.sum ./
RUN go mod download
COPY engine/ ./
ARG VERSION=dev
RUN CGO_ENABLED=0 go build \
    -ldflags "-s -w -X github.com/lakesense/lakesense/engine/internal/buildinfo.Version=${VERSION}" \
    -o /out/lsengine ./cmd/lsengine

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/lsengine /usr/local/bin/lsengine
ENTRYPOINT ["/usr/local/bin/lsengine"]
