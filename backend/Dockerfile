# LakeSense control-plane image. Build context is the repo root so both modules
# are available. Builds the static, CGo-free lakesense control plane AND bundles
# lsengine (the backend invokes it per pipeline run at LAKESENSE_ENGINE_PATH).
FROM golang:1.26-alpine AS build
WORKDIR /src
ARG VERSION=dev

# Engine binary.
COPY engine/go.mod engine/go.sum engine/
RUN cd engine && go mod download
COPY engine/ engine/
RUN cd engine && CGO_ENABLED=0 go build \
    -ldflags "-s -w -X github.com/lakesense/lakesense/engine/internal/buildinfo.Version=${VERSION}" \
    -o /out/lsengine ./cmd/lsengine

# Control-plane binary (migrations are embedded, no files needed at runtime).
COPY backend/go.mod backend/go.sum backend/
RUN cd backend && go mod download
COPY backend/ backend/
RUN cd backend && CGO_ENABLED=0 go build \
    -ldflags "-s -w -X github.com/lakesense/lakesense/backend/internal/buildinfo.Version=${VERSION}" \
    -o /out/lakesense ./cmd/lakesense

FROM alpine:3.20
RUN adduser -D -u 10001 lakesense
COPY --from=build /out/lsengine  /usr/local/bin/lsengine
COPY --from=build /out/lakesense /usr/local/bin/lakesense
USER lakesense
EXPOSE 8080
ENTRYPOINT ["lakesense"]
CMD ["serve"]
