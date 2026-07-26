# LakeSense control-plane image WITH the IBM Db2 connector bundled.
#
# The default backend image (backend/Dockerfile) bundles a static, CGo-free
# lsengine that deliberately EXCLUDES Db2: its only viable Go driver
# (github.com/ibmdb/go_ibm_db) needs IBM's native CLI driver + CGO_ENABLED=1.
# This variant builds lsengine with `-tags db2` and bundles the clidriver so
# Db2 pipelines run from the stack. Opt in via the compose overlay:
#   docker compose -f docker-compose.yml -f docker-compose.db2.yml up -d --build
# Build context is the repo root (both modules available).
FROM golang:1.26-bookworm AS build
WORKDIR /src
ARG VERSION=dev

# Engine binary: CGo + `-tags db2`, with IBM's clidriver fetched into a stable path.
COPY engine/go.mod engine/go.sum engine/
RUN cd engine && go mod download
COPY engine/ engine/
RUN cd engine && go run github.com/ibmdb/go_ibm_db/installer \
    && cp -r /go/pkg/mod/github.com/ibmdb/clidriver /opt/clidriver
ENV IBM_DB_HOME=/opt/clidriver \
    CGO_ENABLED=1 \
    CGO_CFLAGS=-I/opt/clidriver/include \
    CGO_LDFLAGS=-L/opt/clidriver/lib \
    LD_LIBRARY_PATH=/opt/clidriver/lib
RUN cd engine && go build -tags db2 \
    -ldflags "-s -w -X github.com/lakesense/lakesense/engine/internal/buildinfo.Version=${VERSION}" \
    -o /out/lsengine ./cmd/lsengine

# Control-plane binary: static, CGo-free — identical to the default image.
COPY backend/go.mod backend/go.sum backend/
RUN cd backend && go mod download
COPY backend/ backend/
RUN cd backend && CGO_ENABLED=0 go build \
    -ldflags "-s -w -X github.com/lakesense/lakesense/backend/internal/buildinfo.Version=${VERSION}" \
    -o /out/lakesense ./cmd/lakesense

FROM debian:12-slim
# libdb2.so (clidriver) links against these at load time.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libxml2 libpam0g ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --user-group lakesense
COPY --from=build /opt/clidriver /opt/clidriver
COPY --from=build /out/lsengine  /usr/local/bin/lsengine
COPY --from=build /out/lakesense /usr/local/bin/lakesense
ENV LD_LIBRARY_PATH=/opt/clidriver/lib
USER lakesense
EXPOSE 8080
ENTRYPOINT ["lakesense"]
CMD ["serve"]
