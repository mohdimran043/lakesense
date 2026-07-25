# LakeSense engine image WITH the IBM Db2 connector (build tag `db2`).
#
# Honesty note: Db2's only viable Go driver (github.com/ibmdb/go_ibm_db) requires
# IBM's native CLI driver (clidriver) and CGO_ENABLED=1. That is incompatible
# with the default engine image (engine/Dockerfile), which is a static, CGo-free
# distroless binary and therefore deliberately EXCLUDES Db2. This second image is
# glibc-based, bundles the clidriver, and builds with `-tags db2` so the Db2
# connector (and only it) is compiled in. Use this `lsengine-db2` image when you
# need Db2; use the default image for everything else.
#
# Build context is engine/:
#   docker build -f engine/Dockerfile.db2 -t lsengine-db2 engine/
#   docker run --rm lsengine-db2 catalog   # lists "db2" among the connectors

FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
# Fetch IBM's clidriver into the module cache, then stage it at a stable path.
RUN go run github.com/ibmdb/go_ibm_db/installer \
    && cp -r /go/pkg/mod/github.com/ibmdb/clidriver /opt/clidriver
COPY . ./
ARG VERSION=dev
ENV IBM_DB_HOME=/opt/clidriver \
    CGO_ENABLED=1 \
    CGO_CFLAGS=-I/opt/clidriver/include \
    CGO_LDFLAGS=-L/opt/clidriver/lib \
    LD_LIBRARY_PATH=/opt/clidriver/lib
RUN go build -tags db2 \
    -ldflags "-s -w -X github.com/lakesense/lakesense/engine/internal/buildinfo.Version=${VERSION}" \
    -o /out/lsengine ./cmd/lsengine

FROM debian:12-slim
# libdb2.so (clidriver) links against these at load time.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libxml2 libpam0g ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 65532 --user-group --home-dir /nonexistent --shell /usr/sbin/nologin lsengine
COPY --from=build /opt/clidriver /opt/clidriver
COPY --from=build /out/lsengine /usr/local/bin/lsengine
ENV LD_LIBRARY_PATH=/opt/clidriver/lib
USER lsengine
ENTRYPOINT ["/usr/local/bin/lsengine"]
