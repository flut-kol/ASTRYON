# DEV ONLY: all Go services in one container (see README.md). Build context: repository root.
#   docker build -f infra/docker/apps/Dockerfile -t astryon_apps:dev .
# Builds in workspace mode (go.work) so the local libs/* replace/use rules apply.
FROM golang:1.27 AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOOS=linux
COPY go.work ./
COPY libs libs
COPY services services
COPY tests tests
RUN set -e; mkdir /out; \
    for m in \
      gateway:gateway clickhouse-sink:clickhouse-sink file-ingestion:file-api file-ingestion:file-worker \
      query-api:query-api replay-controller:replay-controller replay-worker:replay-worker \
      export-worker:export-worker quarantine:quarantine connector-manager:connector-manager \
      retry-controller:retry-controller; do \
      (cd "services/${m%%:*}" && go build -trimpath -ldflags="-s -w" -o "/out/${m##*:}" "./cmd/${m##*:}"); \
    done

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build --chmod=0755 /out/ /app/
COPY --chmod=0755 infra/docker/apps/entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/bin/sh", "/entrypoint.sh"]
