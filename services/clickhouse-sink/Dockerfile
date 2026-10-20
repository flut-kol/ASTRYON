# Build context = repo root:  docker build -f services/clickhouse-sink/Dockerfile -t astryon/clickhouse-sink .
FROM golang:1.22 AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath GOWORK=off
COPY libs ./libs
COPY services/clickhouse-sink ./services/clickhouse-sink
WORKDIR /src/services/clickhouse-sink
RUN go mod download
RUN go build -ldflags="-s -w" -o /out/clickhouse-sink ./cmd/clickhouse-sink

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/clickhouse-sink /clickhouse-sink
USER 65532:65532
EXPOSE 9102
ENTRYPOINT ["/clickhouse-sink"]
