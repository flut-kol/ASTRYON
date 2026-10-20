# Build context: repository root.  docker build -f services/ch-aggregator/Dockerfile -t astryon/ch-aggregator .
FROM golang:1.22 AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOWORK=off
COPY services/ch-aggregator services/ch-aggregator
WORKDIR /src/services/ch-aggregator
RUN go build -trimpath -ldflags="-s -w" -o /out/ch-aggregator ./cmd/ch-aggregator

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ch-aggregator /ch-aggregator
USER nonroot:nonroot
ENTRYPOINT ["/ch-aggregator"]
