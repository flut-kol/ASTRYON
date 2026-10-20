# Build context: repository root.  docker build -f services/query-api/Dockerfile -t astryon/query-api .
FROM golang:1.22 AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOWORK=off
COPY libs/go-common libs/go-common
COPY services/query-api services/query-api
WORKDIR /src/services/query-api
RUN go build -trimpath -ldflags="-s -w" -o /out/query-api ./cmd/query-api

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/query-api /query-api
USER nonroot:nonroot
EXPOSE 8082
ENTRYPOINT ["/query-api"]
