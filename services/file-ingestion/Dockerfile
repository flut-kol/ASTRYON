# Build context: repository root.
#   docker build -f services/file-ingestion/Dockerfile --build-arg CMD=file-api    -t astryon/file-api .
#   docker build -f services/file-ingestion/Dockerfile --build-arg CMD=file-worker -t astryon/file-worker .
FROM golang:1.27 AS build
ARG CMD=file-worker
WORKDIR /src
ENV CGO_ENABLED=0 GOWORK=off
COPY libs/go-common libs/go-common
COPY services/retry-controller services/retry-controller
COPY services/file-ingestion services/file-ingestion
WORKDIR /src/services/file-ingestion
RUN go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
