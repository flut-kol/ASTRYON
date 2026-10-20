# Build context: repository root.  docker build -f services/export-worker/Dockerfile -t astryon/export-worker .
FROM golang:1.22 AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOWORK=off
COPY libs/go-common libs/go-common
COPY services/export-worker services/export-worker
WORKDIR /src/services/export-worker
RUN go build -trimpath -ldflags="-s -w" -o /out/export-worker ./cmd/export-worker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/export-worker /export-worker
USER nonroot:nonroot
EXPOSE 8083
ENTRYPOINT ["/export-worker"]
