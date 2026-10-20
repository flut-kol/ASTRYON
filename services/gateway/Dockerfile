# Build context = repo root:  docker build -f services/gateway/Dockerfile -t astryon/gateway .
FROM golang:1.22 AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath GOWORK=off
COPY libs ./libs
COPY services/gateway ./services/gateway
WORKDIR /src/services/gateway
RUN go mod download
RUN go build -ldflags="-s -w" -o /out/gateway ./cmd/gateway

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/gateway /gateway
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/gateway"]
