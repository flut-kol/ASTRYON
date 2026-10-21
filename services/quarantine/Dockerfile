FROM golang:1.27-alpine@sha256:f2c872bcee949b7be9c490efd02b48a8a2fbf947cdf533f1c58525ec9d8170fd AS build
WORKDIR /src
COPY libs/go-common/ libs/go-common/
COPY services/quarantine/ services/quarantine/
WORKDIR /src/services/quarantine
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/quarantine ./cmd/quarantine

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/quarantine /quarantine
EXPOSE 8085
ENTRYPOINT ["/quarantine"]
