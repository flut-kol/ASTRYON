FROM golang:1.25-alpine@sha256:1ae0735f00daffa3aaf1363a5184c0d2dc55c78e3db4ec70241cdac97bf84b59 AS build
WORKDIR /src
COPY libs/go-common/ libs/go-common/
COPY services/quarantine/ services/quarantine/
WORKDIR /src/services/quarantine
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/quarantine ./cmd/quarantine

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/quarantine /quarantine
EXPOSE 8085
ENTRYPOINT ["/quarantine"]
