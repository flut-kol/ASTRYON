FROM --platform=linux/amd64 golang:1.22 AS go_builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X main.BuildTime=$(date -u +%Y%m%d)" \
    -o /bin/scan ./cmd/go-defender

FROM ubuntu:focal AS lib_builder
RUN set -x \
 && dpkg --add-architecture i386 && apt-get update -qq \
 && apt-get install -o APT::Immediate-Configure=false -y --no-install-recommends \
      git cabextract build-essential gcc-multilib \
      libreadline-dev:i386 libc6-dev:i386 ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/loadlibrary
RUN git clone https://github.com/taviso/loadlibrary.git . \
 && curl -L -o engine/mpam-fe.exe \
      "http://download.microsoft.com/download/DefinitionUpdates/mpam-fe.exe" \
 && cd engine && cabextract mpam-fe.exe && rm mpam-fe.exe \
 && cd .. && make -j$(nproc)

FROM ubuntu:focal
RUN groupadd -r scanner \
 && useradd -r -g scanner scanner \
 && mkdir -p /shared /opt/scan /opt/loadlibrary \
 && chown -R scanner:scanner /shared /opt/scan /opt/loadlibrary

RUN dpkg --add-architecture i386 \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
      libc6-i386 ca-certificates cabextract \
 && rm -rf /var/lib/apt/lists/*

COPY --from=go_builder /bin/scan      /usr/local/bin/scan
COPY --from=lib_builder /opt/loadlibrary /loadlibrary
RUN chmod -R a+rX /loadlibrary && if [ -f /loadlibrary/mpclient ]; then chmod 0755 /loadlibrary/mpclient; fi

USER scanner
WORKDIR /shared
VOLUME ["/shared"]

ENTRYPOINT ["scan"]
