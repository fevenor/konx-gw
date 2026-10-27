# konx-gw image (single "full" flavor: Go binary + ffmpeg)
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/konx-gw ./cmd/konx-gw

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/konx-gw /usr/local/bin/konx-gw
VOLUME /data
EXPOSE 8080 8554 8000
ENTRYPOINT ["/usr/local/bin/konx-gw"]
CMD ["-config", "/etc/konx-gw/config.yaml"]
