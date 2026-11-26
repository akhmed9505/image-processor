FROM golang:1.27.4-bookworm@sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea AS build
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/
RUN go build -mod=readonly -trimpath -buildvcs=false -ldflags='-s -w -buildid=' -o /out/server ./cmd/server \
    && go build -mod=readonly -trimpath -buildvcs=false -ldflags='-s -w -buildid=' -o /out/worker ./cmd/worker

FROM busybox:1.37.0-musl@sha256:f6a95d611e0a8780fabdb2aec43e31275b2720dfefa3e41b16835bfc42454478
WORKDIR /app
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/server /out/worker /usr/local/bin/
COPY configs/config.yml configs/config.yml
COPY migrations/*.sql migrations/
USER 65532:65532
EXPOSE 8080
STOPSIGNAL SIGTERM
CMD ["server"]
