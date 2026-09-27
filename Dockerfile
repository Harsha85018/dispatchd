# Build both binaries in a full Go image, then copy them into a minimal
# runtime image. The final image carries no compiler or source, just the
# static binaries.
FROM golang:1.27-alpine AS build

WORKDIR /src

# Dependencies first, so this layer caches unless go.mod changes.
COPY go.mod ./
RUN go mod download

COPY . .

# CGO off gives fully static binaries that run in a scratch-like image.
RUN CGO_ENABLED=0 go build -o /out/server ./cmd/server && \
    CGO_ENABLED=0 go build -o /out/worker ./cmd/worker && \
    CGO_ENABLED=0 go build -o /out/loadtest ./cmd/loadtest

FROM alpine:3.20

# wget is used by the compose healthcheck.
RUN apk add --no-cache wget

WORKDIR /app
COPY --from=build /out/server /app/server
COPY --from=build /out/worker /app/worker
COPY --from=build /out/loadtest /app/loadtest

# The WAL lives here; compose mounts a volume over it so job state
# survives container restarts.
RUN mkdir -p /data
WORKDIR /data

EXPOSE 8080
CMD ["/app/server"]