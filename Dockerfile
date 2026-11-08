# One image holding every binary; docker-compose / ECS pick one via `command`.
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/gateway ./cmd/backend ./cmd/loadgen

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/gateway /out/backend /out/loadgen /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/gateway"]
