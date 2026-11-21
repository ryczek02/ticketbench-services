# syntax=docker/dockerfile:1.7
# One Dockerfile for every service: docker build --build-arg SERVICE=store-postgres .
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG TARGETOS
ARG TARGETARCH
ARG SERVICE
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    test -n "$SERVICE" && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
      -ldflags "-s -w \
        -X github.com/ryczek02/ticketbench-contracts/pkg/version.Version=$VERSION \
        -X github.com/ryczek02/ticketbench-contracts/pkg/version.Commit=$COMMIT \
        -X github.com/ryczek02/ticketbench-contracts/pkg/version.Date=$DATE" \
      -o /out/app ./cmd/$SERVICE

FROM gcr.io/distroless/static-debian12:nonroot
ARG SERVICE
LABEL org.opencontainers.image.source="https://github.com/ryczek02/ticketbench-services" \
      org.opencontainers.image.description="ticketbench $SERVICE" \
      org.opencontainers.image.licenses="MIT"
COPY --from=build /out/app /app
USER nonroot:nonroot
HEALTHCHECK --interval=5s --timeout=4s --start-period=10s --retries=30 CMD ["/app", "healthcheck"]
ENTRYPOINT ["/app"]
