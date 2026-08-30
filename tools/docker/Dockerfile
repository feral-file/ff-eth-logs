# ff-eth-logs image: pure-Go build, no CGO.
#
# Stages:
#   1. builder — download modules, compile the single binary.
#   2. runtime — alpine + CA certificates, unprivileged user, config mounted at /app/config.yaml,
#      the SQL schema under /app/db for the deployment to apply.

# syntax=docker/dockerfile:1.4

FROM golang:1.25-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -ldflags="-w -s" -o /build/bin/ff-eth-logs ./cmd/ff-eth-logs

FROM alpine:3.21

RUN apk add --no-cache ca-certificates wget

RUN addgroup -g 1000 appuser && \
    adduser -D -u 1000 -G appuser appuser

WORKDIR /app

COPY --from=builder /build/bin/ff-eth-logs .
# The schema ships with the binary so a deployment applies the exact schema
# the pinned image was built against (ff-deploy extracts /app/db/init_pg_db.sql
# from the image and runs it with psql; every statement is IF NOT EXISTS).
COPY --from=builder /build/db ./db

RUN chown -R appuser:appuser /app

USER appuser

EXPOSE 8545

ENTRYPOINT ["./ff-eth-logs"]
CMD ["serve", "-config", "config.yaml"]
