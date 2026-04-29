FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/analytics-service ./cmd

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /bin/analytics-service /bin/analytics-service
COPY migrations ./migrations
EXPOSE 8004
CMD ["/bin/analytics-service"]
