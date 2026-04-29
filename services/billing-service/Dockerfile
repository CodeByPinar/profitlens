FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/billing-service ./cmd

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /bin/billing-service /bin/billing-service
COPY migrations ./migrations
EXPOSE 8003
CMD ["/bin/billing-service"]
