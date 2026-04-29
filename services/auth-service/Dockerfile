FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/auth-service ./cmd

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /bin/auth-service /bin/auth-service
COPY migrations ./migrations
EXPOSE 8001
CMD ["/bin/auth-service"]
