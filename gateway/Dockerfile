FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/gateway ./cmd

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /bin/gateway /bin/gateway
COPY docs ./docs
EXPOSE 8000
CMD ["/bin/gateway"]
