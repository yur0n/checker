# Builder stage
FROM golang:alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o app .

# Final stage
FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/app .
CMD ["./app"]