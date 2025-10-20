FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o server ./cmd/demo.go

# ___________RUNTIME STAGE_____________

FROM alpine:latest

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /app/server /app/server

EXPOSE 4430

ENV SYNERGYNET_PORT=4430

CMD ["/app/server"]