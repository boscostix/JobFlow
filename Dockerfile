FROM golang:1.24.9-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/api ./cmd/api && CGO_ENABLED=0 go build -o /out/worker ./cmd/worker

FROM alpine:3.22
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 jobflow
COPY --from=build /out/api /out/worker /usr/local/bin/
USER jobflow
CMD ["api"]
