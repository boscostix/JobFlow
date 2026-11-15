.PHONY: fmt build test race vet integration generate
fmt:
	gofmt -w cmd internal tests
build:
	go build ./...
test:
	go test ./...
race:
	go test -race ./...
vet:
	go vet ./...
integration:
	go test -tags=integration -count=1 ./tests/integration
generate:
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.30.0 generate
