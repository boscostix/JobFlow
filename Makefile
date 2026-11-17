.PHONY: fmt build test race vet integration generate
GO_PACKAGES := ./cmd/... ./internal/...
fmt:
	gofmt -w cmd internal tests
build:
	go build $(GO_PACKAGES)
test:
	go test $(GO_PACKAGES)
race:
	go test -race $(GO_PACKAGES)
vet:
	go vet $(GO_PACKAGES)
integration:
	go test -tags=integration -count=1 ./tests/integration
generate:
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.30.0 generate
