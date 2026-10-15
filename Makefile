.PHONY: setup up down test check build integration demo incident recovery
setup:
	go mod download
up:
	docker compose up --build -d --wait
down:
	docker compose down
test:
	go test -race ./...
check:
	go vet ./...
	test -z "$$(gofmt -l $$(find . -name '*.go'))"
build:
	go build ./...
integration:
	DATABASE_URL=postgres://ledgerflow:local_demo_only@localhost:55433/ledgerflow?sslmode=disable go test -race -tags=integration -count=1 ./...
demo:
	python3 scripts/demo.py
incident:
	DATABASE_URL=postgres://ledgerflow:local_demo_only@localhost:55433/ledgerflow?sslmode=disable python3 scripts/incident.py $(ID)
recovery:
	python3 scripts/recovery.py
