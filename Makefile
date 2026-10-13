.PHONY: build test race crash bench data clean

build:
	go build -o bin/ ./cmd/...

test:
	go test ./...

race:
	go test -race ./...

# kill -9 a real writer at random points and verify recovery (20 runs)
crash:
	GHSTORE_CRASH_ITERS=20 go test -race -run TestKill9Recovery -v ./internal/store

# download 24 hours of real GH Archive data into ./gh
data:
	./scripts/download.sh 2024-01-01 ./gh

# ingest ./gh into ./data, then run 1,000 random queries
bench: build
	rm -rf ./data
	./bin/ghstore ingest -data ./data ./gh/*.json.gz
	./bin/ghstore stats -data ./data
	./bin/ghbench -data ./data -queries 1000

clean:
	rm -rf bin data
