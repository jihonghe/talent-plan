.PHONY: all

all: test bench cover

test:
	go test

bench:
	go test -bench Benchmark -run xx -count 5 -benchmem

cover:
	go test -cover
