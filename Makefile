STANDARDS_DIR ?= $(wildcard $(CURDIR)/../go-standards)
include standard.mk

# Repository-specific gates appended to `make check`.
CHECK_EXTRA := readme modverify cross fuzz bench example consumer

# Fixed execution budgets avoid the fuzz deadline race (golang/go#75804).
# Every real fuzz failure remains fatal; no timeout result is ignored.
FUZZ_TARGETS := FuzzMatchChunks FuzzLineCursor FuzzLineRegexParity FuzzRedactorChunks \
	FuzzSessionRedactorAdditiveRules FuzzPublicRedactorChunks FuzzLiteralAllChunks \
	FuzzSessionContinuations FuzzInteractInputPrefixes FuzzInteractInputPatterns FuzzRegexpResume
FUZZ_EXECUTIONS ?= 100000x

.PHONY: readme modverify cross fuzz bench example consumer

# The README example block must equal example_test.go.
readme:
	python3 script/readme.py

modverify:
	$(GO) mod verify

# Windows tests must compile and FreeBSD must vet.
cross:
	GOOS=windows GOARCH=amd64 $(GO) test -run '^$$' -exec true ./...
	GOOS=freebsd GOARCH=amd64 $(GO) vet ./...

fuzz:
	@for target in $(FUZZ_TARGETS); do \
		echo "fuzz $$target"; \
		$(GO) test -run '^$$' -fuzz "^$$target$$" -fuzztime=$(FUZZ_EXECUTIONS) -parallel=2 . || exit 1; \
	done

bench:
	$(GO) test -run '^$$' -bench . -benchmem -benchtime=3x .

example:
	$(GO) run ./examples/dialogue

# Compile a consumer against a snapshot of this module.
consumer:
	bash script/consumer
