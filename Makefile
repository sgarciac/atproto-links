.PHONY: check test fmt clippy
all: check

test:
	cargo test

fmt:
	cargo fmt --all

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

check: test fmt clippy
