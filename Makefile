.PHONY: check test fmt clippy
all: check

test:
	cargo test

fmt:
	cargo fmt --package links --package constellation --package ufos
	cargo +nightly fmt --package jetstream

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

check: test fmt clippy
