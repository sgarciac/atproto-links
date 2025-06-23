# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview
Constellation is a Rust service that processes AT Protocol events from Bluesky's Jetstream to extract and track links from social media records. It connects to WebSocket streams, processes real-time events, and stores cursor state in PostgreSQL.

## Commands
- Build: `cargo build`
- Test: `cargo test`
- Lint: `cargo clippy`
- Format: `cargo fmt`
- Database migrations: `diesel migration run`
- Database setup: `diesel setup` (requires DATABASE_URL environment variable)

## Architecture Overview

### Core Components
- **`src/consumer/`**: Event processing logic - handles Jetstream WebSocket connections and event parsing
- **`src/storage/`**: PostgreSQL persistence layer using Diesel ORM with async connection pooling
- **`src/models.rs`**: Database models, primarily `BigintKeyval` for cursor storage
- **`src/schema.rs`**: Auto-generated Diesel schema (do not edit manually)

### Data Flow
1. **Jetstream Connection**: WebSocket client (`jetstream.rs`) connects to Bluesky's event stream
2. **Event Processing**: Consumer parses JSON events into `ActionableEvent` enum types
3. **Link Extraction**: Uses external `../links/` crate to extract URLs from AT Protocol records
4. **State Persistence**: Stores processing cursor in PostgreSQL to enable resumption after restarts

### Key Dependencies
- **Database**: `diesel-async` with `bb8` connection pooling (15 max connections)
- **WebSocket**: `tungstenite` for Jetstream connectivity
- **Compression**: `zstd` with custom dictionary for Jetstream data decompression
- **Async Runtime**: Full `tokio` with channels for actor-pattern event processing

## Database Schema
The primary table is `bigint_keyvals` used for storing Jetstream cursor positions:
```sql
CREATE TABLE bigint_keyvals (
    name VARCHAR(255) PRIMARY KEY,
    bivalue BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

## Environment Requirements
- `DATABASE_URL`: PostgreSQL connection string (required)
- Jetstream endpoint configuration in code defaults to Bluesky's official stream

## Common Development Tasks
- **Adding new event types**: Extend `ActionableEvent` enum in `lib.rs` and update consumer logic
- **Database changes**: Create new Diesel migration with `diesel migration generate <name>`
- **Testing WebSocket logic**: Consumer uses channels, so unit tests can mock event streams
- **Debugging compression**: Jetstream uses zstd with custom dictionary in `/zstd/dictionary`

## Error Handling Patterns
- Uses `anyhow` for error propagation throughout the application
- WebSocket connections implement retry logic with exponential backoff
- Database operations use connection pooling to handle concurrent access
- Event processing errors are logged but don't crash the service