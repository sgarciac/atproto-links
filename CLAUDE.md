# Claude Configuration

## Project Overview
This is an AT Protocol links extraction and tracking project. It processes and stores links from AT Protocol records.

## Commands
- Build: `cargo build`
- Test: `cargo test`
- Lint: `cargo clippy`
- Format: `cargo fmt`

## Database Integration
PostgreSQL integration options:
- diesel: a rust ORM

## Project Structure
- `links/`: Core data types and structures
- `constellation/`: Service implementation
- `jetstream/`: Event streaming

## Development Workflow
1. Make changes to Rust files
2. Run `cargo clippy` to check for errors
3. Run `cargo test` to verify functionality
4. Run `cargo fmt` to ensure consistent formatting

## Best Practices
- Follow Rust idioms and naming conventions
- Document the code properly
- Use async/await for database operations
- Implement proper error handling with custom error types
- Add unit tests for all new functionality
