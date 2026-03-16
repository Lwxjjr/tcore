# GEMINI.md - Project Context

## Project Overview
**tcore** is a Go-based time-series database engine, likely inspired by `tstorage`. It implements a time-partitioned storage architecture where data is organized into "chunks". These chunks transition from a "mutable" (writable) state to an "immutable" (read-only) state.

### Key Components:
- **`chunk` Interface**: Defines the core operations for a time-series partition, including insertion, querying (by metric and labels), and lifecycle management.
- **`mutableChunk`**: The active, writable partition for hot data.
- **`immutableChunk`**: Read-only partitions for cold data.
- **`chunkList`**: A linked-list-like structure that manages the sequence of chunks, allowing for efficient insertion of new chunks and rotation of old ones.
- **`Row`, `DataPoint`, `Label`**: Core data structures representing time-series data points and their associated metadata.

### Main Technologies:
- **Language**: Go 1.23+
- **Concurrency**: Extensive use of `sync.RWMutex`, `sync.Map`, and `sync/atomic` for thread-safe operations.

## Building and Running
As a library-style project, it does not have a single main entry point, but it follows standard Go conventions.

### Key Commands:
- **Build**: `go build ./...`
- **Test**: `go test ./...`
- **Lint**: `go vet ./...` (TODO: check for specific linting tools like `golangci-lint`)

## Development Conventions
- **Naming**: Uses internal/external naming conventions (e.g., `mutableChunk` is private, `chunk` interface might be exported if it were capitalized, but here it's lowercase `chunk` indicating it's package-private or exported via other means).
- **Concurrency**: Thread safety is a priority. Implementation of `chunk` methods must be goroutine-safe.
- **Data Layout**: Data is partitioned by time. `marshalMetricName` is a placeholder for generating unique identifiers for metrics based on their labels.
- **Lifecycle**: Chunks follow a lifecycle: Active (Writable) -> Immutable (Read-only) -> Expired (Deleted).

## Architecture Details
The project implements a `chunkList` which maintains a head (newest/mutable) and a tail (oldest). It supports swapping chunks (likely for compaction or conversion from mutable to immutable).
