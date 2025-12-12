# grex

A high-performance, multithreaded implementation of the popular ripgrep search tool, written in Rust. This project demonstrates a lightweight alternative to traditional text search utilities with concurrent processing capabilities.

## Features

- **Multithreaded Architecture**: Leverages multiple CPU cores for faster file searching
- **SIMD Acceleration**: Uses SIMD instructions for optimized pattern matching
- **Memory Efficient**: Implements buffered reading and overlap management to handle large files efficiently
- **Real-time Output**: Provides immediate search results as they are found
- **Colored Output**: Highlights matched patterns with syntax highlighting

## Architecture

The project is structured into four main components:

1. **DirWalker.rs**: Manages directory traversal and file discovery using concurrent directory scanning
2. **SearchWorker.rs**: Handles the core pattern matching logic using SIMD operations for performance
3. **PrintWorker.rs**: Manages the output formatting and presentation of search results
4. **main.rs**: Orchestrates the overall workflow with inter-thread communication

## Dependencies

- `wide` (v0.7): SIMD-accelerated operations for efficient pattern matching
- `num_cpus` (1.16): CPU detection for optimal thread count configuration

## How It Works

The implementation follows a producer-consumer pattern with three main threads:
1. **Directory Walker**: Discovers files in the directory tree concurrently
2. **Search Workers**: Multiple worker threads that scan files for the specified pattern
3. **Printer Thread**: Formats and displays matches in real-time

The system dynamically adjusts the number of worker threads based on the number of available CPU cores (typically set to 2x the number of CPUs).

## Usage

```bash
cargo run "search_pattern"
```

The program will search for the specified pattern in all files within the current directory and its subdirectories. Results are displayed with:
- File name
- Line number
- Full line containing the match
- Highlighted pattern in red

## Performance Notes

- The implementation uses SIMD instructions to scan 32 bytes at a time
- Optimized buffering reduces I/O overhead
- Overlap buffers ensure patterns spanning buffer boundaries are detected
- Multithreaded design maximizes CPU utilization

## Building From Source

```bash
# Clone the repository
git clone <repository-url>

# Build the project
cd grex
cargo build --release

# Run the executable
./target/release/grex "pattern"
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
