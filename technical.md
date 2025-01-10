# Technical Decisions Documentation

This document explains the key technical decisions made in the JsonLinkCheck tool.

## Core Architecture Decisions

### 1. Asynchronous Processing
- **Decision**: Use `asyncio` and `aiohttp` for URL checking
- **Why**: 
  - Network I/O is the main bottleneck
  - Async allows checking multiple URLs concurrently
  - More efficient than thread-based HTTP requests
  - Better resource utilization

### 2. Chunked Processing
- **Decision**: Process JSON-Lines file in chunks
- **Why**:
  - Memory efficiency for large files
  - Allows parallel processing
  - Better error isolation
  - Progress tracking per chunk
- **Implementation**: 
  - Async generator for lazy loading
  - Configurable chunk size
  - No pre-loading of entire file

### 3. Multi-Threading
- **Decision**: Use thread pool for chunk processing
- **Why**:
  - Distributes CPU load across cores
  - Each thread handles its own chunk independently
  - Scalable with `--threads` parameter
- **Implementation**:
  - Thread-local file output
  - Final merge of results
  - Thread-safe statistics collection

### 4. Memory Management
- **Decision**: Streaming approach with minimal memory footprint
- **Why**:
  - Handle large files efficiently
  - Avoid out-of-memory issues
  - Predictable memory usage
- **Implementation**:
  - Async file reading
  - Process chunks on demand
  - Immediate result writing
  - Clean up temporary files

### 5. URL Processing
- **Decision**: Two-step URL validation with retry mechanism
- **Why**:
  - Fast rejection of invalid URLs
  - Detailed status tracking
  - Handle redirects properly
  - Resilient against network issues
- **Implementation**:
  - Syntactic validation first (fast)
  - HTTP check second (slow)
  - Support for arrays of URLs
  - Configurable timeout handling
  - Exponential backoff for retries
  - Default: Keep timeout URLs
  - Optional: Delete timeout URLs

### 6. Progress Tracking
- **Decision**: Dual progress display system
- **Why**:
  - User feedback for long operations
  - Different detail levels needed
  - Support for multi-threaded view
- **Implementation**:
  - Visual mode with tqdm
  - Logging mode for details
  - Per-thread progress
  - Overall progress tracking

### 7. Data Structures
- **Decision**: Use dataclasses for statistics and error handling
- **Why**:
  - Type safety
  - Clear structure
  - Easy serialization
  - Maintainable code
- **Implementation**:
  - `FieldStats` for per-field stats
  - `Statistics` for overall collection
  - Thread-safe counters
  - Custom error classes
  - Redirect mapping

### 8. Error Handling
- **Decision**: Hierarchical error handling system
- **Why**:
  - Resilient processing
  - Detailed error reporting
  - Continue on partial failures
  - Clear error categorization
- **Implementation**:
  - Base `ProcessingError` class
  - Specialized error types (Network, File, Validation)
  - Per-URL error handling
  - Per-chunk error isolation
  - JSON parsing error recovery
  - Network error handling with retries

### 9. File Handling
- **Decision**: Use temporary directory for thread output
- **Why**:
  - Memory efficient
  - Thread-safe output
  - Recoverable state
  - Clean cleanup
- **Implementation**:
  - Temporary directory with prefix
  - Automatic cleanup in finally block
  - Sequential merge
  - Redirect and timeout logging

### 10. Configuration
- **Decision**: Command-line interface with sensible defaults
- **Why**:
  - Flexible usage
  - Script-friendly
  - Self-documenting
  - Safe defaults
- **Implementation**:
  - argparse for CLI
  - Environment-independent
  - Validated parameters
  - Default: keep timeout URLs
  - Optional redirect tracking

## Dependencies

### Core Dependencies
- **aiohttp**: Async HTTP client
- **aiofiles**: Async file operations
- **tqdm**: Progress bars
- **Python 3.12+**: Modern language features

### Why These Versions
- aiohttp >= 3.9.1: Stable async HTTP support
- tqdm >= 4.66.1: Thread-safe progress bars
- aiofiles >= 23.2.1: Async file operations
- Python >= 3.12: Type hints, async features

## Performance Considerations

### Memory Usage
- Streaming file processing
- Configurable chunk size
- No full file loading
- Garbage collection friendly
- Temporary file cleanup

### CPU Usage
- Async I/O for network
- Thread pool for processing
- Configurable parallelism
- Balanced load distribution

### Network Usage
- Concurrent URL checking
- Configurable timeouts
- Connection pooling
- Error resilience
- Retry mechanism with backoff

## Future Considerations

### Potential Improvements
- Redis for distributed processing
- Persistent statistics storage
- URL validation caching
- Custom HTTP client configuration
- Batch mode for repeated runs
- Support for more URL schemes
- Custom redirect policies 