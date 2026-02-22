# Performance Tuning

This guide covers performance tuning for Lance Trino operations in large-scale analytics scenarios.

## Understanding Lance's Default Optimization

Lance is **optimized by default for random access patterns** - fast point lookups, vector searches, and selective column reads.
These defaults work well for ML/AI workloads where you frequently access individual records or small batches.

For **large-scale batch ETL and scan-heavy OLAP operations** (writing millions of rows, full table scans, bulk exports),
you can tune Lance's environment variables and connector properties to better utilize available resources.

## Caching

Lance Trino uses a multi-level caching strategy to minimize redundant I/O and improve query performance.

### How Caching Works

Lance Trino implements two levels of caching:

1. **Session Cache** - Contains index and metadata caches:

    - **Index Cache**: Caches opened vector indices, fragment reuse indices, and index metadata
    - **Metadata Cache**: Caches manifests, transactions, deletion files, row ID indices, and file metadata

2. **Dataset Cache** - Caches opened datasets by `(userIdentity, tablePath, version)` key. Since a dataset at a specific version is immutable, this ensures:

    - Each dataset is opened only once per worker
    - All workers read the same version for snapshot isolation
    - Schema and fragment metadata are reused from the cached dataset

### Cache Configuration

Configure caching behavior via connector properties in your catalog file:

```properties
# Session cache settings
lance.cache.session.max_entries=100                       # Maximum cached sessions (default: 100)
lance.cache.session.ttl_minutes=60                        # Session cache TTL in minutes (default: 60)
lance.cache.session.index_cache_size_bytes=6442450944     # Index cache size: 6GB
lance.cache.session.metadata_cache_size_bytes=1073741824  # Metadata cache size: 1GB

# Dataset cache settings
lance.cache.dataset.max_entries=100          # Maximum cached datasets (default: 100)
lance.cache.dataset.ttl_minutes=30           # Dataset cache TTL in minutes (default: 30)
```

| Property | Description | Default |
|----------|-------------|---------|
| `lance.cache.session.max_entries` | Maximum number of cached sessions | `100` |
| `lance.cache.session.ttl_minutes` | Session cache TTL in minutes | `60` |
| `lance.cache.session.index_cache_size_bytes` | Index cache size in bytes | Lance default (6GB) |
| `lance.cache.session.metadata_cache_size_bytes` | Metadata cache size in bytes | Lance default (1GB) |
| `lance.cache.dataset.max_entries` | Maximum number of cached datasets | `100` |
| `lance.cache.dataset.ttl_minutes` | Dataset cache TTL in minutes | `30` |

The index cache stores vector indices which can be large but provide significant speedup for vector search queries.
Increase this if you frequently query tables with vector indices.

The metadata cache stores manifests, file metadata, and other dataset metadata.
Each column's metadata can be around 40MB, so increase this if your tables have many columns.

## Lance Environment Variables

Lance uses environment variables for low-level I/O tuning. Set these on your Trino coordinator and worker nodes.

### Read Performance

#### I/O Threads

Set via environment variable `LANCE_IO_THREADS` (default: 64).

Controls the number of I/O threads used for parallel reads from storage.
For large scans, increasing this to match your CPU core count enables more concurrent S3 requests.

```bash
export LANCE_IO_THREADS=128
```

### Write Performance

#### Upload Concurrency

Set via environment variable `LANCE_UPLOAD_CONCURRENCY` (default: 10).

Controls the number of concurrent multipart upload streams to S3.
Increasing this to match your CPU core count can improve throughput.

```bash
export LANCE_UPLOAD_CONCURRENCY=32
```

#### Upload Part Size

Set via environment variable `LANCE_INITIAL_UPLOAD_SIZE` (default: 5MB).

Controls the initial part size for S3 multipart uploads.
Larger part sizes reduce the number of API calls and can improve throughput for large writes.
However, larger part sizes use more memory and may increase latency for small writes.
Use the default for interactive workloads.

!!!note
    Lance automatically increments the multipart upload size by 5MB every 100 uploads,
    so large file writes progressively use increasingly large upload parts.
    There is no configuration for a fixed upload size.

```bash
export LANCE_INITIAL_UPLOAD_SIZE=33554432  # 32MB
```

### Environment Variables Summary

| Variable | Description | Default |
|----------|-------------|---------|
| `LANCE_IO_THREADS` | Number of I/O threads for parallel reads | `64` |
| `LANCE_UPLOAD_CONCURRENCY` | Number of concurrent S3 upload streams | `10` |
| `LANCE_INITIAL_UPLOAD_SIZE` | Initial S3 multipart upload part size (bytes) | `5242880` (5MB) |

## Index-Aware Split Planning

Lance Trino optimizes split planning based on index availability. When a table has indexes on filtered columns, larger splits are used because index lookups are efficient.

```properties
# Rows per split when btree index is used (default: 100M)
lance.index.btree.rows_per_split=100000000

# Rows per split when bitmap index is used (default: 10M)
lance.index.bitmap.rows_per_split=10000000
```

| Property | Description | Default |
|----------|-------------|---------|
| `lance.index.btree.rows_per_split` | Row count threshold for btree-indexed splits | `100000000` (100M) |
| `lance.index.bitmap.rows_per_split` | Row count threshold for bitmap-indexed splits | `10000000` (10M) |

## Read and Write Batch Sizes

Control batch sizes for vectorized operations:

```properties
# Read batch size (rows per batch during scans)
lance.read_batch_size=8192

# Write settings
lance.write_batch_size=10000      # Rows to batch before writing to Arrow
lance.max_rows_per_file=1000000   # Maximum rows per Lance file
lance.max_rows_per_group=100000   # Maximum rows per row group
```

| Property | Description | Default |
|----------|-------------|---------|
| `lance.read_batch_size` | Rows per batch during vectorized reads | `8192` |
| `lance.write_batch_size` | Rows to batch before writing to Arrow | `10000` |
| `lance.max_rows_per_file` | Maximum rows per Lance file | `1000000` |
| `lance.max_rows_per_group` | Maximum rows per row group | `100000` |
