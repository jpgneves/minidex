# Minidex - a filesystem index library

![Crates.io License](https://img.shields.io/crates/l/minidex) ![docs.rs](https://img.shields.io/docsrs/minidex) ![Crates.io Version](https://img.shields.io/crates/v/minidex)

Minidex is a fast, lightweight index for filesystem entries meant to be
embedded in desktop applications.

## Overview

Minidex implements a Log-Structured Merge-tree with Finite
State Transducers and an Inverted Index to efficiently store and serve
filesystem data (file paths, names and metadata) and provide instant
multi-word search with a minimal disk footprint.

### Features

* **Fully memory mapped** - no data is loaded eagerly
* **Zstd Dictionary Compression** - efficient storage of path and metadata records using Zstd with per-segment trained dictionaries, typically reducing data file size by ~40%
* **Unicode-aware Tokenization** - specialized tokenizer with NFD normalization, case folding, and support for CJK fragmentation and camelCase splitting
* **SIMD-accelerated Search** - hardware-accelerated intersection of posting lists for high-performance multi-term queries
* **Write-Ahead Log backed ingestion** - Real-time inserts and deletes buffered in an in-memory data structure backed by a WAL for persistence in face of crashes
* **Volume Management** - built-in support for filtering by volume names and types (Local, Network, Removable, etc.)
* **Fast category filtering** - user-provided file categories allow quickly filtering the index for documents, images, text, etc.
* **O(1) tree pruning** - Prefix tombstones instantly delete indexed data for whole path prefixes
* **Background compaction** - independent thread managing segment merging using a zero allocation K-Way Merge with dictionary reuse across tiers
* **Hybrid scoring** - Hardware accelerated metadata pre-ranking and filtering combined with a TF-IDF scoring step
* **Pagination** - Support for offset and limit pagination

Minidex offers support for offset+limit pagination, a Write-Ahead Log
for real-time insertion and background compaction of segments.

## Note
Minidex does _not_ implement the filesystem indexing process itself
(see `examples/fs-index`) on how to implement a basic indexer.
