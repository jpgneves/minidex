# Minidex - a filesystem index library

![Crates.io License](https://img.shields.io/crates/l/minidex) ![docs.rs](https://img.shields.io/docsrs/minidex) ![Crates.io Version](https://img.shields.io/crates/v/minidex)

Minidex is a fast, lightweight index for filesystem entries meant to be
embedded in desktop applications.

## Overview

Minidex implements a Log-Structured Merge-tree with Finite
State Transducers and an Inverted Index to efficiently store and serve
filesystem data (file paths, names and metadata) and provide instant
multi-word search with a small disk footprint.

### Features

* **Fully memory mapped** - no data is loaded eagerly
* **Write-Ahead Log backed ingestion** - Real-time inserts and deletes buffered in an in-memory data structure backed by a WAL for persistence in face of crashes
* **O(1) tree pruning** - Prefix tombstones instantly delete indexed data for whole path prefixes
* **Background compaction** - independent thread managing segment merging using a zero allocation K-Way Merge
* **Hybrid scoring** - Hardware accelerated metadata pre-ranking and filtering combined with a TF-IDF scoring step
* **Pagination** - Support for offset and limit pagination

Minidex offers support for offset+limit pagination, a Write-Ahead Log
for real-time insertion and background compaction of segments.

## Note
Minidex does _not_ implement the filesystem indexing process itself
(see `examples/fs-index`) on how to implement a basic indexer.
