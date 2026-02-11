//! # BigTent - An Opinionated Graph Database for Software Artifacts
//!
//! BigTent is a high-performance graph database designed to serve millions of GitOIDs
//! (Git Object Identifiers) representing software artifacts and their relationships.
//!
//! ## Overview
//!
//! The database stores software artifacts as **Items** connected by typed **Edges**.
//! Items are identified by GitOIDs (content-addressable hashes) and can represent
//! source files, packages, builds, or any software artifact.
//!
//! ## Architecture
//!
//! BigTent is organized into several key modules:
//!
//! - **[`rodeo`]** - The core graph database implementation ("Goat Rodeo")
//!   - [`rodeo::goat`] - File-backed cluster implementation
//!   - [`rodeo::goat_herd`] - Multi-cluster aggregation
//!   - [`rodeo::goat_trait`] - Core trait defining graph operations
//!   - [`rodeo::robo_goat`] - In-memory synthetic clusters
//!
//! - **[`item`]** - Core data model (Item, Edge types, metadata)
//! - **[`server`]** - REST API layer (Axum-based HTTP server)
//! - **[`fresh_merge`]** - Cluster merging functionality
//! - **[`config`]** - CLI configuration and arguments
//! - **[`util`]** - Shared utilities (hashing, I/O, serialization)
//!
//! ## Usage Modes
//!
//! BigTent operates in three modes:
//!
//! 1. **Rodeo Mode** (`--rodeo`): Load and serve existing cluster files via HTTP
//! 2. **Fresh Merge Mode** (`--fresh-merge`): Merge multiple clusters into one
//! 3. **Lookup Mode** (`--rodeo` + `--lookup`): Batch identifier lookup from a JSON file,
//!    outputting results to stdout or a file without starting a server
//!
//! ## Example
//!
//! ```rust,no_run
//! use bigtent::ApiDoc;
//! use utoipa::OpenApi;
//!
//! // Access the OpenAPI specification
//! let spec = ApiDoc::openapi();
//! println!("API version: {}", spec.info.version);
//! ```
//!
//! ## File Formats
//!
//! BigTent uses three file types (see `info/files_and_formats.md`):
//! - `.grc` - Cluster files (metadata and file references)
//! - `.gri` - Index files (GitOID → data offset mapping)
//! - `.grd` - Data files (CBOR-encoded Items)

pub mod config;
pub mod fresh_merge;
pub mod server;
pub mod rodeo {
    pub mod cluster;
    pub mod data;
    pub mod goat;
    pub mod goat_herd;
    pub mod goat_trait;
    pub mod holder;
    pub mod index;
    pub mod member;
    pub mod robo_goat;
    pub mod writer;
}
pub mod item;
pub mod util;
pub extern crate arc_swap;
pub extern crate axum;
pub extern crate serde_cbor;
pub extern crate tokio;
pub extern crate tokio_util;
pub extern crate tower_http;
pub extern crate utoipa;

// Re-export key types for convenience
pub use server::ApiDoc;
