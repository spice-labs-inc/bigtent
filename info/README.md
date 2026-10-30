# Info Directory

This directory contains technical documentation for Big Tent.

## Documentation in This Directory

* [Files and Formats](files_and_formats.md) - Detailed file format specifications (.grc, .gri, .grd)
  ([LLM companion](llm/files_and_formats_llm.md))
* [Goat Rodeo Producer Upgrade Guide](goat_rodeo_upgrade.md) - What changes when writing version 4 clusters
  ([LLM companion](llm/goat_rodeo_upgrade_llm.md))
* [Configuration Reference](config.md) - CLI arguments, environment variables, and tuning
  ([LLM companion](llm/config_llm.md))
* [Operations Guide](operations.md) - Production deployment, TLS, monitoring, upgrades, and runbook
  ([LLM companion](llm/operations_llm.md))

### Queries

* [Writing a Query](writing_queries.md) - the stages, the edges, `tee`, and the functions a program can call
  ([LLM companion](writing_queries_llm.md))
* [Barker](barker.md) - Starlark in the pipeline: the phases, streaming, natives, boundedness
  ([LLM companion](llm/barker_llm.md))

### Sansho (the JMESPath engine)

* [Getting Started](sansho/getting_started.md) - the pieces, a first projection, the cache, in-memory documents
  ([LLM companion](sansho/getting_started_llm.md))
* [Source Model](sansho/source_model.md) - the trait surface, the three sources, navigation and validation
  ([LLM companion](sansho/source_model_llm.md))
* [Architecture](sansho/architecture.md) - the layers, the limits, the CBOR-to-JSON mapping
  ([LLM companion](sansho/architecture_llm.md))
* [Consumer Guide](sansho/consumer_guide.md) - the entry points and the backing contract
  ([LLM companion](sansho/consumer_guide_llm.md))
* [Benchmarks](sansho/benchmarks.md) - the three-pipeline comparison, the gates, the protocol
  ([LLM companion](sansho/benchmarks_llm.md))
* [Troubleshooting](sansho/troubleshooting.md) - the common failures and what they mean
  ([LLM companion](sansho/troubleshooting_llm.md))

## Top-Level Documentation

See also the documentation in the repository root:

* [README.md](../README.md) - Project overview and quick start
* [GETTING_STARTED.md](../GETTING_STARTED.md) - Installation and first steps
* [ARCHITECTURE.md](../ARCHITECTURE.md) - System design and component overview
* [PERFORMANCE.md](../PERFORMANCE.md) - Performance tuning guide

## API Documentation

The REST API is documented via OpenAPI 3.1. When running the server:

```bash
curl http://localhost:3000/openapi.json
```

The specification is auto-generated from code using [utoipa](https://github.com/juhaku/utoipa).
