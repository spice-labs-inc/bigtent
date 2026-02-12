# Info Directory

This directory contains technical documentation for Big Tent.

## Documentation in This Directory

* [Files and Formats](files_and_formats.md) - Detailed file format specifications (.grc, .gri, .grd)
* [Configuration Reference](config.md) - CLI arguments, environment variables, and tuning
* [Operations Guide](operations.md) - Production deployment, TLS, monitoring, upgrades, and runbook

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
