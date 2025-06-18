# GitHub Actions Workflows

This folder contains GitHub Actions workflows.

- `rust-ci.yml` uses Cargo to build and test the Rust project  
  - This workflow automatically triggers on:
    1. Push to any branch (`**`)
    2. Pull requests targeting the `main` branch

- `rust_container_publishing.yml` publishes Docker images to [Docker Hub](https://hub.docker.com/u/spicelabs) as `spicelabs/bigtent`  
  - The image includes:
    - Provenance attestations
    - Software Bill of Materials (SBOM)
    - Multi-format semver tags (e.g. `v1.2.3`, `v1.2`, `v1`)
  - This workflow triggers automatically on:
    1. Push of a semantic version tag (e.g. `v1.2.3`)
    2. Manual invocation via the GitHub Actions UI

