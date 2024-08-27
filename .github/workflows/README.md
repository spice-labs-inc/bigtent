# Github Actions Workflows

This folder contains Github Actions workflows

- `rust-ci.yml` uses cargo to run a build & test job 
  - This automatically triggers on the following events:
    1. Push to any branch (`**`)
    2. Pull requests to `main` branch
- `rust_container_publishing.yml` publishes container images to docker hub and ghcr (github container repo) with attestations
  - This will automatically trigger on the following events:
    1. Push to the 'main' branch (which should only happen with a merge)
    2. Setting of tags on `main` with semver tags 
    3. The Rust Build & Test (`rust-ci.yml`) job runs successfully
