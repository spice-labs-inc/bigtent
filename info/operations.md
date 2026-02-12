# Big Tent Operations Guide

This guide covers everything an IT administrator needs to run Big Tent in production:
deployment, TLS/authentication, log management, monitoring, upgrades, and troubleshooting.

## Table of Contents

- [Deployment Options](#deployment-options)
- [TLS and Authentication](#tls-and-authentication)
- [Log Management](#log-management)
- [Monitoring and Alerting](#monitoring-and-alerting)
- [Upgrade and Migration](#upgrade-and-migration)
- [Operational Runbook](#operational-runbook)

---

## Deployment Options

### Docker Compose (recommended)

The repository includes a production-ready `docker-compose.yml` with Caddy for automatic HTTPS.

```bash
# 1. Edit examples/Caddyfile — replace bigtent.example.com with your domain
# 2. Place cluster data in the bigtent-data volume (or use a bind mount)
# 3. Start
docker compose up -d

# Check health
curl https://bigtent.example.com/healthz

# View logs
docker compose logs -f bigtent
```

**Key files:**
- [`docker-compose.yml`](../docker-compose.yml) — Service definitions, resource limits, health checks
- [`examples/Caddyfile`](../examples/Caddyfile) — Caddy reverse proxy with auto-HTTPS

To use a host directory instead of a named volume, edit `docker-compose.yml`:

```yaml
volumes:
  - /path/to/your/clusters:/data:ro
```

### Systemd Native Service

For bare-metal or VM deployments without Docker.

```bash
# 1. Install the binary
sudo cp target/release/bigtent /usr/local/bin/

# 2. Create a system user
sudo useradd --system --no-create-home bigtent

# 3. Create data directories
sudo mkdir -p /var/lib/bigtent/clusters
sudo chown bigtent:bigtent /var/lib/bigtent/clusters

# 4. Install the unit file
sudo cp examples/bigtent.service /etc/systemd/system/

# 5. Enable and start
sudo systemctl daemon-reload
sudo systemctl enable --now bigtent

# 6. Check status
sudo systemctl status bigtent
journalctl -u bigtent -f
```

**Key file:** [`examples/bigtent.service`](../examples/bigtent.service)

Cluster reload without downtime:

```bash
sudo systemctl reload bigtent   # sends SIGHUP
```

### Pre-flight Validation

Before starting the server, validate cluster files:

```bash
bigtent --rodeo /path/to/clusters --check
```

This loads and validates all `.grc`, `.gri`, and `.grd` files, then exits with
code 0 (all valid) or 1 (errors found). Use this in CI/CD pipelines or before
a deployment cutover.

---

## TLS and Authentication

### Why No Built-in Auth

Big Tent is a read-only data server. It intentionally does not include built-in
authentication or TLS because:

1. **Defense in depth**: TLS termination and auth belong at the edge (reverse proxy,
   load balancer, or service mesh), not in every backend.
2. **Flexibility**: Organizations use different auth mechanisms (OAuth2, mTLS, LDAP,
   API keys). A reverse proxy lets you choose without recompiling.
3. **Simplicity**: Fewer moving parts in the core server means fewer security bugs.

All Big Tent endpoints are read-only. There are no mutation endpoints — `POST`
endpoints like `/bulk`, `/aa`, `/north`, `/north_purls`, `/flatten`, and
`/flatten_source` accept identifier lists but only perform lookups.

### nginx with TLS + Basic Auth

See [`examples/nginx.conf`](../examples/nginx.conf) for a complete configuration
with TLS termination, basic auth, rate limiting, and security headers.

```bash
# Generate a password file
sudo htpasswd -c /etc/nginx/.htpasswd admin

# Test and reload
sudo nginx -t && sudo systemctl reload nginx
```

### Caddy with Automatic HTTPS

Caddy automatically obtains and renews Let's Encrypt certificates.
See [`examples/Caddyfile`](../examples/Caddyfile).

```bash
# With Docker Compose (included in docker-compose.yml)
docker compose up -d

# Standalone
caddy run --config examples/Caddyfile
```

### mTLS with Service Mesh

For service-to-service authentication in a microservices environment, deploy
Big Tent behind an Envoy, Linkerd, or Istio sidecar proxy. The sidecar handles
mTLS transparently — Big Tent binds to localhost and the sidecar handles all
external traffic.

### TLS Version Requirements

- Minimum: TLS 1.2
- Recommended: TLS 1.3
- Configure in your reverse proxy (see examples for nginx `ssl_protocols` and
  Caddy's automatic defaults).

---

## Log Management

### Log Format

Big Tent outputs structured JSON logs to stderr via `tracing-subscriber`.
Each line is a JSON object:

```json
{"timestamp":"2025-04-19T17:10:26Z","level":"INFO","message":"Starting big tent git sha abc1234"}
{"timestamp":"2025-04-19T17:10:28Z","level":"INFO","message":"Served /item/gitoid:blob:sha256:... response 200 OK time 1.234ms"}
```

Fields include: `timestamp`, `level`, `message`, plus tracing span fields
(e.g., `event`, `status`, `duration_ms` on reload events).

### Log Volume by RUST_LOG Level

| Level | Approximate Volume | Use Case |
|-------|-------------------|----------|
| `error` | Very low (< 1 line/min) | Production minimum |
| `warn` | Low (a few lines/hour) | Production recommended baseline |
| `info` | Moderate (1 line per HTTP request) | Default, good for most deployments |
| `debug` | High (detailed internal operations) | Debugging only |
| `trace` | Very high | Development only |

### Docker Log Management

Configure the Docker logging driver in `docker-compose.yml` or daemon.json:

```yaml
services:
  bigtent:
    logging:
      driver: json-file
      options:
        max-size: "100m"
        max-file: "5"
```

Or forward to a log aggregator:

```yaml
services:
  bigtent:
    logging:
      driver: fluentd
      options:
        fluentd-address: "localhost:24224"
        tag: "bigtent"
```

### Native (systemd) Log Management

With systemd, logs go to the journal by default:

```bash
# View live logs
journalctl -u bigtent -f

# View logs since last hour
journalctl -u bigtent --since "1 hour ago"

# Export as JSON
journalctl -u bigtent -o json --since today
```

Configure journal retention in `/etc/systemd/journald.conf`:

```ini
SystemMaxUse=2G
MaxRetentionSec=30day
```

If redirecting to a file instead, use [`examples/logrotate.conf`](../examples/logrotate.conf).

### Actionable Log Messages

| Log Message Pattern | Severity | Action Required |
|--------------------|----------|----------------|
| `"Reload failed"` | ERROR | Fix cluster files, send another SIGHUP |
| `"Reload produced zero clusters"` | ERROR | Check cluster directory paths and file permissions |
| `"SIGHUP suppressed (cooldown)"` | WARN | Wait 5 seconds between reloads |
| `"Web server error"` | ERROR | Check port binding, address conflicts |
| `"Path to .grc does not point to a file"` | ERROR | Verify --rodeo paths exist |
| `"Too many open files"` | ERROR | Increase ulimit (`LimitNOFILE` in systemd) |
| `"Unexpected magic number"` | ERROR | Cluster file is corrupt or wrong format |

---

## Monitoring and Alerting

### Prometheus Metrics

Big Tent exposes a `/metrics` endpoint in Prometheus text exposition format.

**Application metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `bigtent_http_requests_total` | Counter (method, path, status) | Total HTTP requests |
| `bigtent_http_request_duration_seconds` | Histogram (method, path) | Request latency |
| `bigtent_node_count` | Gauge | Number of nodes (items) loaded |
| `bigtent_cluster_count` | Gauge | Number of clusters loaded |
| `bigtent_uptime_seconds` | Gauge | Server uptime |
| `bigtent_reload_total` | Counter | Total successful reloads |
| `bigtent_reload_last_success_timestamp` | Gauge | Unix timestamp of last reload |

**Process metrics** (provided by the `prometheus` crate `process` feature):

| Metric | Type | Description |
|--------|------|-------------|
| `process_resident_memory_bytes` | Gauge | Resident memory (RSS) |
| `process_cpu_seconds_total` | Counter | Total CPU time |
| `process_open_fds` | Gauge | Open file descriptors |
| `process_max_fds` | Gauge | Max file descriptors |

### Prometheus Scrape Configuration

```yaml
# prometheus.yml
scrape_configs:
  - job_name: bigtent
    scrape_interval: 15s
    static_configs:
      - targets: ['bigtent.example.com:3000']
    # If behind a reverse proxy with auth:
    # basic_auth:
    #   username: prometheus
    #   password: secret
```

### Recommended Alert Rules

```yaml
# prometheus-alerts.yml
groups:
  - name: bigtent
    rules:
      - alert: BigTentNoData
        expr: bigtent_node_count == 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "BigTent has no data loaded"
          description: "bigtent_node_count has been 0 for 5 minutes"

      - alert: BigTentHighLatency
        expr: histogram_quantile(0.99, rate(bigtent_http_request_duration_seconds_bucket[5m])) > 2
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "BigTent p99 latency above 2s"

      - alert: BigTentHighErrorRate
        expr: rate(bigtent_http_requests_total{status=~"5.."}[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "BigTent 5xx error rate elevated"

      - alert: BigTentHighMemory
        expr: process_resident_memory_bytes / on() group_left() machine_memory_bytes > 0.8
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "BigTent using > 80% of available memory"
```

### Grafana Dashboard Queries

Useful PromQL queries for a Grafana dashboard:

- **Request rate**: `rate(bigtent_http_requests_total[5m])`
- **Error rate**: `rate(bigtent_http_requests_total{status=~"5.."}[5m])`
- **p50 latency**: `histogram_quantile(0.5, rate(bigtent_http_request_duration_seconds_bucket[5m]))`
- **p99 latency**: `histogram_quantile(0.99, rate(bigtent_http_request_duration_seconds_bucket[5m]))`
- **Node count**: `bigtent_node_count`
- **Memory usage**: `process_resident_memory_bytes`
- **Open FDs**: `process_open_fds`
- **Uptime**: `bigtent_uptime_seconds`

### Health Endpoints

| Endpoint | Purpose | Use For |
|----------|---------|---------|
| `GET /healthz` | Liveness probe | Load balancer health check, Kubernetes liveness |
| `GET /readyz` | Readiness probe | Kubernetes readiness, blue-green cutover |
| `GET /health` | Detailed status | Dashboards, debugging, human inspection |

---

## Upgrade and Migration

### Cluster File Versioning

Big Tent cluster files (`.grc`) contain a version field and magic number
(`0xba4a4a`). The current format version is 3. Big Tent validates the magic
number and version on load — incompatible files produce clear error messages.

### Upgrade Procedure

Big Tent serves read-only data. There is no database migration or state to
preserve between versions. The upgrade procedure is:

1. **Stop** the old instance
2. **Start** the new version with the same `--rodeo` or `--cluster-list` arguments
3. **Verify** with `curl /healthz` and `curl /readyz`

### Zero-Downtime Upgrade (Blue-Green)

For zero-downtime upgrades using a reverse proxy:

1. Start the new Big Tent instance on a different port:
   ```bash
   bigtent --rodeo /data/clusters --port 3001
   ```
2. Wait for it to be ready:
   ```bash
   until curl -sf http://localhost:3001/readyz; do sleep 1; done
   ```
3. Update the reverse proxy to point to port 3001
4. Stop the old instance on port 3000

### Rolling Reload via SIGHUP

To update cluster data without restarting:

1. Place new cluster files on disk (in the `--rodeo` or `--cluster-list` directories)
2. Send SIGHUP:
   ```bash
   kill -HUP $(cat /run/bigtent/bigtent.pid)
   # or
   sudo systemctl reload bigtent
   ```
3. Big Tent atomically swaps in the new data. Existing in-flight requests
   complete against the old data; new requests use the new data.
4. Check the logs for `"reload" "success"` or `"Reload failed"`.

There is a 5-second cooldown between reloads to prevent thrashing.

### Docker Tag Pinning

In production, pin to a specific semver tag rather than `:latest`:

```yaml
# Good
image: spicelabs/bigtent:0.13.0

# Avoid in production
image: spicelabs/bigtent:latest
```

### Breaking Change Policy

Big Tent follows semantic versioning (semver):
- **Patch** (0.13.x): Bug fixes, no config changes needed
- **Minor** (0.x.0): New features, backward-compatible
- **Major** (x.0.0): Breaking changes — check release notes before upgrading

---

## Operational Runbook

| Scenario | Diagnosis | Resolution |
|----------|-----------|------------|
| **Server unresponsive** | Check `curl /healthz`, check process with `ps aux \| grep bigtent`, check logs | Restart service. Check `dmesg` for OOM killer. |
| **Memory spike** | Check `process_resident_memory_bytes` metric, check `bigtent_node_count` | Reduce loaded clusters. Use `--cache-index false` (default). |
| **Disk full during merge** | Check `--dest` filesystem with `df -h` | Free space. Set `--buffer-limit` lower. |
| **Corrupt cluster file** | `bigtent --rodeo <path> --check` reports errors | Remove the corrupt cluster. Re-merge from source data. |
| **SIGHUP reload fails** | Check logs for `"Reload failed"` message | Fix cluster files. Send another SIGHUP. |
| **Stale PID file** | `kill -0 <pid>` to check if process exists | Delete PID file if process is dead. Restart service. |
| **"Too many open files"** | Check `ulimit -n`, check `process_open_fds` metric | `ulimit -n 65536` or set `LimitNOFILE=65536` in systemd unit. |
| **Slow queries** | Check `bigtent_http_request_duration_seconds` p99 by path | Enable `--cache-index true`. Use SSD storage. Check if the slow path is `/north` or `/flatten` (graph traversals are inherently slower). |
| **5xx errors** | Check `bigtent_http_requests_total{status=~"5.."}` and logs | Usually indicates cluster file I/O errors. Check disk health and file permissions. |
| **Readyz returns 503** | `curl /readyz` returns `{"ready":false}` | Cluster loading is still in progress, or no clusters were found. Check `--rodeo` paths and logs. |
