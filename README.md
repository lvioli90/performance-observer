# Performance Observer Suite

A Python-based observability and KPI collection suite for performance testing
of a satellite product ingestion pipeline.

**Scope:** MinIO-triggered ingestion → Argo Workflows → workflow steps/pods → STAC catalog publication.

This is **not** a load generator. A separate Argo workflow already injects
products at ~10 products/minute. This suite watches that run and collects
metrics, computes KPIs, and generates diagnostic plots.

---

## Project Structure

```
performance-observer/
├── config/
│   └── example.config.yaml       # Annotated configuration template
├── collectors/
│   ├── argo.py                   # Argo workflow state collector
│   ├── k8s.py                    # Pod status + CPU/memory metrics collector
│   └── stac.py                   # STAC catalog visibility collector
├── core/
│   ├── models.py                 # Dataclasses: WorkflowRecord, PodRecord, etc.
│   ├── run_context.py            # Config loading + shared in-memory state
│   ├── correlator.py             # Product ↔ workflow ↔ pod ↔ STAC correlation
│   ├── kpi.py                    # KPI computation engine
│   └── persistence.py            # Incremental NDJSON persistence + checkpoints
├── reporting/
│   ├── export_csv.py             # CSV writers (products, steps, timeseries)
│   ├── export_json.py            # run_summary.json + Markdown summary
│   └── plots.py                  # Matplotlib diagnostic plots
├── scripts/
│   ├── observe_run.py            # Main observer (runs during the test)
│   ├── compute_kpis.py           # Post-run KPI computation
│   └── plot_results.py           # Diagnostic plot generation
└── requirements.txt
```

---

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure

```bash
cp config/example.config.yaml config/my-run.yaml
# Edit: argo.server_url, stac.endpoint, correlation rules, etc.
```

### 3. Start the existing injection workflow

```bash
# Your existing Argo injection workflow (already in place):
# kubectl -n datalake apply -f <injection-workflow.yaml>
# OR trigger via Argo UI
```

### 4. Start the observer

```bash
# Basic run (uses max_duration_sec from config)
python scripts/observe_run.py --config config/my-run.yaml

# Override run ID and duration
python scripts/observe_run.py --config config/my-run.yaml \
    --run-id baseline-001 --duration 1800
```

The observer writes raw data incrementally to `./results/<run-id>/raw/`.

### 5. Compute KPIs (after or during the run)

```bash
python scripts/compute_kpis.py --run-dir results/baseline-001
```

Produces: `products.csv`, `steps.csv`, `timeseries.csv`, `step_kpis.csv`,
`run_summary.json`, `run_summary.md`.

### 6. Generate plots

```bash
python scripts/plot_results.py --run-dir results/baseline-001
```

Plots are written to `results/baseline-001/plots/`.

---

## Configuration Reference

See [`config/example.config.yaml`](config/example.config.yaml) for the full
annotated schema. Key sections:

| Section | Purpose |
|---------|---------|
| `argo` | Argo Server URL, namespace, workflow filter |
| `kubernetes` | Pod namespaces, kubeconfig path |
| `stac` | STAC API endpoint, collection, auth token |
| `polling` | Per-collector poll intervals (seconds) |
| `timing` | `max_duration_sec`, `grace_period_sec` |
| `correlation` | Rules for linking object key → product_id → STAC item |
| `tracked_steps` | Step names to collect individually |
| `output` | Base directory, checkpoint interval |

---

## Output Files

| File | Description |
|------|-------------|
| `raw/workflows.ndjson` | Raw workflow snapshots (append-only NDJSON) |
| `raw/pods.ndjson` | Raw pod snapshots |
| `raw/products.ndjson` | Correlated product records |
| `raw/timeseries.ndjson` | Timeseries snapshots (one per poll tick) |
| `raw/stac.ndjson` | STAC item visibility records |
| `checkpoint.json` | Full in-memory state snapshot (periodic) |
| `products.csv` | End-to-end product view with derived latencies |
| `steps.csv` | Per-pod/step execution details |
| `timeseries.csv` | Workflow + product counts over time |
| `step_kpis.csv` | Aggregated per-step KPIs |
| `run_summary.json` | Full KPI summary (all computed metrics) |
| `run_summary.md` | Human-readable Markdown summary |
| `plots/*.png` | Diagnostic plots (8 charts) |

---

## Plots Generated

| Plot | Diagnoses |
|------|-----------|
| `throughput_over_time.png` | Is throughput meeting the target? |
| `workflows_pending_over_time.png` | Is Argo backlog growing? (Case A) |
| `workflows_running_over_time.png` | Concurrency vs. capacity |
| `e2e_latency_over_time.png` | End-to-end latency trend |
| `queue_time_over_time.png` | Scheduling/orchestration bottleneck |
| `step_duration_comparison.png` | Which step is slowest? |
| `step_cpu_peak.png` | CPU-bound steps (Case C) |
| `step_mem_peak.png` | Memory pressure (Case D) |

---

## Correlation Strategy

```
MinIO object key
      │  label: workflows.argoproj.io/parameter-product-key
      ▼
Argo workflow  ──(product_id_from_object_key regex)──► product_id
      │
      │  label: workflows.argoproj.io/workflow
      ▼
Kubernetes pod ──(step_name label)──► step KPIs
                                            │
                                            ▼
                              STAC item id = template.format(product_id=...)
```

**Three correlation strategies** (in priority order):

1. **Label-based** (primary): workflow carries a label/parameter with the
   MinIO object key; product_id extracted by regex.
2. **Name-based** (secondary): product_id extracted from the workflow name
   by regex (configurable).
3. **Time-window fallback**: workflows matched to injection events by
   creation timestamp proximity (unreliable at high concurrency).

All assumptions are configurable in `correlation:` section of the YAML.

---

## Bottleneck Diagnosis

The KPI engine emits diagnostic hints based on these patterns:

| Case | Indicators | Interpretation |
|------|-----------|----------------|
| **A** | `max_pending_workflows` high, queue time p95 high | Argo orchestration bottleneck |
| **B** | Queue time low, pod running time high | Pod logic or external dependency |
| **C** | CPU peak high + long running time | CPU-bound code |
| **D** | OOM kills, restart count > 0, high memory | Memory pressure |
| **E** | Low CPU + long running time | Waiting on MinIO/STAC/external |

---

## Long-Run Safety

| Feature | Implementation |
|---------|---------------|
| Incremental persistence | NDJSON append on every poll cycle |
| Retry on API errors | `try/except` in all collector poll loops; logs and continues |
| Graceful shutdown | `SIGINT`/`SIGTERM` → `stop_event.set()` → final flush |
| Checkpoints | Full state JSON saved every `checkpoint_interval_sec` seconds |
| Thread isolation | Each collector in a daemon thread; main thread monitors time limit |
| Grace period | After `max_duration_sec`, observer waits `grace_period_sec` more for late STAC items |

---

## Execution Flow (Recommended)

```bash
# 1. Start the existing injection workflow (already in datalake namespace)
#    kubectl apply -f ingestion-load-workflow.yaml

# 2. Start the observer immediately
python scripts/observe_run.py \
    --config config/my-run.yaml \
    --run-id capacity-test-001 \
    --duration 14400   # 4 hours

# 3. Observer stops automatically after duration + grace period
#    (or press Ctrl-C for early stop)

# 4. Compute KPIs
python scripts/compute_kpis.py --run-dir results/capacity-test-001

# 5. Generate plots
python scripts/plot_results.py --run-dir results/capacity-test-001

# 6. Inspect results
open results/capacity-test-001/run_summary.md
open results/capacity-test-001/plots/
```

---

## Plug-In Points

The following items must be configured or adapted for your specific cluster:

| Item | Where | What to change |
|------|-------|---------------|
| Argo Server URL | `config.yaml argo.server_url` | Your Argo Server address |
| STAC endpoint | `config.yaml stac.endpoint` | Your STAC API base URL |
| Correlation label | `config.yaml correlation.object_key_label` | The exact label name on your workflows |
| Product ID regex | `config.yaml correlation.product_id_from_object_key` | Your naming convention |
| STAC item ID | `config.yaml correlation.stac_item_id_from_product_id` | Your STAC item id format |
| Tracked steps | `config.yaml tracked_steps` | Your workflow step names |
| kubeconfig | `config.yaml kubernetes.kubeconfig` | In-cluster or file path |

---

## Test Scenarios

| Scenario | `--duration` | Expected products |
|----------|-------------|-------------------|
| Smoke (15 min) | `900` | ~150 |
| Baseline (30 min) | `1800` | ~300 |
| Capacity (4 h) | `14400` | ~2,400 |
| Soak (8 h) | `28800` | ~4,800 |
| Validation (24 h) | `86400` | ~14,400 |
