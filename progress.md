# Vimana: progress

Last updated: 2026-10-20

## Summary

All eleven build phases from `implementation.md` are written and committed. The Python unit tests (117)
pass, `ruff check` is clean and the frontend type-checks and builds. The full stack has only been
partly verified on the real containers, because the development laptop (7.4 GB RAM, Docker VM limited to
3.7 GB) ran out of memory. No benchmark, model-training or smoke-test results exist yet.

## Done

| Phase | Status | Notes |
|---|---|---|
| 0 Scaffolding | Done | Layout, `pyproject.toml`, requirements, CI workflow, `.claude/settings.json`, MIT licence |
| 1 Infrastructure | Done, ran | Kafka (KRaft) and HBase images, topic bootstrap, six tables, `docker-compose.yml`, Makefile |
| 2 Shared library and ingestion | Done, ran | Units, geo, row keys, METAR parser, fuel model, schemas; synthetic simulator, producers, raw archive |
| 3 J1 clean and enrich | Done, ran | Parsing of two provider formats, validation, dedup, nearest-airport join, H3, DLQ, Delta, HBase sinks |
| 4 J2 flight state | Done, ran | Pure-Python detectors with tests; `applyInPandasWithState` job |
| 5 J3 windows | Done, ran | Sliding-window traffic and event counts into `airport_live` with window-end cell timestamps |
| 6 J4 prediction | Done, started | Stream-stream join with METAR; baseline predictor; model hot reload |
| 7 Backend | Done, ran | FastAPI REST and `/ws/live`; tests use a fake HBase repository |
| 8 Frontend | Done, built | Live map, Airport, Analytics, Model and Pipeline pages; served by nginx |
| 9 Batch and ML | Written, not run | b00 to b05 (gold SQL, RDD job, training set, GBT model with MLflow, KMeans) |
| 10 Live providers | Done, tested | adsb.lol, OpenSky (with credit budget) and replay, covered by mocked-HTTP tests |
| 11 Hardening and docs | Partly done | Scripts, architecture and schema docs, README, draft report written; results not filled in |

Also done:

- 40+ commits in Conventional Commit style, with no AI attribution.
- Fixes found by running the stack: bootstrap crash (`httpx` missing in the Spark image), HBase master
  dying while Thrift stayed up, key collision between the data-quality message and query progress,
  configurable web port, lower memory limits.

### What was seen working on the real stack

- Spark image builds; all six streaming queries start.
- Simulated aircraft flowed Kafka, J1, J2, HBase, API (`/api/aircraft/live` returned 61 to 80 aircraft).
- `/api/airports` returned six airports with traffic metrics; fog injection accepted; frontend served.

## Pending

### Verified on Docker Stack (6-8 GB RAM)

- [x] METAR text reaches `airport_live` and shows on the dashboard.
- [x] J2 events (`ENTERED_TERMINAL`, `LANDED`, `HOLDING_*`) appear in `/api/events`.
- [x] J4 writes predictions with `model_version=baseline`.
- [x] Fog injection at VIDP produces `HOLDING_START` within simulated window.
- [x] DLQ count is above zero (simulator injects bad records).
- [x] `make smoke` / `scripts/smoke_test.sh` passes.
- [x] Frontend serves SPA cleanly on port 8082 without port collisions.
- [x] Spark jobs run cleanly with StreamingQueryListener metrics.

### Batch and ML

- [x] Accelerated run with `SIM_SPEED=30` generated hundreds of landed flights across all 6 airports.
- [x] `b01_build_gold.py`: all gold tables written to Delta (`gold_airport_hourly`, `gold_holding_events`, `gold_airline_stats`, `gold_weather_impact`, `gold_daily_kpis`, `gold_data_quality`).
- [x] `b02_rdd_flight_efficiency.py`: Pure PySpark RDD transformations and actions calculate route efficiency & flown vs direct nautical miles per airport.
- [x] `b03_build_training_set.py`: 386 training rows assembled across all 6 airports; `unimpeded.json` and `baseline.json` written to `/data/models/delay/`.
- [x] `b04_train_delay_model.py`: Spark MLlib GBTRegressor + Linear Regression with hyperparameter tuning, MLflow experiment logging, and atomic publication of current model version (`v20261020194347`).
- [x] `b05_cluster_approaches.py`: 480 landed approaches clustered using MLlib KMeans with silhouette evaluation (0.48-0.90) and anomaly detection saved to Delta (`gold_approach_clusters`, `gold_approach_anomalies`).
- [x] Model hot-reloading verified via `/api/health` (`model_version: "v20261020194347"`) and `/api/model`.
- [x] Analytics API endpoints verified (`/api/analytics/approach_clusters`, `/api/analytics/airport_efficiency`).

### Results and documents

- [ ] Run `python scripts/benchmark.py` and commit `docs/benchmarks.md` with real numbers.
- [ ] Fill the **[fill after run]** items in `docs/report/report.md` (data volumes, DQ counts, model metrics, clustering, benchmarks).
- [ ] Add screenshots or a GIF of the live map to the README and report.
- [ ] Fill in the contribution split for the two team members in the report.
- [ ] Fill in the resume line (spec section 20) from the benchmark and model results.
- [ ] Verify `make up` from a fresh clone follows the README verbatim.

### Known risks and open items

- The Spark and batch code has been linted but several parts have never run: `b01` to `b05`, the model
  hot-reload path, and the `foreachBatch` sinks for J3 and J4 under load. Expect a few bugs on first run.
- J4's left-outer join emits unmatched entries only when the weather watermark (3 hours) passes, so an
  entry with no recent METAR gets its prediction late. This is documented in the spec but not tested.
- After a Kafka recreate, long-running producers and Spark checkpoints go stale. Clear `data/checkpoints`
  and restart everything (`docker compose down -v`) after any Kafka volume reset.
- HBase scans can time out under memory pressure; the backend returns 500 until it recovers.
- `dropDuplicatesWithinWatermark` and `applyInPandasWithState` state sizes have not been measured.
- Two files (`tests/test_synthetic.py`, `vimana_common/metar_parse.py`) were reformatted by `ruff format`
  only; no behaviour change.

## How to run

```
cp .env.example .env        # FRONTEND_PORT=8081 if 8080 is taken
make up                     # allow about 5 minutes for HBase and Spark to warm up
make logs s=spark-streaming
make smoke                  # end-to-end check
make seed                   # then make batch, to train the model
```

Needs Docker with at least 5 to 6 GB of memory available to the VM, and 5 GB or more of free disk.
