# Vimana: Real-Time Airspace Intelligence Platform

Course: Big Data and Modern Databases (final package)
Author: Sajin MK

Items marked **[fill after run]** need numbers from a run of the stack (`make seed`, `make batch`,
`python scripts/benchmark.py`). Everything else describes what is implemented in this repository.

## 1. Abstract

Vimana is an end-to-end streaming system for air-traffic monitoring over six major Indian airports. It
ingests ADS-B aircraft telemetry and METAR weather reports into Kafka, processes them with Spark
Structured Streaming, stores operational state in an HBase wide-column schema and history in a Delta Lake
lakehouse, and serves a live web dashboard. Stateful stream processing detects holding patterns,
go-arounds, landings and suspected diversions; a Spark MLlib gradient-boosted-tree model predicts the
additional time each arriving aircraft will spend in the terminal area; and batch jobs built with Spark
SQL, the RDD API and MLlib produce analytics on holding, fuel and CO₂, flight efficiency and approach
paths. The system runs fully offline using a built-in traffic simulator, and can also use live data from
adsb.lol or OpenSky.

## 2. Problem statement and motivation

Arrivals at Delhi, Mumbai, Bengaluru, Chennai, Hyderabad and Kolkata regularly enter holding patterns,
and winter fog at Delhi causes go-arounds and diversions. Each minute of airborne holding burns fuel and
emits CO₂, yet this congestion is rarely visible or quantified in real time. Vimana makes it visible:

1. detect holds, go-arounds and diversions as they happen;
2. measure congestion per airport over sliding windows;
3. predict, at the moment an aircraft enters the 40 NM terminal ring, how much extra time it will spend
   there (modelled on the ASMA additional-time KPI used by Eurocontrol);
4. convert holding time into fuel and CO₂.

## 3. Data sources and characteristics

| Source | Format | Variety |
|---|---|---|
| OpenSky Network | JSON with positional arrays (18 fields per state vector, SI units) | semi-structured |
| adsb.lol | nested JSON objects (feet, knots, feet/min; `alt_baro` may be the string `"ground"`) | semi-structured |
| METAR | coded text, for example `VIDP 190000Z 00000KT 0200 FG VV001 12/12 Q1013` | unstructured text |
| Synthetic simulator | OpenSky-shaped envelopes with injected noise | substitute for live data |
| OurAirports | CSV with coordinates, elevation, runway headings | structured reference |

- **Volume and velocity.** The simulator emits one message per aircraft every 5 s of simulated time.
  **[fill after run: messages per second and per day at 1x and 30x]**
- **Veracity.** The simulator injects duplicates (0.5%), dropped messages (2%), position noise, impossible
  latitudes and missing identifiers (about 0.2%). J1 rejects these into a dead-letter topic with a reason.
  **[fill after run: valid / invalid counts and top reasons from the Pipeline page or `gold_data_quality`]**
- **Variety.** Positional arrays, nested objects and free text are mapped to one canonical schema.

## 4. Architecture and design decisions

See `docs/architecture.md` for the diagram. Key decisions:

- **Kafka** decouples producers, jobs and the API. Keys (aircraft id, airport) preserve per-aircraft
  ordering; a dead-letter topic isolates bad records; a control topic lets the UI drive the simulator.
- **HBase** serves the low-latency access patterns of a live application: newest-first track scans,
  point lookups of state and predictions, and a native time series through cell versions.
- **Delta Lake** gives ACID appends from streaming and a medallion layout (bronze raw, silver cleaned,
  gold aggregated) for batch analytics and ML.
- **One SparkSession** runs all streaming queries to fit the memory budget of a single machine.
- **Pure-Python detectors** keep the event rules unit-testable; Spark only handles state and I/O.

## 5. ETL and stream processing

**J1 clean and enrich.** Parses both provider formats with `get_json_object`, converts to feet, knots and
feet per minute, validates identifiers, coordinates, region, altitude, speed and timestamp skew,
deduplicates on `(icao24, ts)` within a 2-minute watermark, finds the nearest airport with a
haversine-based join, computes distance, bearing and altitude above ground, and adds H3 cells at
resolutions 5 and 7. Sinks: Kafka `adsb.clean`, `dlq`, Delta bronze and silver, HBase tracks and cells,
and data-quality metrics.

**J2 flight state.** `applyInPandasWithState` keeps a `FlightState` per aircraft with an event-time
timeout of 15 minutes. Rules (all thresholds in `config/app.yaml`):

- holding: cumulative signed heading change of at least 330 degrees over a 6-minute buffer, altitude range
  at most 600 ft, ground speed at most 280 kt, bounding radius at most 8 NM;
- go-around: below 1500 ft within 5 NM, then a climb of at least 800 ft with positive vertical rate;
- landing, entry to the terminal area, departure, and diversion (landing elsewhere or climbing away).

**J3 windows.** Sliding 10-minute traffic and 15-minute event-count windows write to HBase with the
window end as the cell timestamp, which makes late updates idempotent and gives a per-minute history.

**J4 prediction.** A left-outer stream-stream join with watermarks attaches the latest METAR from the
previous 90 minutes to each terminal entry, then scores it with the current MLlib model, or a
per-airport median baseline until a model exists.

**Fault tolerance.** Offsets and state are checkpointed; HBase writes are idempotent; Delta appends use
`txnAppId` and `txnVersion`; Kafka sinks are at-least-once and consumers de-duplicate.

## 6. Data model

HBase tables, row-key design, versions and TTLs are documented in `docs/hbase-schema.md`. The Delta
medallion layout is `bronze/adsb_raw`, `silver/{tracks, metar, events, airport_metrics, predictions, dq}`
and `gold/*`.

## 7. Batch analytics

**Spark SQL (`b01`).** Gold tables built with CTEs and window functions: hourly airport statistics with
`LAG` and a 3-hour rolling average, holding events paired with `LEAD`, an airline league table with
`RANK`, weather impact through a range join to the nearest earlier METAR, daily KPIs and data quality.

**RDD API (`b02`).** Horizontal flight inefficiency: `flatMap` to key by aircraft, `groupByKey`,
`mapValues` with sorting, `flatMap` to split flights on gaps or ground contact, `map` / `filter` /
`reduceByKey` per destination airport, a broadcast of airport coordinates, an accumulator counting bad
points, and the actions `count`, `takeOrdered`, `take` and `collect`.
**[fill after run: segments, bad points, per-airport inefficiency %]**

## 8. Machine learning

**Label.** `additional_min = max(transit_min - unimpeded_min, 0)` where `unimpeded_min` is the 20th
percentile of transit time per airport, entry sector (eight 45-degree sectors) and aircraft category.

**Features** (shared between training and streaming in `vimana_common/features.py`): airport, entry sector,
category, hour of day (sine and cosine, IST), weekend flag, airport traffic and holds at entry, traffic
ratio to capacity, visibility, ceiling, wind, crosswind against the main runway, fog and low-visibility
flags, a missing-weather flag, entry altitude and speed.

**Model.** `StringIndexer`, `OneHotEncoder`, `VectorAssembler` and `GBTRegressor`, tuned with
`TrainValidationSplit` over depth, iterations and step size. The split is by time (last 20% held out) to
avoid leakage. Baselines: per-airport median and `LinearRegression`.

| Model | MAE (min) | RMSE (min) | R² |
|---|---|---|---|
| Per-airport median | **[fill after run]** | | |
| Linear regression | | | |
| GBT | | | |

Improvement over the median baseline: **[fill after run; target at least 15%]**. Feature importances and
the predicted-versus-actual plot are on the Model page.

**Approach clustering (`b05`).** Landed approaches are resampled to 20 equally spaced points in a local
east/north frame, standardised, and clustered with KMeans for k = 3 to 10, choosing k by silhouette.
Approaches farther than the 95th percentile of their cluster's distances are flagged.
**[fill after run: chosen k and silhouette per airport]**

## 9. Web application

Five pages: **Live** (MapLibre and deck.gl map with aircraft coloured by phase, airport rings coloured by
congestion, density layer, aircraft panel with track and altitude profile, alert toasts); **Airport**
(congestion gauge, decoded METAR, 24-hour chart, predictions table, events); **Analytics** (holding heat
map, airline table, visibility versus delay, efficiency, daily KPIs, approach clusters); **Model**; and
**Pipeline** (query throughput, latency, dead-letter queue, scenario controls).
**[add screenshots from a run]**

## 10. Evaluation

Throughput, latency and consumer lag per simulator speed are produced by `scripts/benchmark.py` and
written to `docs/benchmarks.md`. **[fill after run]**

Correctness is covered by 117 unit tests (units, geodesy, row keys, METAR parsing, simulator, detectors on
scripted trajectories, features, batch helpers, providers, API) and by `scripts/smoke_test.sh`, which
checks the whole stack from a clean start.

## 11. Limitations and future work

- ADS-B coverage over India is patchy, so most results use simulated traffic; conclusions about real
  traffic need live or replayed data.
- The fuel model uses rough per-category burn rates, not type-specific performance data.
- The unimpeded-time label is an empirical percentile, not a procedural benchmark.
- Everything runs on one node (one Kafka broker, standalone HBase, Spark `local[4]`).
- Future work: multi-node Kafka and HBase, Kubernetes, sequence models for delay, real airline schedules
  and runway configuration, and per-type fuel burn.

## 12. Contribution split

**[fill in: contribution of each team member]**
