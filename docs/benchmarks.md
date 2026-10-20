# Benchmarks

No measurements have been recorded yet. Run the benchmark on the target machine and it will replace
this file with a results table:

```
make up
python scripts/benchmark.py --speeds 1 10 50 100 --minutes 3
```

For each simulator speed the script restarts the producers with `SIM_SPEED=<speed>`, waits 60 s, then
samples `/api/pipeline` every 5 s for the measurement window. It reports, per streaming query
(`j1_clean`, `j2_flight_state`, `j3_traffic`, `j4_predict`):

- median `processedRowsPerSecond`,
- worst p95 end-to-end latency (`now - fetched_at`, published by J1),
- worst `maxOffsetsBehindLatest` (consumer lag).

Notes for interpreting results:

- Latency is only meaningful at 1x, because above that the simulated clock runs ahead of wall time.
- Throughput is bounded by the single-node deployment (Spark `local[4]`, one Kafka broker, standalone
  HBase) and the memory limits in `docker-compose.yml`.
