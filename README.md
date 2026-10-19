# Vimana

Real-time airspace intelligence platform: streams ADS-B and METAR data through
Kafka and Spark Structured Streaming, stores operational state in HBase and
Delta Lake, and serves a live map dashboard.

Status: under construction. See [implementation.md](implementation.md) for the
full specification and build phases.

## Quick start

```
cp .env.example .env
make up
```

Then open http://localhost:8080.

## License

MIT
