# HBase schema

HBase 2.5.10 runs in standalone mode with the Thrift1 server on port 9090. Clients use happybase.
Values are stored as UTF-8 strings so they are readable in `hbase shell`; numbers that must sort are
zero-padded to 13 digits. Tables are created idempotently by `scripts/hbase_create_tables.py`.

| Table | Row key | Families (versions, TTL) | Columns | Main access pattern |
|---|---|---|---|---|
| `flight_track` (16 regions) | `salt\|icao24\|rev_ts` | `pos` (1, 7 d), `ctx` (1, 7 d), `wx` (1, 7 d) | `pos:lat lon alt_ft gs_kt track vr_fpm gnd`, `ctx:callsign airport dist_nm h3_r7 src` | Prefix scan `salt\|icao24\|` returns an aircraft's track newest first |
| `flight_state` | `icao24` | `s` (1, 1 d) | `s:callsign lat lon alt_ft gs_kt track vr_fpm phase airport category holding last_seen` | Full scan with a server-side `SingleColumnValueFilter` on `last_seen` for the live map |
| `airspace_cell` | `h3_r5\|yyyyMMddHHmm` | `a` (1, 1 d) | one dynamic column `a:<icao24>` per aircraft, value = altitude | Density heat map |
| `airport_live` | `ICAO` | `c` (1440 versions, 2 d), `w` (200 versions, 7 d) | `c:traffic_40nm low_alt_count avg_gs_kt arrivals_15m holds_15m go_arounds_15m`, `w:raw vis_m wind_dir wind_kt ceiling_ft fog category` | Latest = `row()`; history = `cells(..., versions=1440)` |
| `events` | `ICAO\|rev_ts\|icao24\|type` | `e` (1, 30 d) | `e:type icao24 callsign ts details` | Prefix scan per airport returns recent events first |
| `predictions` | `icao24\|entry_ts` | `p` (1, 30 d) | `p:airport pred_min model_version features actual_min` | Point lookup of a prediction and its outcome |

## Why this design

**Salting.** Track keys start with `crc32(icao24) % 16`, formatted as two digits. Keys that begin with a
timestamp or an increasing id would send every write to one region; the salt spreads writes across
all 16 regions. `flight_track` is pre-split on `01` to `15` so those regions exist from the start.

**Reverse timestamp.** `rev_ts = 9999999999999 - ts_ms`, zero-padded. HBase sorts keys ascending, so
the newest sample of an aircraft sorts first and `scan(row_prefix=..., limit=N)` returns the latest
N positions without reading the rest.

**Dynamic columns.** An H3 cell can contain any number of aircraft over a minute. `airspace_cell` stores
one qualifier per aircraft (`a:800c4a`), so rows are sparse, grow as needed and need no schema change.
The number of qualifiers in a row is the aircraft count.

**Cell versions as a time series.** `airport_live` keeps up to 1440 versions of each metric. J3 writes
every value with an explicit timestamp equal to the window end, so versions form a per-minute series for
24 hours without extra rows, and a late update of a window overwrites that window's own version.

**TTL retention.** Families expire automatically (tracks 7 days, state 1 day, events and predictions 30
days), which bounds storage without a cleanup job.

**Idempotent writes.** Every key and cell timestamp is a deterministic function of event time and ids.
Replaying a micro-batch after a failure rewrites identical cells, so HBase output is effectively
exactly-once.

## Useful shell commands

```
make shell-hbase
list
describe 'flight_track'
scan 'airport_live', {LIMIT => 1, VERSIONS => 3}
get 'airport_live', 'VIDP', {COLUMN => 'c:traffic_40nm', VERSIONS => 10}
scan 'events', {STARTROW => 'VIDP|', LIMIT => 5}
```
