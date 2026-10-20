"""J2: per-flight stateful processing over adsb.clean.

applyInPandasWithState keeps one FlightState per aircraft (detectors.py),
emitting events (ENTERED_TERMINAL, HOLDING_*, GO_AROUND, LANDED, ...) and a
state snapshot per updated flight each micro-batch.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import orjson
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

from streaming.detectors import FlightState, detector_config
from streaming.j1_clean_enrich import checkpoint, delta_append, kafka_options, write_kafka
from streaming.sinks.hbase_sink import event_mutation, write_rows, write_state_partition
from vimana_common.schemas import EVENT_COLUMNS, position_schema

OUTPUT_DDL = (
    "kind string, event_id string, type string, icao24 string, callsign string, "
    "airport string, ts long, details string, lat double, lon double, alt_ft double, "
    "gs_kt double, track_deg double, vr_fpm double, phase string, category string, "
    "holding boolean, last_seen long"
)
OUTPUT_COLS = [c.split()[0] for c in OUTPUT_DDL.split(", ")]
STATE_DDL = "blob string"
ALERT_TYPES = {"HOLDING_START", "GO_AROUND", "DIVERSION_SUSPECTED"}
SEVERITY = {"HOLDING_START": "warning", "GO_AROUND": "critical", "DIVERSION_SUSPECTED": "critical"}


def read_clean(spark: SparkSession) -> DataFrame:
    """adsb.clean as a typed streaming DataFrame with an event_time column."""
    return (
        spark.readStream.format("kafka")
        .options(**kafka_options())
        .option("subscribe", "adsb.clean")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 50000)
        .option("failOnDataLoss", "false")
        .load()
        .select(F.from_json(F.col("value").cast("string"), position_schema()).alias("p"))
        .select("p.*")
        .withColumn("event_time", (F.col("ts") / 1000).cast("timestamp"))
    )


def _clean(v: Any) -> Any:
    return None if v is None or (isinstance(v, float) and v != v) else v


def _event_row(ev) -> dict[str, Any]:
    row = dict.fromkeys(OUTPUT_COLS)
    row.update(
        kind="event",
        event_id=ev.event_id,
        type=ev.type,
        icao24=ev.icao24,
        callsign=ev.callsign,
        airport=ev.airport,
        ts=ev.ts,
        details=json.dumps(ev.details, separators=(",", ":")),
    )
    return row


def _state_row(st: FlightState, last: dict[str, Any]) -> dict[str, Any]:
    row = dict.fromkeys(OUTPUT_COLS)
    row.update(
        kind="state",
        icao24=st.icao24,
        callsign=st.callsign,
        airport=st.airport,
        ts=st.last_ts,
        lat=last.get("lat"),
        lon=last.get("lon"),
        alt_ft=last.get("alt_ft"),
        gs_kt=last.get("gs_kt"),
        track_deg=last.get("track_deg"),
        vr_fpm=last.get("vr_fpm"),
        phase=st.phase,
        category=st.category,
        holding=st.holding_active,
        last_seen=st.last_ts,
    )
    return row


def update_flight(
    key: tuple[str], pdfs: Iterator[pd.DataFrame], state: GroupState
) -> Iterator[pd.DataFrame]:
    cfg = detector_config()
    timeout_ms = cfg["state_timeout_minutes"] * 60000
    if state.hasTimedOut:
        st = FlightState.from_tuple(state.get)
        state.remove()
        yield pd.DataFrame(
            [_event_row(e) for e in st.finalise_lost(st.last_ts)], columns=OUTPUT_COLS
        )
        return
    st = FlightState.from_tuple(state.get) if state.exists else FlightState.new(key[0])
    rows = pd.concat(list(pdfs)).sort_values("ts")
    out: list[dict[str, Any]] = []
    last: dict[str, Any] = {}
    for r in rows.to_dict("records"):
        last = {k: _clean(v) for k, v in r.items()}
        out.extend(_event_row(e) for e in st.step(last, cfg))
    state.update(st.to_tuple())
    state.setTimeoutTimestamp(int(st.last_ts) + timeout_ms)
    if last:
        out.append(_state_row(st, last))
    yield pd.DataFrame(out, columns=OUTPUT_COLS)


def build_flight_stream(spark: SparkSession) -> DataFrame:
    return (
        read_clean(spark)
        .withWatermark("event_time", "2 minutes")
        .groupBy("icao24")
        .applyInPandasWithState(
            update_flight,
            OUTPUT_DDL,
            STATE_DDL,
            "append",
            GroupStateTimeout.EventTimeTimeout,
        )
    )


def alert_payload(e: dict[str, Any]) -> dict[str, Any]:
    details = json.loads(e["details"] or "{}")
    return {
        "alert_id": e["event_id"],
        "kind": e["type"],
        "severity": SEVERITY[e["type"]],
        "airport": e["airport"],
        "icao24": e["icao24"],
        "callsign": e["callsign"],
        "ts": e["ts"],
        "message": f"{e['type']} {e['callsign'] or e['icao24']} at {e['airport']}",
        "details": details,
    }


def process_batch(batch: DataFrame, batch_id: int) -> None:
    if batch.isEmpty():
        return
    spark = batch.sparkSession
    batch = batch.persist()
    try:
        events = batch.filter(F.col("kind") == "event")
        states = batch.filter(F.col("kind") == "state")

        ev_rows = [r.asDict() for r in events.collect()]
        if ev_rows:
            msgs, alerts = [], []
            for e in ev_rows:
                body = {
                    "event_id": e["event_id"],
                    "type": e["type"],
                    "icao24": e["icao24"],
                    "callsign": e["callsign"],
                    "airport": e["airport"],
                    "ts": e["ts"],
                    "details": json.loads(e["details"] or "{}"),
                }
                msgs.append((e["icao24"], orjson.dumps(body).decode()))
                if e["type"] in ALERT_TYPES:
                    alerts.append((e["airport"], orjson.dumps(alert_payload(e)).decode()))
            ddl = "key string, value string"
            write_kafka(spark.createDataFrame(msgs, ddl), "flight.events")
            if alerts:
                write_kafka(spark.createDataFrame(alerts, ddl), "alerts")
            write_rows("events", (event_mutation(e) for e in ev_rows))
            delta_append(
                events.select(*[n for n, _ in EVENT_COLUMNS]),
                "silver/events",
                "j2_events",
                batch_id,
            )
        states.foreachPartition(write_state_partition)
    finally:
        batch.unpersist()


def start_flight_query(spark: SparkSession) -> StreamingQuery:
    return (
        build_flight_stream(spark)
        .writeStream.outputMode("append")
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint("j2_flight_state"))
        .trigger(processingTime="5 seconds")
        .queryName("j2_flight_state")
        .start()
    )
