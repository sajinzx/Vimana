"""J1: parse, validate, standardise, deduplicate and enrich raw ADS-B; ingest METAR.

adsb.raw -> adsb.clean (Kafka), bronze/silver Delta, HBase flight_track and
airspace_cell, dlq for rejected records, pipeline.metrics for data quality.
weather.metar -> silver/metar Delta and HBase airport_live weather columns.
"""

from __future__ import annotations

import time
from typing import Any

import orjson
import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from streaming.sinks.hbase_sink import write_metar_partition, write_tracks_partition
from vimana_common.airports import load_airports, outer_radius_nm
from vimana_common.geo import h3_cell
from vimana_common.metar_parse import parse_metar_safe
from vimana_common.schemas import METAR_COLUMNS, POSITION_COLUMNS, metar_schema
from vimana_common.settings import app_config, get_settings
from vimana_common.units import m_to_ft, ms_to_fpm, ms_to_kt

M_TO_FT = m_to_ft(1.0)
MS_TO_KT = ms_to_kt(1.0)
MS_TO_FPM = ms_to_fpm(1.0)
EARTH_RADIUS_NM = 3440.065

# adsb.lol aircraft type designators that are wide-body / regional.
WIDEBODY_TYPES = r"^(B74|B77|B78|B76|A33|A34|A35|A38|A30|A310|MD11|IL96)"
REGIONAL_TYPES = r"^(AT4|AT7|DH8|E1|E7|E9|CRJ|CRK|SF3|B190|F100|F70|SU9|AN2|AN3|D328)"

TRIGGER = "5 seconds"


def kafka_options() -> dict[str, str]:
    return {"kafka.bootstrap.servers": get_settings().kafka_bootstrap}


def delta_path(rel: str) -> str:
    return f"{get_settings().data_dir}/delta/{rel}"


def checkpoint(name: str) -> str:
    return f"{get_settings().data_dir}/checkpoints/{name}"


# --------------------------------------------------------------------------
# Parsing and standardisation
# --------------------------------------------------------------------------


def read_raw(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .options(**kafka_options())
        .option("subscribe", "adsb.raw")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 50000)
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.col("value").cast("string").alias("raw_value"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
        )
    )


def _jget(path: str) -> Column:
    return F.get_json_object(F.col("raw_value"), path)


def _clean_callsign(c: Column) -> Column:
    s = F.upper(F.trim(c))
    return F.when(s == "", F.lit(None)).otherwise(s)


def parse_opensky(raw: DataFrame) -> DataFrame:
    """OpenSky-shaped positional arrays (also emitted by the synthetic provider)."""

    def st(i: int) -> Column:
        return _jget(f"$.state[{i}]")

    cat = st(17).cast("int")
    category = F.when(cat <= 3, "regional").when(cat >= 6, "widebody").otherwise("narrowbody")
    return raw.filter(_jget("$.state").isNotNull()).select(
        "raw_value",
        "kafka_partition",
        "kafka_offset",
        "kafka_ts",
        F.lower(F.trim(st(0))).alias("icao24"),
        _clean_callsign(st(1)).alias("callsign"),
        (st(3).cast("double") * 1000).cast("long").alias("ts"),
        _jget("$.fetched_at").cast("long").alias("fetched_at"),
        st(6).cast("double").alias("lat"),
        st(5).cast("double").alias("lon"),
        (st(7).cast("double") * M_TO_FT).alias("alt_ft"),
        (st(9).cast("double") * MS_TO_KT).alias("gs_kt"),
        st(10).cast("double").alias("track_deg"),
        (st(11).cast("double") * MS_TO_FPM).alias("vr_fpm"),
        (st(8) == "true").alias("on_ground"),
        category.alias("category"),
        F.coalesce(_jget("$.source"), F.lit("opensky")).alias("source"),
    )


def parse_adsblol(raw: DataFrame) -> DataFrame:
    """adsb.lol nested objects; already in ft / kt / fpm."""

    def ac(name: str) -> Column:
        return _jget(f"$.ac.{name}")

    fetched = _jget("$.fetched_at").cast("long")
    typ = F.upper(F.coalesce(ac("t"), F.lit("")))
    category = (
        F.when(typ.rlike(WIDEBODY_TYPES), "widebody")
        .when(typ.rlike(REGIONAL_TYPES), "regional")
        .otherwise("narrowbody")
    )
    return raw.filter(_jget("$.ac").isNotNull()).select(
        "raw_value",
        "kafka_partition",
        "kafka_offset",
        "kafka_ts",
        F.lower(F.trim(ac("hex"))).alias("icao24"),
        _clean_callsign(ac("flight")).alias("callsign"),
        (
            fetched - (F.coalesce(ac("seen_pos").cast("double"), F.lit(0.0)) * 1000).cast("long")
        ).alias("ts"),
        fetched.alias("fetched_at"),
        ac("lat").cast("double").alias("lat"),
        ac("lon").cast("double").alias("lon"),
        ac("alt_baro").cast("double").alias("alt_ft"),
        ac("gs").cast("double").alias("gs_kt"),
        ac("track").cast("double").alias("track_deg"),
        ac("baro_rate").cast("double").alias("vr_fpm"),
        (ac("alt_baro") == "ground").alias("on_ground"),
        category.alias("category"),
        F.coalesce(_jget("$.source"), F.lit("adsblol")).alias("source"),
    )


def parse_raw(raw: DataFrame) -> DataFrame:
    parsed = parse_opensky(raw).unionByName(parse_adsblol(raw))
    return parsed.withColumn(
        "event_time",
        (
            F.coalesce(F.col("ts"), F.col("fetched_at"), F.col("kafka_ts").cast("long") * 1000)
            / 1000
        ).cast("timestamp"),
    )


# --------------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------------


def rejection_reason() -> Column:
    """First failing validation rule, or NULL when the record is valid."""
    v = app_config()["validation"]
    bb = v["india_bbox"]
    c = F.col
    return (
        F.when(c("icao24").isNull() | ~c("icao24").rlike(v["icao24_regex"]), "bad_icao24")
        .when(c("lat").isNull() | c("lon").isNull(), "missing_position")
        .when(c("ts").isNull(), "missing_ts")
        .when(
            ~c("lat").between(-90, 90) | ~c("lon").between(-180, 180),
            "bad_coordinates",
        )
        .when(
            ~c("lat").between(bb["lat_min"], bb["lat_max"])
            | ~c("lon").between(bb["lon_min"], bb["lon_max"]),
            "outside_region",
        )
        .when(
            c("alt_ft").isNotNull() & ~c("alt_ft").between(v["alt_ft"]["min"], v["alt_ft"]["max"]),
            "bad_altitude",
        )
        .when(
            c("gs_kt").isNotNull() & ~c("gs_kt").between(v["gs_kt"]["min"], v["gs_kt"]["max"]),
            "bad_speed",
        )
        .when(c("ts") < c("fetched_at") - v["max_age_s"] * 1000, "stale")
        .when(c("ts") > c("fetched_at") + v["max_future_s"] * 1000, "future_ts")
    )


def with_validation(parsed: DataFrame) -> DataFrame:
    """Tag rejects and deduplicate within the watermark.

    Valid rows dedupe on (icao24, ts); rejects use their Kafka position so
    distinct bad records are never collapsed into one.
    """
    tagged = parsed.withColumn("invalid_reason", rejection_reason()).withColumn(
        "dedup_id",
        F.when(
            F.col("invalid_reason").isNull(),
            F.concat_ws("|", F.col("icao24"), F.col("ts").cast("string")),
        ).otherwise(
            F.concat_ws(
                "#", F.col("kafka_partition").cast("string"), F.col("kafka_offset").cast("string")
            )
        ),
    )
    return tagged.withWatermark("event_time", "2 minutes").dropDuplicatesWithinWatermark(
        ["dedup_id"]
    )


# --------------------------------------------------------------------------
# Enrichment
# --------------------------------------------------------------------------


def haversine_nm_col(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dphi, dlmb = p2 - p1, F.radians(lon2 - lon1)
    a = F.pow(F.sin(dphi / 2), 2) + F.cos(p1) * F.cos(p2) * F.pow(F.sin(dlmb / 2), 2)
    return 2 * EARTH_RADIUS_NM * F.asin(F.least(F.lit(1.0), F.sqrt(a)))


def bearing_col(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dlmb = F.radians(lon2 - lon1)
    y = F.sin(dlmb) * F.cos(p2)
    x = F.cos(p1) * F.sin(p2) - F.sin(p1) * F.cos(p2) * F.cos(dlmb)
    return F.pmod(F.degrees(F.atan2(y, x)) + 360.0, F.lit(360.0))


def _h3_udf(res: int):
    @F.pandas_udf("string")
    def udf(lat: pd.Series, lon: pd.Series) -> pd.Series:
        def one(a: float, b: float) -> str | None:
            try:
                return h3_cell(float(a), float(b), res)
            except Exception:  # noqa: BLE001 - invalid coordinates map to no cell
                return None

        return pd.Series([one(a, b) for a, b in zip(lat, lon, strict=True)], dtype="object")

    return udf


def enrich(df: DataFrame) -> DataFrame:
    """Nearest configured airport, distance/bearing, AGL altitude and H3 cells."""
    airports = load_airports()
    candidates = F.array(
        *[
            F.struct(
                haversine_nm_col(F.col("lat"), F.col("lon"), F.lit(a.lat), F.lit(a.lon)).alias(
                    "dist"
                ),
                F.lit(a.icao).alias("icao"),
                F.lit(a.lat).alias("alat"),
                F.lit(a.lon).alias("alon"),
                F.lit(a.elev_ft).alias("elev"),
            )
            for a in airports.values()
        ]
    )
    df = df.withColumn("_near", F.array_min(candidates))
    in_range = F.col("_near.dist") <= outer_radius_nm()
    df = (
        df.withColumn("airport", F.when(in_range, F.col("_near.icao")))
        .withColumn("dist_nm", F.when(in_range, F.col("_near.dist")))
        .withColumn(
            "bearing_from_apt",
            F.when(
                in_range,
                bearing_col(F.col("_near.alat"), F.col("_near.alon"), F.col("lat"), F.col("lon")),
            ),
        )
        .withColumn(
            "alt_ft",
            F.when(
                F.col("on_ground") & F.col("alt_ft").isNull() & in_range, F.col("_near.elev")
            ).otherwise(F.col("alt_ft")),
        )
        .withColumn(
            "alt_agl_ft",
            F.when(in_range & F.col("alt_ft").isNotNull(), F.col("alt_ft") - F.col("_near.elev")),
        )
        .drop("_near")
    )
    return df.withColumn("h3_r5", _h3_udf(5)(F.col("lat"), F.col("lon"))).withColumn(
        "h3_r7", _h3_udf(7)(F.col("lat"), F.col("lon"))
    )


def build_clean_stream(spark: SparkSession) -> DataFrame:
    return enrich(with_validation(parse_raw(read_raw(spark))))


# --------------------------------------------------------------------------
# Sinks
# --------------------------------------------------------------------------

_POSITION_NAMES = [n for n, _ in POSITION_COLUMNS]


def write_kafka(df: DataFrame, topic: str) -> None:
    df.write.format("kafka").options(**kafka_options()).option("topic", topic).save()


def delta_append(df: DataFrame, rel: str, app_id: str, batch_id: int, partition_by=()) -> None:
    writer = (
        df.write.format("delta")
        .mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", batch_id)
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(delta_path(rel))


def _publish_dq(spark: SparkSession, batch: DataFrame, valid: DataFrame, batch_id: int) -> None:
    reasons = {
        r["invalid_reason"]: r["count"]
        for r in batch.filter(F.col("invalid_reason").isNotNull())
        .groupBy("invalid_reason")
        .count()
        .collect()
    }
    now_ms = int(time.time() * 1000)
    q = valid.select(
        F.percentile_approx(
            F.lit(now_ms) - F.col("fetched_at"), F.array(F.lit(0.5), F.lit(0.95))
        ).alias("lat"),
        F.count("*").alias("n"),
    ).first()
    p50, p95 = (q["lat"] or [None, None])[:2]
    payload = {
        "query": "j1_clean",
        "batchId": batch_id,
        "ts": now_ms,
        "valid": q["n"],
        "invalid": sum(reasons.values()),
        "invalid_by_reason": reasons,
        "latency_ms_p50": p50,
        "latency_ms_p95": p95,
    }
    row = [("j1_clean", orjson.dumps(payload).decode())]
    write_kafka(spark.createDataFrame(row, "key string, value string"), "pipeline.metrics")


def process_batch(batch: DataFrame, batch_id: int) -> None:
    """foreachBatch body for the clean-track query (idempotent per batch_id)."""
    if batch.isEmpty():
        return
    spark = batch.sparkSession
    batch = batch.persist()
    try:
        valid = batch.filter(F.col("invalid_reason").isNull())
        invalid = batch.filter(F.col("invalid_reason").isNotNull())

        delta_append(
            batch.select("raw_value", "kafka_partition", "kafka_offset", "kafka_ts"),
            "bronze/adsb_raw",
            "j1_bronze",
            batch_id,
        )

        clean = valid.select(*_POSITION_NAMES)
        write_kafka(
            clean.select(
                F.col("icao24").alias("key"),
                F.to_json(F.struct(*[F.col(n) for n in _POSITION_NAMES])).alias("value"),
            ),
            "adsb.clean",
        )
        delta_append(
            clean.withColumn("date", F.to_date((F.col("ts") / 1000).cast("timestamp"))),
            "silver/tracks",
            "j1_silver",
            batch_id,
            partition_by=("date", "airport"),
        )

        if not invalid.isEmpty():
            write_kafka(
                invalid.select(
                    F.col("source").alias("key"),
                    F.to_json(
                        F.struct(
                            F.col("source"),
                            F.col("invalid_reason").alias("reason"),
                            F.col("raw_value").alias("raw"),
                            F.col("fetched_at").alias("ts"),
                        )
                    ).alias("value"),
                ),
                "dlq",
            )

        clean.foreachPartition(write_tracks_partition)
        _publish_dq(spark, batch, valid, batch_id)
    finally:
        batch.unpersist()


def start_clean_query(spark: SparkSession) -> StreamingQuery:
    return (
        build_clean_stream(spark)
        .writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint("j1_clean"))
        .trigger(processingTime=TRIGGER)
        .queryName("j1_clean")
        .start()
    )


# --------------------------------------------------------------------------
# METAR
# --------------------------------------------------------------------------


@F.pandas_udf(metar_schema())
def _parse_metar_udf(raw: pd.Series, ref_ts: pd.Series) -> pd.DataFrame:
    cols = [n for n, _ in METAR_COLUMNS]
    rows: list[dict[str, Any]] = []
    for r, t in zip(raw, ref_ts, strict=True):
        rows.append(parse_metar_safe(r, int(t)) or {} if isinstance(r, str) else {})
    df = pd.DataFrame(rows, columns=cols).astype(object)
    return df.where(df.notna(), None)


def metar_stream(spark: SparkSession) -> DataFrame:
    """Parsed METAR stream (METAR_COLUMNS plus obs_time); reused by J4."""
    raw = (
        spark.readStream.format("kafka")
        .options(**kafka_options())
        .option("subscribe", "weather.metar")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .select(F.col("value").cast("string").alias("v"))
    )
    parsed = raw.select(
        _parse_metar_udf(
            F.get_json_object("v", "$.raw"),
            F.coalesce(
                F.get_json_object("v", "$.obs_time").cast("long"), F.lit(int(time.time() * 1000))
            ),
        ).alias("m")
    ).select("m.*")
    return parsed.filter(F.col("station").isNotNull() & F.col("obs_ts").isNotNull()).withColumn(
        "obs_time", (F.col("obs_ts") / 1000).cast("timestamp")
    )


def process_metar_batch(batch: DataFrame, batch_id: int) -> None:
    if batch.isEmpty():
        return
    delta_append(batch.drop("obs_time"), "silver/metar", "j1_metar", batch_id)
    batch.foreachPartition(write_metar_partition)


def start_metar_query(spark: SparkSession) -> StreamingQuery:
    return (
        metar_stream(spark)
        .writeStream.foreachBatch(process_metar_batch)
        .option("checkpointLocation", checkpoint("j1_metar"))
        .trigger(processingTime=TRIGGER)
        .queryName("j1_metar")
        .start()
    )
