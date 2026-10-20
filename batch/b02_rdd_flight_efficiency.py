"""b02: horizontal flight inefficiency with the RDD API.

Demonstrates map / filter / flatMap / groupByKey / mapValues / reduceByKey /
sortBy transformations, broadcast variables, accumulators and the
count / take / takeOrdered / collect actions.
"""

from __future__ import annotations

from batch.flight_efficiency import (
    compute_efficiency,
    inefficiency_pct,
    split_into_flights,
)
from streaming.j1_clean_enrich import delta_path
from streaming.spark_session import build_spark
from vimana_common.airports import load_airports, outer_radius_nm


def main() -> None:
    spark = build_spark("vimana-b02")
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    airports_bc = sc.broadcast({a.icao: (a.lat, a.lon) for a in load_airports().values()})
    bad_points = sc.accumulator(0)
    ring = outer_radius_nm()

    rdd = (
        spark.read.format("delta")
        .load(delta_path("silver/tracks"))
        .select("icao24", "callsign", "ts", "lat", "lon", "alt_ft", "on_ground")
        .rdd
    )

    def to_pair(row):
        if row.lat is None or row.lon is None:
            bad_points.add(1)
            return []
        return [
            (
                row.icao24,
                (row.ts, row.lat, row.lon, row.alt_ft, bool(row.on_ground), row.callsign),
            )
        ]

    segments = (
        rdd.flatMap(to_pair)
        .groupByKey()
        .mapValues(lambda pts: sorted(pts))
        .flatMap(split_into_flights)
        .cache()
    )

    per_flight = segments.map(
        lambda f: (f[0], compute_efficiency(f, airports_bc.value, ring))
    ).filter(lambda kv: kv[1] is not None and kv[1][1][1] > 30)
    per_airport = per_flight.map(lambda kv: kv[1]).reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])
    )

    print("segments:", segments.count())
    top10 = per_flight.map(lambda kv: (kv[0], inefficiency_pct(*kv[1][1][:2]))).takeOrdered(
        10, key=lambda x: -x[1]
    )
    print("least efficient flights (id, % extra distance):")
    for fid, pct in top10:
        print(f"  {fid}: {pct:.1f}%")
    summary = per_airport.mapValues(lambda v: inefficiency_pct(v[0], v[1])).collect()
    print("airport inefficiency %:", sorted(summary))
    sample = per_flight.sortBy(lambda kv: kv[0]).take(3)
    print("sample:", sample)
    print("bad points:", bad_points.value)

    flights_df = spark.createDataFrame(
        per_flight.map(
            lambda kv: (
                kv[0],
                kv[1][0],
                float(kv[1][1][0]),
                float(kv[1][1][1]),
                float(inefficiency_pct(kv[1][1][0], kv[1][1][1])),
            )
        ),
        "flight_id string, airport string, flown_nm double, "
        "direct_nm double, inefficiency_pct double",
    )
    flights_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        delta_path("gold/gold_flight_efficiency")
    )
    airports_df = spark.createDataFrame(
        per_airport.map(
            lambda kv: (
                kv[0],
                int(kv[1][2]),
                float(kv[1][0]),
                float(kv[1][1]),
                float(inefficiency_pct(kv[1][0], kv[1][1])),
            )
        ),
        "airport string, flights long, flown_nm double, direct_nm double, inefficiency_pct double",
    )
    airports_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        delta_path("gold/gold_airport_efficiency")
    )
    spark.stop()


if __name__ == "__main__":
    main()
