from vimana_common import rowkeys as rk


def test_salt_is_stable_and_bounded():
    assert rk.salt("800c4a") == rk.salt("800c4a")
    buckets = {rk.salt(f"{i:06x}") for i in range(2000)}
    assert buckets <= {f"{i:02d}" for i in range(rk.SALT_BUCKETS)}
    assert len(buckets) == rk.SALT_BUCKETS


def test_reverse_timestamp_orders_newest_first():
    older = rk.track_rowkey("800c4a", 1_760_869_195_000)
    newer = rk.track_rowkey("800c4a", 1_760_869_200_000)
    assert newer < older
    assert rk.rev_ts(1) == f"{rk.MAX_TS - 1:013d}"
    assert len(rk.rev_ts(1_760_869_195_000)) == 13


def test_track_key_round_trip_and_prefix():
    key = rk.track_rowkey("800c4a", 1_760_869_195_000)
    assert key.startswith(rk.track_prefix("800c4a"))
    assert rk.parse_track_rowkey(key) == ("800c4a", 1_760_869_195_000)


def test_event_keys_group_by_airport_newest_first():
    a = rk.event_rowkey("vidp", 1000, "abc123", "LANDED")
    b = rk.event_rowkey("VIDP", 2000, "abc123", "LANDED")
    assert a.startswith(rk.event_prefix("VIDP"))
    assert b < a


def test_prediction_and_cell_keys():
    assert rk.prediction_rowkey("800c4a", 42) == b"800c4a|0000000000042"
    # 2025-10-19 10:00:00 UTC
    assert rk.cell_rowkey("85abc", 1_760_868_000_000) == b"85abc|202510191000"
    assert rk.airport_rowkey("vidp") == b"VIDP"
    assert rk.state_rowkey("800c4a") == b"800c4a"
