import datetime as dt

from pyspark.sql import functions as F

from revint.config import load_config
from revint.detectors import burst, rings
from revint.scoring import noisy_or

CFG = load_config()


def _reviews(spark, rows):
    df = spark.createDataFrame(rows, "category string, review_id long, user_id string, parent_asin string, "
                                     "rating double, review_ts timestamp, verified_purchase boolean")
    return df.withColumn("review_date", F.to_date("review_ts"))


def test_burst_detects_spike_not_flat(spark):
    t0 = dt.datetime(2020, 1, 6)
    rows, rid = [], 0
    for prod, spike in (("SPIKY", True), ("FLAT", False)):
        for w in range(40):
            n = 2 if not (spike and w == 30) else 30
            for i in range(n):
                rid += 1
                rows.append(("C", rid, f"u{rid}", prod, 5.0, t0 + dt.timedelta(weeks=w, hours=i), True))
    res = burst.detect(_reviews(spark, rows), CFG)
    hit = {r["parent_asin"] for r in res["bursts"].collect()}
    assert hit == {"SPIKY"}
    assert res["members"].count() == 30


def test_rings_find_lockstep_group_and_ignore_random(spark):
    t0 = dt.datetime(2021, 3, 1)
    rows, rid = [], 0
    ring_users = [f"ring{i}" for i in range(6)]
    for p in range(5):
        for u in ring_users:
            rid += 1
            rows.append(("C", rid, u, f"P{p}", 5.0, t0 + dt.timedelta(days=30 * p, hours=rid % 40), False))
    for i in range(200):  # random background users, spread out in time
        rid += 1
        rows.append(("C", rid, f"bg{i}", f"P{i % 5}", 4.0, t0 + dt.timedelta(days=3 * i), True))
    res = rings.detect(_reviews(spark, rows), CFG)
    r = res["rings"].filter(~F.col("is_giant")).collect()
    assert len(r) == 1 and r[0]["n_members"] == 6 and r[0]["n_products"] == 5
    assert res["members"].count() == 30


def test_noisy_or(spark):
    df = spark.createDataFrame([(1.0, 1.0), (0.0, 0.0), (1.0, 0.0)], "a double, b double")
    out = [round(r[0], 4) for r in df.select(noisy_or([(F.col("a"), 0.5), (F.col("b"), 0.5)])).collect()]
    assert out == [0.75, 0.0, 0.5]
