from revint.graph import connected_components


def test_connected_components_min_label(spark):
    edges = spark.createDataFrame([(1, 2), (2, 3), (3, 4), (10, 11), (20, 21), (21, 22), (22, 20), (5, 4)],
                                  "src long, dst long")
    comp, iters = connected_components(edges)
    got = {r["node"]: r["component"] for r in comp.collect()}
    assert got == {1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 10: 10, 11: 10, 20: 20, 21: 20, 22: 20}
    assert iters >= 1


def test_long_chain_converges(spark):
    n = 300
    edges = spark.createDataFrame([(i, i + 1) for i in range(n)], "src long, dst long")
    comp, iters = connected_components(edges)
    assert comp.select("component").distinct().count() == 1
    assert iters < 25  # pointer jumping: far fewer rounds than the chain length
