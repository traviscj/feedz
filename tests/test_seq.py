from feedz.queries import SeqQueries


def test_next_is_monotonic_from_one(engine):
    sq = SeqQueries(engine)
    assert [sq.next("s"), sq.next("s"), sq.next("s")] == [1, 2, 3]


def test_sequences_are_independent(engine):
    sq = SeqQueries(engine)
    sq.next("a")
    sq.next("a")
    assert sq.next("b") == 1
    assert {s.name: s.value for s in sq.all()} == {"a": 2, "b": 1}


def test_version_tracks_every_advance(engine):
    sq = SeqQueries(engine)
    sq.next("s")
    sq.next("s")
    assert sq.get("s").version == 2
    sq.set("s", 100)
    got = sq.get("s")
    assert (got.value, got.version) == (100, 3)


def test_get_missing(engine):
    assert SeqQueries(engine).get("nope") is None
