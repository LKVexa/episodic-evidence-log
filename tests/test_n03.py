import unittest
from pathlib import Path

from n03.core import EpisodeError, EpisodicLog


def ev(eid, actor="alice", scope="sess-1", t=100.0, payload=None):
    return {"event_id": eid, "actor": actor, "source": "agent-run",
            "event_time": t, "ingestion_time": t + 1, "scope": scope, "confidence": 0.9,
            "payload": {"act": eid} if payload is None else payload}


class Recording(unittest.TestCase):
    def setUp(self):
        self.log = EpisodicLog()

    def test_record_binds_all_fields(self):
        r = self.log.record(ev("e1"))
        self.assertEqual(r["seq"], 1)
        self.assertTrue(r["link_digest"].startswith("sha256:"))

    def test_validation(self):
        with self.assertRaises(EpisodeError):
            self.log.record({"event_id": "x"})
        with self.assertRaises(EpisodeError):
            self.log.record(dict(ev("e1"), confidence=1.5))
        with self.assertRaises(EpisodeError):
            self.log.record(dict(ev("e1"), payload="not-a-dict"))

    def test_append_only_duplicate_refused(self):
        self.log.record(ev("e1"))
        with self.assertRaises(EpisodeError) as ctx:
            self.log.record(ev("e1"))
        self.assertIn("append-only", str(ctx.exception))

    def test_scopes_are_independent_chains(self):
        self.log.record(ev("e1", scope="a"))
        r = self.log.record(ev("e1", scope="b"))
        self.assertEqual(r["seq"], 1)


class Replay(unittest.TestCase):
    def setUp(self):
        self.log = EpisodicLog()
        for i in range(1, 5):
            self.log.record(ev(f"e{i}", t=100.0 + i))

    def test_full_replay_deterministic(self):
        r1 = self.log.replay("sess-1")
        r2 = self.log.replay("sess-1")
        self.assertEqual([e["event_id"] for e in r1["events"]],
                         ["e1", "e2", "e3", "e4"])
        self.assertEqual(r1["replay_digest"], r2["replay_digest"])
        self.assertIn("never invented", r1["note"])

    def test_range_replay(self):
        r = self.log.replay("sess-1", from_seq=2, to_seq=3)
        self.assertEqual([e["seq"] for e in r["events"]], [2, 3])

    def test_out_of_range_refused_not_invented(self):
        with self.assertRaises(EpisodeError) as ctx:
            self.log.replay("sess-1", from_seq=1, to_seq=9)
        self.assertIn("refusing to invent", str(ctx.exception))
        with self.assertRaises(EpisodeError):
            self.log.replay("ghost-scope")


class Deletion(unittest.TestCase):
    def setUp(self):
        self.log = EpisodicLog()
        self.log.record(ev("e1", actor="alice"))
        self.log.record(ev("e2", actor="bob"))
        self.log.record(ev("e3", actor="alice"))

    def test_tombstone_erases_payload_keeps_chain(self):
        r = self.log.tombstone_actor("sess-1", "alice")
        self.assertEqual(r["tombstoned"], 2)
        rep = self.log.replay("sess-1")
        by_id = {e["event_id"]: e for e in rep["events"]}
        self.assertTrue(by_id["e1"]["redacted"])
        self.assertIsNone(by_id["e1"]["payload"])
        self.assertFalse(by_id["e2"]["redacted"])
        self.assertEqual(by_id["e2"]["payload"], {"act": "e2"})
        # chain still verifies after deletion
        self.assertEqual(self.log.verify()["verdict"], "PASS")


class Integrity(unittest.TestCase):
    def setUp(self):
        self.log = EpisodicLog()
        self.log.record(ev("e1"))
        self.log.record(ev("e2"))

    def test_verify_pass(self):
        self.assertEqual(self.log.verify()["verdict"], "PASS")

    def test_payload_tamper_detected(self):
        self.log._scopes["sess-1"][0]["payload"] = {"forged": True}
        rep = self.log.verify()
        self.assertEqual(rep["verdict"], "FAIL")
        self.assertEqual(rep["problems"][0]["issue"], "payload tampered")

    def test_reorder_breaks_chain(self):
        self.log._scopes["sess-1"].reverse()
        rep = self.log.verify()
        self.assertEqual(rep["verdict"], "FAIL")
        self.assertTrue(any(p["issue"] == "chain link broken"
                            for p in rep["problems"]))

    def test_no_network_imports(self):
        import n03.core as m
        imports = " ".join(l for l in Path(m.__file__).read_text(encoding="utf-8").splitlines()
                           if l.startswith(("import ", "from ")))
        for bad in ("socket", "http", "urllib", "requests"):
            self.assertNotIn(bad, imports)


class Hardening011(unittest.TestCase):
    """Regression tests for 0.1.1-partial fixes (A029-F1..F4)."""

    def setUp(self):
        self.log = EpisodicLog()

    # F1: aliasing / isolation
    def test_record_payload_isolated_from_caller(self):
        p = {"a": 1}
        self.log.record(ev("e1", payload=p))
        p["a"] = 999
        self.assertEqual(self.log.verify()["verdict"], "PASS")
        rep = self.log.replay("sess-1")
        self.assertEqual(rep["events"][0]["payload"], {"a": 1})

    def test_replay_output_isolated_from_store(self):
        self.log.record(ev("e1"))
        rep = self.log.replay("sess-1")
        rep["events"][0]["payload"]["act"] = "forged"
        self.assertEqual(self.log.verify()["verdict"], "PASS")
        self.assertEqual(self.log.replay("sess-1")["events"][0]["payload"],
                         {"act": "e1"})

    # F2: error contract — invalid input is EpisodeError, never a bare
    # TypeError/ValueError
    def test_invalid_inputs_raise_episode_error_only(self):
        bad = [dict(ev("x"), confidence=None),
               dict(ev("x"), confidence="hi"),
               dict(ev("x"), confidence=True),
               dict(ev("x"), event_id={"d": 1}),
               dict(ev("x"), scope={"d": 1}),
               dict(ev("x"), payload={"s": {1, 2}})]
        for b in bad:
            with self.assertRaises(EpisodeError):
                EpisodicLog().record(b)

    def test_replay_seq_type_error_contract(self):
        self.log.record(ev("e1"))
        with self.assertRaises(EpisodeError):
            self.log.replay("sess-1", to_seq="2")
        with self.assertRaises(EpisodeError):
            self.log.replay("sess-1", from_seq=1.5)

    def test_valid_inputs_still_accepted(self):
        r = self.log.record(ev("ok", payload={"n": 1, "s": "x"}))
        self.assertEqual(r["seq"], 1)
        self.log.record(dict(ev("ok2"), confidence=1))  # int confidence ok

    # F3: strict canonicalization — NaN/Infinity are not JSON
    def test_nan_and_inf_payload_refused(self):
        for v in (float("nan"), float("inf"), float("-inf")):
            with self.assertRaises(EpisodeError):
                EpisodicLog().record(ev("n1", payload={"x": v}))

    # F4: a refused record burns nothing — retry with same id succeeds
    def test_failed_record_is_atomic(self):
        with self.assertRaises(EpisodeError):
            self.log.record(ev("a1", payload={"s": {1, 2}}))
        r = self.log.record(ev("a1"))
        self.assertEqual(r["seq"], 1)
        self.assertEqual(r["event_id"], "a1")
        self.assertEqual(self.log._ingest_n, 1)
        self.assertEqual(self.log.verify()["verdict"], "PASS")

    def test_version_constant(self):
        import n03.core as m
        self.assertEqual(m.VERSION, "0.1.2a1")
        self.assertEqual(m.__version__, m.VERSION)


if __name__ == "__main__":
    unittest.main()
