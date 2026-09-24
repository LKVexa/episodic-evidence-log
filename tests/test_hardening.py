import copy
import json
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from n03.core import EpisodicLog, EpisodeError, IntegrityError, _digest


def event(eid="e1", **changes):
    value = {"event_id": eid, "actor": "alice", "source": "worker", "scope": "s1",
             "event_time": 10, "ingestion_time": 11, "confidence": 0.5, "payload": {"value": 1}}
    value.update(changes)
    return value


class Validation(unittest.TestCase):
    def test_explicit_ingestion_time_required(self):
        e = event(); del e["ingestion_time"]
        with self.assertRaises(EpisodeError): EpisodicLog().record(e)

    def test_shapes_and_unknown_fields_refused(self):
        for e in ([], None, "event", event(extra=1), event(seq=1), event(tombstoned=False), event(link_digest="x")):
            with self.assertRaises(EpisodeError): EpisodicLog().record(e)

    def test_exact_bounded_identity_and_source(self):
        for field, maximum in (("event_id", 128), ("scope", 128), ("actor", 128), ("source", 1024)):
            for value in (True, [], {}, "", " x", "x ", "x\n", "\ud800", "é"*(maximum//2+1)):
                with self.subTest(field=field, value=value), self.assertRaises(EpisodeError):
                    EpisodicLog().record(event(**{field: value}))

    def test_both_timestamps_are_bounded_exact_numbers(self):
        for field in ("event_time", "ingestion_time"):
            for value in (True, "1", float("nan"), float("inf"), 10**1000, -62135596801, 253402300800):
                with self.assertRaises(EpisodeError): EpisodicLog().record(event(**{field: value}))

    def test_skewed_times_do_not_reorder_submissions(self):
        log = EpisodicLog(); log.record(event("a", event_time=20, ingestion_time=10))
        log.record(event("b", event_time=-10, ingestion_time=-20))
        self.assertEqual([e["event_id"] for e in log.replay("s1")["events"]], ["a", "b"])

    def test_confidence_exact_finite_range(self):
        for value in (True, "1", None, -0.1, 1.1, float("nan"), float("inf"), 10**1000):
            with self.assertRaises(EpisodeError): EpisodicLog().record(event(confidence=value))
        for value in (0, 1, 0.0, 1.0):
            self.assertEqual(EpisodicLog().record(event(confidence=value))["seq"], 1)

    def test_payload_strict_types(self):
        for value in ([], None, {1: "x"}, {"x": (1, 2)}, {"x": 2**53}, {"x": object()}, {"x": "\ud800"}):
            with self.assertRaises(EpisodeError): EpisodicLog().record(event(payload=value))
        log = EpisodicLog(); log.record(event(payload={}))
        self.assertEqual(log.replay("s1")["events"][0]["payload"], {})

    def test_payload_depth_count_bytes_and_cycles(self):
        cycle = {}; cycle["self"] = cycle
        deep = {}
        for _ in range(18): deep = {"x": deep}
        for value in (cycle, deep, {"x": [0]*10001}, {"x": "a"*65536}, {"x": "é"*12000}):
            with self.assertRaises(EpisodeError): EpisodicLog().record(event(payload=value))

    def test_refusal_does_not_create_scope_or_burn_id(self):
        log = EpisodicLog()
        with self.assertRaises(EpisodeError): log.record(event(scope="new", actor=[]))
        self.assertEqual(log.verify()["scopes"], 0)
        self.assertEqual(log.record(event(scope="new"))["ingestion_index"], 1)

    def test_replay_and_tombstone_argument_types(self):
        log = EpisodicLog(); log.record(event())
        for kwargs in ({"from_seq": True}, {"to_seq": False}, {"from_seq": 1.0}, {"to_seq": "1"}):
            with self.assertRaises(EpisodeError): log.replay("s1", **kwargs)
        for value in (None, [], " ", "x\n"):
            with self.assertRaises(EpisodeError): log.replay(value)
            with self.assertRaises(EpisodeError): log.tombstone_actor("s1", value)


class Chains(unittest.TestCase):
    def test_every_metadata_field_changes_link(self):
        original = EpisodicLog().record(event())["link_digest"]
        for change in ({"actor": "bob"}, {"source": "other"}, {"event_time": 12},
                       {"ingestion_time": 12}, {"scope": "s2"}, {"confidence": 0.6},
                       {"event_id": "other"}, {"payload": {"value": 2}}):
            self.assertNotEqual(EpisodicLog().record(event(**change))["link_digest"], original)

    def test_global_ingestion_order_is_bound_across_scopes(self):
        log = EpisodicLog(); a = log.record(event(scope="s1")); b = log.record(event(scope="s2"))
        c = log.record(event("e2", scope="s1"))
        self.assertEqual([a["ingestion_index"], b["ingestion_index"], c["ingestion_index"]], [1, 2, 3])
        self.assertEqual([a["seq"], b["seq"], c["seq"]], [1, 1, 2])
        self.assertEqual(log.verify()["verdict"], "PASS")

    def test_metadata_tampering_blocks_reads_and_writes(self):
        for field, value in (("actor", "bob"), ("source", "other"), ("event_time", 12),
                             ("ingestion_time", 13), ("scope", "s2"), ("confidence", 0.9),
                             ("ingestion_index", 2), ("schema", "old")):
            log = EpisodicLog(); log.record(event()); log._scopes["s1"][0][field] = value
            self.assertEqual(log.verify()["verdict"], "FAIL")
            for action in (lambda: log.record(event("e2")), lambda: log.replay("s1"),
                           lambda: log.tombstone_actor("s1", "alice"), lambda: log.ledger):
                with self.assertRaises(IntegrityError): action()

    def test_suffix_removal_cannot_pass_verification(self):
        log = EpisodicLog(); log.record(event()); log.record(event("e2"))
        log._scopes["s1"].pop()
        self.assertEqual(log.verify()["verdict"], "FAIL")
        with self.assertRaises(IntegrityError): log.replay("s1")

    def test_seen_ledger_counters_and_registry_are_bound(self):
        for mutate in (lambda l: l._seen["s1"].clear(), lambda l: l._ledger.clear(),
                       lambda l: setattr(l, "_ingest_n", 10**1000),
                       lambda l: setattr(l, "_payload_bytes", 0),
                       lambda l: l._blocked.add(("s1", "alice"))):
            log = EpisodicLog(); log.record(event()); mutate(log)
            self.assertEqual(log.verify()["verdict"], "FAIL")

    def test_malformed_record_or_missing_state_fails_closed(self):
        for replacement in (None, [], {}, {"tombstoned": False}):
            log = EpisodicLog(); log.record(event()); log._scopes["s1"][0] = replacement
            self.assertEqual(log.verify()["verdict"], "FAIL")
            with self.assertRaises(IntegrityError): log.replay("s1")
        log = EpisodicLog(); del log._scopes
        self.assertEqual(log.verify()["verdict"], "FAIL")

    def test_ledger_event_digests_and_detachment(self):
        log = EpisodicLog(); log.record(event()); log.tombstone_actor("s1", "alice")
        for seq, e in enumerate(log.ledger, 1):
            self.assertEqual(e["ledger_seq"], seq)
            self.assertEqual(e["event_digest"], _digest({k: v for k, v in e.items() if k != "event_digest"}))
        log.ledger[0]["scope"] = "evil"
        self.assertEqual(log.verify()["verdict"], "PASS")


class Replay(unittest.TestCase):
    def test_range_anchors_allow_chain_recomputation(self):
        log = EpisodicLog()
        for i in range(4): log.record(event(str(i)))
        rep = log.replay("s1", 2, 3)
        previous = rep["previous_link"]
        for e in rep["events"]:
            body = {k: v for k, v in e.items() if k not in ("payload", "redacted", "link_digest")}
            self.assertEqual(e["link_digest"], _digest({"previous": previous, "event": body}))
            previous = e["link_digest"]
        self.assertEqual(previous, rep["range_head"])

    def test_digest_binds_entire_replay_envelope(self):
        log = EpisodicLog(); log.record(event()); rep = log.replay("s1")
        self.assertEqual(rep["replay_digest"], _digest({k: v for k, v in rep.items() if k != "replay_digest"}))
        rep["range"] = [2, 2]
        self.assertNotEqual(rep["replay_digest"], _digest({k: v for k, v in rep.items() if k != "replay_digest"}))

    def test_event_range_cap_refuses_instead_of_truncating(self):
        log = EpisodicLog()
        for i in range(3): log.record(event(str(i)))
        with patch("n03.core.MAX_REPLAY_EVENTS", 2):
            with self.assertRaises(EpisodeError): log.replay("s1")
            self.assertEqual(len(log.replay("s1", 2, 3)["events"]), 2)

    def test_byte_cap_refuses_instead_of_truncating(self):
        log = EpisodicLog()
        for i in range(2): log.record(event(str(i), payload={"large": "x"*3000}))
        with patch("n03.core.MAX_REPLAY_BYTES", 8500):
            with self.assertRaises(EpisodeError): log.replay("s1")
            rep = log.replay("s1", 1, 1)
            self.assertEqual(len(rep["events"]), 1)
            self.assertLess(len(json.dumps(rep).encode()), 8500)

    def test_unchanged_range_stays_stable_after_unrelated_append(self):
        log = EpisodicLog(); log.record(event()); before = log.replay("s1", 1, 1)
        log.record(event("e2"))
        self.assertEqual(before, log.replay("s1", 1, 1))

    def test_scope_is_not_implicit_authentication(self):
        log = EpisodicLog(); log.record(event(scope="other"))
        self.assertEqual(log.replay("other")["authority"], "evidence-only")


class Redaction(unittest.TestCase):
    def test_tombstone_retains_links_metadata_and_blocks_future_payloads(self):
        log = EpisodicLog(); log.record(event())
        before = log.replay("s1")["events"][0]
        result = log.tombstone_actor("s1", "alice")
        after = log.replay("s1")["events"][0]
        self.assertTrue(result["future_records_blocked"])
        self.assertIsNone(after["payload"]); self.assertTrue(after["redacted"])
        for key in ("link_digest", "payload_digest", "actor", "source", "event_time", "ingestion_time"):
            self.assertEqual(before[key], after[key])
        with self.assertRaises(EpisodeError): log.record(event("e2"))
        self.assertEqual(log.verify()["events"], 1)

    def test_redaction_does_not_erase_previously_returned_copy(self):
        log = EpisodicLog(); log.record(event()); old_copy = log.replay("s1")
        log.tombstone_actor("s1", "alice")
        self.assertEqual(old_copy["events"][0]["payload"], {"value": 1})

    def test_tombstone_is_scope_specific(self):
        log = EpisodicLog(); log.record(event()); log.record(event(scope="s2"))
        log.tombstone_actor("s1", "alice")
        self.assertFalse(log.replay("s2")["events"][0]["redacted"])
        self.assertEqual(log.record(event("e2", scope="s2"))["seq"], 2)

    def test_other_actor_remains_writable(self):
        log = EpisodicLog(); log.record(event()); log.tombstone_actor("s1", "alice")
        self.assertEqual(log.record(event("e2", actor="bob"))["seq"], 2)
        self.assertEqual(log.verify()["verdict"], "PASS")

    def test_repeated_tombstone_is_idempotent_without_new_ledger_entry(self):
        log = EpisodicLog(); log.record(event()); log.tombstone_actor("s1", "alice")
        before = log.ledger
        self.assertEqual(log.tombstone_actor("s1", "alice")["tombstoned"], 0)
        self.assertEqual(log.ledger, before)

    def test_unknown_actor_or_scope_refused_without_mutation(self):
        log = EpisodicLog(); log.record(event()); before = log.ledger
        for scope, actor in (("s1", "unknown"), ("missing", "alice")):
            with self.assertRaises(EpisodeError): log.tombstone_actor(scope, actor)
        self.assertEqual(log.ledger, before)

    def test_payload_and_event_capacity_full_still_allows_redaction(self):
        log = EpisodicLog()
        with patch("n03.core.MAX_EVENTS", 1), patch("n03.core.MAX_PAYLOAD_BYTES", 2):
            log.record(event(payload={}))
            with self.assertRaises(EpisodeError): log.record(event("e2"))
            self.assertEqual(log.tombstone_actor("s1", "alice")["tombstoned"], 1)
            self.assertEqual(log._payload_bytes, 0)
            self.assertEqual(log.verify()["verdict"], "PASS")

    def test_redaction_frees_payload_bytes_but_keeps_event_ids(self):
        log = EpisodicLog()
        with patch("n03.core.MAX_PAYLOAD_BYTES", 2):
            log.record(event(payload={}))
            log.tombstone_actor("s1", "alice")
            with self.assertRaises(EpisodeError): log.record(event(actor="bob", payload={}))
            self.assertEqual(log.record(event("e2", actor="bob", payload={}))["seq"], 2)

    def test_unauthorized_private_redaction_and_restore_detected(self):
        for change in ({"tombstoned": True, "payload": None}, {"tombstoned": 1}):
            log = EpisodicLog(); log.record(event()); log._scopes["s1"][0].update(change)
            self.assertEqual(log.verify()["verdict"], "FAIL")
        log = EpisodicLog(); log.record(event()); log.tombstone_actor("s1", "alice")
        log._scopes["s1"][0]["payload"] = {"value": 1}
        self.assertEqual(log.verify()["verdict"], "FAIL")

    def test_redaction_changes_replay_digest_without_changing_chain_head(self):
        log = EpisodicLog(); log.record(event()); before = log.replay("s1")
        log.tombstone_actor("s1", "alice"); after = log.replay("s1")
        self.assertEqual(before["range_head"], after["range_head"])
        self.assertNotEqual(before["replay_digest"], after["replay_digest"])


class CapacityConcurrency(unittest.TestCase):
    def test_scope_and_payload_capacity_are_atomic(self):
        for variable, maximum in (("MAX_SCOPES", 1), ("MAX_PAYLOAD_BYTES", 11)):
            log = EpisodicLog(); log.record(event()); before = log.ledger
            with patch("n03.core."+variable, maximum), self.assertRaises(EpisodeError):
                log.record(event("other", scope="s2"))
            self.assertEqual(log.ledger, before); self.assertEqual(log.verify()["scopes"], 1)
            self.assertEqual(log.record(event("other", scope="s2"))["ingestion_index"], 2)

    def test_concurrent_records_have_unique_contiguous_indices(self):
        log = EpisodicLog()
        with ThreadPoolExecutor(max_workers=4) as pool:
            receipts = list(pool.map(lambda i: log.record(event(str(i))), range(40)))
        self.assertEqual(sorted(r["seq"] for r in receipts), list(range(1, 41)))
        self.assertEqual(sorted(r["ingestion_index"] for r in receipts), list(range(1, 41)))
        self.assertEqual(log.verify()["verdict"], "PASS")

    def test_duplicate_race_only_accepts_one(self):
        log = EpisodicLog()
        def record(_):
            try: log.record(event()); return "OK"
            except EpisodeError: return "REFUSED"
        with ThreadPoolExecutor(max_workers=4) as pool: results = list(pool.map(record, range(4)))
        self.assertEqual(results.count("OK"), 1)
        self.assertEqual(log.verify()["events"], 1)

    def test_record_redact_race_never_leaves_live_actor_payload(self):
        log = EpisodicLog(); log.record(event())
        def record(_):
            try: log.record(event("e2"))
            except EpisodeError: pass
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(record, 0), pool.submit(log.tombstone_actor, "s1", "alice")]
            for f in futures: f.result()
        self.assertTrue(all(e["redacted"] and e["payload"] is None for e in log.replay("s1")["events"]))
        self.assertEqual(log.verify()["verdict"], "PASS")
