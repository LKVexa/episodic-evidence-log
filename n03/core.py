"""Bounded in-memory event evidence with metadata-bound chains and payload redaction."""
from __future__ import annotations
import copy
import hashlib
import json
import math
import threading

VERSION = "0.1.2a1"
__version__ = VERSION
MAX_EVENTS = 1000
MAX_SCOPES = 64
MAX_PAYLOAD_BYTES = 16777216
MAX_REPLAY_EVENTS = 250
MAX_REPLAY_BYTES = 1048576
REQUIRED = ("event_id", "actor", "source", "event_time", "ingestion_time", "scope", "confidence", "payload")
CHAIN_FIELDS = ("schema", "event_id", "actor", "source", "event_time", "ingestion_time", "scope", "confidence", "seq", "ingestion_index", "payload_digest")

class EpisodeError(Exception):
    pass

class IntegrityError(EpisodeError):
    pass

def _canonical(obj):
    try:
        return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=True, allow_nan=False).encode()
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise EpisodeError("value is not strictly canonical JSON") from exc


def _digest(obj):
    return "sha256:" + hashlib.sha256(_canonical(obj)).hexdigest()


def _label(value, field, maximum=256):
    if (type(value) is not str or not value or value != value.strip()
            or len(value) > maximum or any(ord(c) < 32 or ord(c) == 127 for c in value)):
        raise EpisodeError(field + " must be bounded nonblank control-free text")
    try:
        if len(value.encode()) > maximum:
            raise EpisodeError(field + " exceeds byte bounds")
    except UnicodeError as exc:
        raise EpisodeError(field + " contains invalid Unicode") from exc
    return value


def _time(value, field):
    if (type(value) not in (int, float) or not -62135596800 <= value <= 253402300799
            or not math.isfinite(value)):
        raise EpisodeError(field + " must be finite Unix seconds in years 1..9999")
    return value


def _value(value):
    stack = [(value, 0)]
    count = text_bytes = 0
    while stack:
        item, depth = stack.pop()
        count += 1
        if depth > 16 or count > 10000:
            raise EpisodeError("value exceeds depth/count bounds or contains a cycle")
        kind = type(item)
        if kind is dict:
            if len(item) > 10000 or any(type(k) is not str for k in item):
                raise EpisodeError("JSON keys must be strings")
            stack.extend((v, depth+1) for v in item.values())
            stack.extend((k, depth+1) for k in item)
        elif kind is list:
            if len(item) > 10000:
                raise EpisodeError("value exceeds count bounds")
            stack.extend((v, depth+1) for v in item)
        elif kind is str:
            if len(item) > 65536:
                raise EpisodeError("value text exceeds byte bounds")
            try:
                text_bytes += len(item.encode())
            except UnicodeError as exc:
                raise EpisodeError("value contains invalid Unicode") from exc
            if text_bytes > 65536:
                raise EpisodeError("value exceeds byte bounds")
        elif kind is int:
            if abs(item) > 2**53-1:
                raise EpisodeError("integer exceeds exact JSON number range")
        elif kind is float:
            if not math.isfinite(item):
                raise EpisodeError("nonfinite JSON number")
        elif item is not None and kind is not bool:
            raise EpisodeError("non-JSON value")
    if len(_canonical(value)) > 65536:
        raise EpisodeError("canonical value exceeds 64 KiB")
    return copy.deepcopy(value)


def _event(event):
    if type(event) is not dict or set(event) != set(REQUIRED):
        raise EpisodeError("event must contain exactly the required fields, including ingestion_time")
    clean = {k: _label(event[k], k, 1024 if k == "source" else 128)
             for k in ("event_id", "actor", "source", "scope")}
    for k in ("event_time", "ingestion_time"):
        clean[k] = _time(event[k], k)
    conf = event["confidence"]
    if type(conf) not in (int, float) or not 0 <= conf <= 1 or not math.isfinite(conf):
        raise EpisodeError("confidence must be a finite number in [0, 1]")
    clean["confidence"] = conf
    if type(event["payload"]) is not dict:
        raise EpisodeError("payload must be an exact JSON object")
    clean["payload"] = _value(event["payload"])
    return clean


def _genesis(scope):
    return _digest({"schema": "n03/genesis/v2", "scope": scope})


def _link(previous, rec):
    return _digest({"previous": previous, "event": {k: rec[k] for k in CHAIN_FIELDS}})


def _seal(scopes, seen, blocked, ingest_n, payload_bytes, ledger):
    return _digest({"scopes": scopes, "seen": {k: sorted(v) for k, v in seen.items()},
                    "blocked": sorted([list(p) for p in blocked]), "ingest_n": ingest_n,
                    "payload_bytes": payload_bytes, "ledger": ledger})


def _ledger_event(ledger, **event):
    event["ledger_seq"] = len(ledger)+1
    event["event_digest"] = _digest(event)
    return event


class EpisodicLog:
    def __init__(self):
        self._scopes, self._seen = {}, {}
        self._blocked = set()
        self._ingest_n = self._payload_bytes = 0
        self._ledger = []
        self._lock = threading.RLock()
        self._seal = self._state_hash()

    def _state_hash(self):
        return _seal(self._scopes, self._seen, self._blocked, self._ingest_n, self._payload_bytes, self._ledger)

    @property
    def ledger(self):
        with self._lock:
            self._require_integrity()
            return copy.deepcopy(self._ledger)

    def _problems(self):
        problems = []
        try:
            indices, actual_seen, actor_pairs = [], {}, set()
            payload_bytes = 0
            for scope, chain in self._scopes.items():
                previous = _genesis(scope)
                actual_seen[scope] = set()
                if not chain:
                    problems.append({"scope": scope, "issue": "empty scope"})
                for seq, rec in enumerate(chain, 1):
                    if type(rec["tombstoned"]) is not bool:
                        problems.append({"scope": scope, "seq": seq, "issue": "invalid redaction flag"})
                    if not rec["tombstoned"]:
                        if _digest(rec["payload"]) != rec["payload_digest"]:
                            problems.append({"scope": scope, "seq": seq, "issue": "payload tampered"})
                        payload_bytes += len(_canonical(rec["payload"]))
                    elif rec["payload"] is not None:
                        problems.append({"scope": scope, "seq": seq, "issue": "redacted payload retained"})
                    if _link(previous, rec) != rec["link_digest"]:
                        problems.append({"scope": scope, "seq": seq, "issue": "chain link broken"})
                    if type(rec["seq"]) is not int or rec["seq"] != seq:
                        problems.append({"scope": scope, "issue": "sequence gap"})
                    if (rec["scope"] != scope or rec["schema"] != "n03/event/v2"
                            or set(rec) != set(CHAIN_FIELDS) | {"payload", "tombstoned", "link_digest"}):
                        problems.append({"scope": scope, "seq": seq, "issue": "invalid event metadata"})
                    if rec["event_id"] in actual_seen[scope]:
                        problems.append({"scope": scope, "seq": seq, "issue": "duplicate event ID"})
                    actual_seen[scope].add(rec["event_id"])
                    if type(rec["ingestion_index"]) is not int:
                        problems.append({"scope": scope, "seq": seq, "issue": "invalid ingestion index"})
                    indices.append(rec["ingestion_index"])
                    pair = (scope, rec["actor"])
                    actor_pairs.add(pair)
                    if rec["tombstoned"] != (pair in self._blocked):
                        problems.append({"scope": scope, "seq": seq, "issue": "redaction registry mismatch"})
                    previous = rec["link_digest"]
            if (type(self._ingest_n) is not int or not 0 <= self._ingest_n <= MAX_EVENTS
                    or sorted(indices) != list(range(1, self._ingest_n+1))
                    or actual_seen != self._seen or not self._blocked <= actor_pairs
                    or self._payload_bytes != payload_bytes):
                problems.append({"issue": "state index or counter mismatch"})
            for seq, event in enumerate(self._ledger, 1):
                if (type(event["ledger_seq"]) is not int or event["ledger_seq"] != seq
                        or event["event_digest"] != _digest({k: v for k, v in event.items() if k != "event_digest"})):
                    problems.append({"issue": "ledger tampered"})
            if len(self._ledger) != self._ingest_n + len(self._blocked) or self._state_hash() != self._seal:
                problems.append({"issue": "state tampered"})
        except (EpisodeError, KeyError, TypeError, AttributeError, ValueError):
            problems.append({"issue": "state tampered"})
        return problems

    def _require_integrity(self):
        if self._problems():
            raise IntegrityError("episodic log integrity failed")

    def record(self, event):
        clean = _event(event)
        with self._lock:
            self._require_integrity()
            scope = clean["scope"]
            if (scope, clean["actor"]) in self._blocked:
                raise EpisodeError("actor is tombstoned in this scope; further payloads refused")
            if clean["event_id"] in self._seen.get(scope, set()):
                raise EpisodeError("duplicate event ID: log is append-only")
            if self._ingest_n >= MAX_EVENTS or scope not in self._scopes and len(self._scopes) >= MAX_SCOPES:
                raise EpisodeError("event or scope capacity exceeded")
            size = len(_canonical(clean["payload"]))
            payload_bytes = self._payload_bytes + size
            if payload_bytes > MAX_PAYLOAD_BYTES:
                raise EpisodeError("retained payloads exceed 16 MiB")
            chain = self._scopes.get(scope, [])
            rec = {"schema": "n03/event/v2", **clean, "seq": len(chain)+1,
                   "ingestion_index": self._ingest_n+1, "payload_digest": _digest(clean["payload"]),
                   "tombstoned": False}
            rec["link_digest"] = _link(chain[-1]["link_digest"] if chain else _genesis(scope), rec)
            scopes = dict(self._scopes, **{scope: chain + [rec]})
            seen = dict(self._seen, **{scope: self._seen.get(scope, set()) | {clean["event_id"]}})
            ledger = self._ledger + [_ledger_event(self._ledger, op="recorded", scope=scope,
                       event_id=rec["event_id"], seq=rec["seq"], link_digest=rec["link_digest"])]
            seal = _seal(scopes, seen, self._blocked, self._ingest_n+1, payload_bytes, ledger)
            self._scopes, self._seen, self._ledger = scopes, seen, ledger
            self._ingest_n += 1
            self._payload_bytes, self._seal = payload_bytes, seal
            return {k: rec[k] for k in ("event_id", "scope", "seq", "ingestion_index", "link_digest")}

    def replay(self, scope, from_seq=1, to_seq=None):
        scope = _label(scope, "scope", 128)
        if type(from_seq) is not int or to_seq is not None and type(to_seq) is not int:
            raise EpisodeError("from_seq and to_seq must be exact integers")
        with self._lock:
            self._require_integrity()
            chain = self._scopes.get(scope)
            if chain is None:
                raise EpisodeError("unknown ordering scope")
            end = len(chain) if to_seq is None else to_seq
            if not 1 <= from_seq <= end <= len(chain):
                raise EpisodeError("range outside recorded window: refusing to invent events")
            if end - from_seq + 1 > MAX_REPLAY_EVENTS:
                raise EpisodeError("replay exceeds 250 events; request smaller explicit ranges")
            events, size = [], 4096
            for rec in chain[from_seq-1:end]:
                size += len(_canonical(rec)) + 1
                if size > MAX_REPLAY_BYTES:
                    raise EpisodeError("replay exceeds 1 MiB; request a smaller explicit range")
                out = copy.deepcopy(rec)
                out["redacted"] = out.pop("tombstoned")
                events.append(out)
            result = {"schema": "n03/replay/v2", "scope": scope, "range": [from_seq, end],
                      "previous_link": chain[from_seq-2]["link_digest"] if from_seq > 1 else _genesis(scope),
                      "range_head": chain[end-1]["link_digest"], "events": events,
                      "authority": "evidence-only",
                      "note": "replay reproduces recorded events only; omitted experiences and actions are never invented"}
            result["replay_digest"] = _digest(result)
            return result

    def tombstone_actor(self, scope, actor):
        scope = _label(scope, "scope", 128)
        actor = _label(actor, "actor", 128)
        with self._lock:
            self._require_integrity()
            chain = self._scopes.get(scope)
            if chain is None or not any(r["actor"] == actor for r in chain):
                raise EpisodeError("actor unavailable in this scope")
            if (scope, actor) in self._blocked:
                return {"scope": scope, "actor": actor, "tombstoned": 0, "future_records_blocked": True}
            seqs = [r["seq"] for r in chain if r["actor"] == actor]
            erased_bytes = sum(len(_canonical(r["payload"])) for r in chain if r["actor"] == actor)
            replacement = [dict(r, payload=None, tombstoned=True) if r["actor"] == actor else r for r in chain]
            scopes = dict(self._scopes, **{scope: replacement})
            blocked = self._blocked | {(scope, actor)}
            ledger = self._ledger + [_ledger_event(self._ledger, op="tombstoned", scope=scope,
                         actor=actor, event_seqs=seqs, scope_head=chain[-1]["link_digest"])]
            payload_bytes = self._payload_bytes - erased_bytes
            seal = _seal(scopes, self._seen, blocked, self._ingest_n, payload_bytes, ledger)
            self._scopes, self._blocked, self._ledger = scopes, blocked, ledger
            self._payload_bytes, self._seal = payload_bytes, seal
            return {"scope": scope, "actor": actor, "tombstoned": len(seqs), "future_records_blocked": True}

    def verify(self):
        with self._lock:
            problems = self._problems()
            try:
                scopes = len(self._scopes)
                events = sum(len(chain) for chain in self._scopes.values())
                ledger_length = len(self._ledger)
            except (TypeError, AttributeError):
                scopes = events = ledger_length = None
            return {"schema": "n03/integrity/v2", "scopes": scopes, "events": events,
                    "ledger_length": ledger_length, "problems": problems,
                    "verdict": "FAIL" if problems else "PASS"}
