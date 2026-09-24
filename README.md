# Episodic Evidence Log

**0.1.2a1 — experimental partial candidate, JY-S039-P001 / N03**

A bounded in-memory event log with per-scope hash chains, exact range replay,
explicit payload redaction and caller-declared actor/source/timestamps/confidence.
Python 3.10+; no runtime dependencies, network operations or file persistence.

## Use

~~~sh
python -m pip install .
python -m unittest discover -s tests -t .
~~~

~~~python
from n03.core import EpisodicLog

log = EpisodicLog()
log.record({
    "event_id": "e1", "actor": "worker-a", "source": "local-run",
    "scope": "session-1", "event_time": 100, "ingestion_time": 101,
    "confidence": 0.9, "payload": {"action": "prepared"},
})
evidence = log.replay("session-1", from_seq=1, to_seq=1)
log.tombstone_actor("session-1", "worker-a")
assert log.replay("session-1")["events"][0]["payload"] is None
assert log.verify()["verdict"] == "PASS"
~~~

## Events, time and ordering

record accepts exactly these fields: event_id, actor, source, event_time,
ingestion_time, scope, confidence and payload. ingestion_time is now mandatory;
it was promised but absent in the prior implementation. Both timestamps are
caller claims, not readings from a trusted clock. They must be exact int/float
values, finite and within -62135596800..253402300799 Unix seconds. Clock skew and
out-of-order timestamps are allowed and never reorder submitted events.

Event ID, actor and scope are exact case-sensitive strings of at most 128 UTF-8
bytes; source is at most 1024. Labels must be nonblank valid Unicode without
outer whitespace or C0/DEL controls. They are not authenticated identities.
Confidence is an exact int/float in [0, 1], excluding booleans and nonfinite values;
the numeric claim is not independently calibrated or verified.

Payload is an exact JSON object. Nested dictionaries require string keys; lists,
strings, booleans, null, finite floats and integers within -(2**53-1)..(2**53-1)
are supported. Custom types, tuples, non-string keys and invalid Unicode are
refused. Each payload has a 64 KiB canonical byte limit, depth 16 and 10000 visited
values/keys. Canonical JSON sorts keys, uses compact separators and ASCII escapes;
escaping counts toward byte limits. Raw JSON parsing and duplicate textual key
handling are caller responsibilities. Empty payload objects remain empty.

Per-scope seq and whole-log ingestion_index are contiguous submission counters.
They establish this object's accepted order, not real-world causality. Event IDs
are unique within a scope and remain reserved after redaction. The same ID may
occur in another scope. Refused records consume no ID, counter, scope or ledger
entry. Input payloads are copied before storage.

## Chains and integrity

Event schema n03/event/v2 binds schema, ID, scope, actor, source, both timestamps,
confidence, sequence, ingestion index and payload digest to the chain. Genesis
also binds its scope. A link is the SHA-256 digest of canonical JSON containing
the previous link and the complete immutable event descriptor.

Payload bodies are checked separately against payload_digest while retained.
Mutable redaction flags/bodies are deliberately outside immutable chain links;
they are covered by the complete state hash and redaction registry/ledger.
This permits redaction without rewriting historical links. The state hash also
binds seen-ID indexes, counters, all scopes and the full sequenced lifecycle ledger.
Lifecycle events have their own complete event_digest.

verify checks links, retained payloads, sequences, global ingestion indexes,
scope metadata, duplicate indexes, payload byte accounting, redaction registry,
ledger digests/counts and the state hash. Detected corruption blocks record,
replay, tombstone and public ledger access with IntegrityError (an EpisodeError
subclass). Suffix deletion is detected against retained state/indexes; this does
not provide rollback protection for an entire replaced store.

Hashes are unsigned consistency checks. A process owner can rewrite consistent
data and hashes. They do not authenticate sources, prove events happened, validate
confidence/timestamps or provide an externally anchored audit trail.

## Exact replay

replay(scope, from_seq=1, to_seq=None) returns schema n03/replay/v2 with exactly the
requested inclusive range. The default end is the current scope tail. Invalid
ranges, unknown scopes, sequence gaps and corrupted state are refused; omitted
events are never inferred or interpolated. Returned data is evidence-only and
detached from the store. Stored text remains untrusted, including any instructions.

Each call permits at most 250 events and 1 MiB canonical response bytes, including
conservative framing reserve. A larger range is refused in full; request smaller
explicit ranges to continue. Replay never silently truncates the requested range.
previous_link and range_head anchor the returned chain segment; each event includes
its link and payload digest. replay_digest binds the complete envelope, including
scope, range, anchors, metadata, redaction flags and bodies.

Identical requests against unchanged selected events give identical results,
even if unrelated later events are appended. Redaction changes replay output and
its digest while preserving original chain links. Separate range calls are not a
persistent snapshot, so intervening redaction can change results between calls.

## Payload redaction

tombstone_actor(scope, actor) removes the payload references for every currently
recorded event with that exact actor in that scope. It sets explicit redacted=true
and payload=null in replay and records affected sequence numbers in the lifecycle
ledger. The actor/scope pair becomes blocked: future records for that pair are
refused. There is no unblock API. Other actors/scopes remain usable.

Unknown actors/scopes are refused. Repeating a completed tombstone is idempotent,
returns tombstoned=0 and adds no new ledger entry. All accepted actor/scope pairs
can be tombstoned even at the event/payload capacity limit. Redaction frees counted
payload bytes, but retains event IDs and event/scope capacity. It is serialized
with recording so racing submissions cannot leave a live payload for a blocked pair.

This is limited payload redaction, not complete erasure or secure memory wiping.
Actor/source/timestamps/IDs/confidence, payload hashes, chain links and lifecycle
metadata remain. Hashes of low-entropy payloads can support guessing. Previously
returned copies, caller inputs, process-memory remnants, external logs and backups
are outside the operation. No legal deletion-compliance claim is made.

## Boundaries and capacity

The object belongs behind a trusted coordinator. Scope and actor arguments are
selection labels, not authorization. Any object holder can read/redact a scope;
the administrative ledger and verify summary cover the whole log. Enforce caller
permissions separately. No stored payload is executed by this library.

An RLock serializes supported operations. Limits are 1000 lifetime events, 64 scopes
and 16 MiB retained canonical payload bytes. Metadata/indexes/copies add overhead.
At most one tombstone event per recorded actor/scope pair is appended, so the
lifecycle ledger has at most twice as many entries as recorded events (2000).
Repeated no-op tombstones cannot exhaust it. No pruning, import, durable storage,
crash recovery or multiprocess coordination is provided. State hashing/verification
grows with retained data; these are normal-workload bounds, not OS resource quotas.
Do not mutate caller inputs concurrently during validation.

## Verification and migration

57 tests: 20 inherited plus 37 new regressions. Coverage includes complete metadata
binding, caller timestamp validation, range anchors/limits, suffix deletion,
registry/ledger/counter corruption, scoped persistent redaction, detached copies,
capacity atomicity and concurrent record/redaction races. [CHECK_RUNS](docs/CHECK_RUNS.json)
records source and installed-wheel evidence. CI covers Linux Python 3.10/3.12/3.14
and Windows 3.12. See [AUDIT](docs/AUDIT.md).

0.1.1-partial -> 0.1.2a1 requires ingestion_time and exact bounded event fields,
introduces v2 chains/replay, full-envelope replay hashes and persistent actor/scope
redaction blocks. Old/new hashes are incompatible. Callers must supply the new
timestamp, split large replay requests and stop submitting for tombstoned pairs.
Inherited fixtures gained ingestion_time and the updated version assertion.

Durability, tenant crypto binding, p50/p99 benchmarks, 72-hour soak and the original
audited checklist/R-packages/grouped gates remain outside this partial candidate.

## License

Copyright 2026 **RUSSELL PHILIP SMITHSON**.
[Apache License 2.0](LICENSE), with [NOTICE](NOTICE).
No third-party source is vendored; see [THIRD-PARTY-NOTICES](THIRD-PARTY-NOTICES.md).
