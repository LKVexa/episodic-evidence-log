# Audit and hardening — 0.1.2a1

Date: 2026-09-23. Source: JY-S039-P001 / 0.1.1-partial / run-0001 / product.
Reviewed event schemas, ordering, chain binding, replay, redaction, integrity,
capacity and concurrency. Original source remains separate.

## Repaired findings

- Chain links omitted actor/source/timestamps/scope/confidence/ingestion index.
  Complete immutable v2 event descriptors and scoped genesis now bind every field.
- Documentation promised an ingestion timestamp but no such field was stored.
  Explicit caller-supplied ingestion_time is now required and validated alongside
  event_time; neither is a trusted clock or used to infer real-world causal order.
- Payload JSON, labels, timestamp types and unknown fields lacked strict bounds.
  Exact bounded schemas, nested JSON validation and safe numeric ranges now apply.
- Verification could miss suffix deletion, redaction flag changes, indexes and
  counters; replay could serve tampered payloads. Full-state checks, registry/index
  accounting and fail-closed read/write paths now cover these changes.
- Public ledger mutation and record/redaction races were possible. Detached views,
  prospective state replacement and an RLock serialize supported operations.
- Tombstoning let future payloads for the same actor return, and repeated no-op
  calls grew history indefinitely. Known actor/scope pairs now remain blocked;
  repeats are idempotent and no-op history does not grow.
- Replay/history/payload retention were unbounded. Explicit event/scope/payload
  caps and exact-range response limits apply, without preventing redaction of
  accepted actors at capacity. Refused records consume no IDs or counters.
- Deletion and append-only claims overstated the implementation. Immutable links
  coexist with mutable payload redaction; retained metadata/hashes, external copies,
  memory remnants and lack of durability are now explicit.

## Verification

20 baseline tests passed. Their fixture now provides required ingestion_time and
preserves an explicitly empty payload; version expectation is 0.1.2a1. The source
inspection fixture now uses closed UTF-8 Path reading rather than leaking a file.

All 57 source and installed-wheel tests pass, adding 37 regressions for full metadata
binding, strict types/budgets, chain segment anchors, complete replay receipts,
suffix/registry/counter/ledger tampering, payload-only redaction limits, capacity
atomicity, persistent scoped blocks and concurrent record/redaction races.
CHECK_RUNS.json records current evidence; BASELINE_CHECK_RUNS.json retains historical
evidence. CI covers Linux Python 3.10/3.12/3.14 and Windows 3.12.

No actual event/source verification, secure erasure test, durable/crash recovery,
throughput benchmark, 72-hour soak, independent security audit, build-tool
vulnerability scan or full original gate certification was performed. No third-party
runtime dependency required upgrading. JSON validation follows the explicit strict
choices documented in the primary [Python JSON documentation](https://docs.python.org/3/library/json.html);
raw JSON decoding remains the caller's responsibility.

Version advanced from 0.1.1-partial to 0.1.2a1, with v2 event/replay/integrity schemas.
Added packaging, pinned-action CI, README, security guidance and Apache 2.0
LICENSE/NOTICE naming RUSSELL PHILIP SMITHSON.
