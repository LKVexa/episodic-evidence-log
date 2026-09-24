# 0.1.2a1 — 2026-09-23

- Require ingestion_time and bind full event metadata in scoped v2 chains.
- Fail closed on corruption of payloads, redactions, indexes, counters or ledger.
- Bound strict JSON, retention and exact replay ranges with complete receipts.
- Persist actor/scope redaction blocks and serialize record/redaction races.
- Add 37 regressions, packaging, CI and honest payload-redaction boundaries.
- Include README and Apache 2.0 LICENSE/NOTICE; full certification remains open.

# Changelog — JY-S039-P001 n03-episodic-memory

## 0.1.1-partial (2026-09-14) — maintenance run-0001 (audit A029)

Baseline fingerprint: build-0001 product.zip
sha256 188d4f657aea3a33708ab4685d795b661b6120700b38c07485d72a4e23065609
(16473 bytes), baseline version 0.1.0-partial, baseline tests 12/12 PASS.
All findings reproduced on the baseline with live probes before fixing.

### Fixes (repairs only — patch bump)

- **A029-F1 Payload aliasing (HIGH).** `record` stored the caller's
  payload dict by reference and `replay` returned references to stored
  payloads. Observed: mutating the caller's dict after `record`, or the
  `replay` result, silently rewrote the stored log and flipped
  `verify()` to FAIL ("payload tampered") with no actual tamper.
  Fixed: payloads are deep-copied on record and on replay output.
- **A029-F2 Error-contract leaks (MEDIUM).** Bare `TypeError`/
  `ValueError` escaped the documented `EpisodeError` contract for
  `confidence=None`/non-numeric, unhashable `event_id`/`scope`,
  non-JSON-serializable payloads, and non-int `to_seq` in `replay`.
  Fixed: explicit type validation (`event_id`/`scope` strings,
  `confidence` a non-bool finite number, `from_seq`/`to_seq` ints);
  every refusal is `EpisodeError`. Boolean confidence is now refused.
- **A029-F3 NaN canonicalization (MEDIUM).** Payloads containing
  NaN/Infinity were accepted; canonical digests were computed from
  non-JSON text ("NaN"), breaking strict round-trip canonicalization.
  Fixed: `allow_nan=False` in the canonical digest; such payloads are
  refused with `EpisodeError`.
- **A029-F4 Non-atomic record (MEDIUM).** A record refused mid-way
  (e.g. unserializable payload) had already registered the event_id
  and bumped the ingestion counter, so a corrected retry with the same
  event_id was falsely refused as a duplicate. Fixed: all validation
  and digest derivation happens before any state mutation.

### Compatibility

- No public API removed or renamed. New refusals only affect inputs
  that previously crashed with undocumented exceptions or corrupted
  invariants (bool confidence, NaN payloads).
- No baseline test asserted the old weaker behavior; all 12 baseline
  tests pass unchanged. 8 new regression tests added (20/20 PASS).

### Rollback

Restore build-0001 product.zip (sha256 above). No on-disk state
format exists (in-memory kernel), so rollback is file replacement only.
