# Security boundaries

This is a trusted coordinator's bounded in-memory log. Actor/scope/source labels,
confidence and timestamps are unverified caller data. Scope selection is not
authorization; any object holder can replay or redact, and administrative views
cover all scopes. Enforce access separately before exposing a service API.

Unsigned hashes detect consistency changes, not a process owner rewriting complete
state and hashes. Submission order is not proof of real-world causality. No durable
storage, externally anchored audit trail, freshness or rollback protection exists.
Payload text is untrusted evidence, never executed or granted instruction authority.

Tombstoning removes only the store's current payload references and blocks future
records for that actor/scope pair. Metadata and hashes remain, including potential
identifiers and low-entropy payload fingerprints. Previously returned copies and
external storage remain outside the operation. It is not secure memory wiping,
complete erasure or a legal deletion-compliance mechanism.

Capacity and replay bounds describe accepted normal work, not OS memory/time limits.
Verification/hashing costs grow with retained data. No independent security audit,
build-tool vulnerability scan, performance/soak qualification or full original
certification is claimed.
