# Current hard-link facts

Run `node rust/proofs/qualify.mjs native-hard-link-detached-facts`.

The existing native lazy-source publication control opens one real source hard
link, optionally writes and promotes its sibling, unlinks the original name,
and writes through the retained handle. Its new link reply must preserve the
current detached length, metadata and identity together with the installed
binding count and original completion stamp. The new name reads the same bytes;
removing it restores the original orphan and publication checks.

The retained inverse restores the original authored-only link reply in the
production mutation callback. The unchanged control must fail its detached-facts
assertion before the restored callback passes. It introduces no decoder, wire
format, model, fixture, separate test or readiness wait.

All ordinary lookup, directory and link facts share `project_authored`. It is
synchronous because its wrapper has no suspension; the original detached file
snapshot owns its existing metered runtime invocation and state lock. The link
reply retains its existing mutation stamp and installed binding count. Its
final List check withholds optional facts if access is revoked during projection.
No namespace/history scan, payload copy, task, timer or durability barrier is
introduced. Credential verification and the original metadata read retain their
existing input-sensitive costs.
