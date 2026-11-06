# Original typed failure codec

Run `python3 rust/proofs/qualify.py guest-failure`.

Invocation replies and failed-input metadata share `GuestError::encode/decode`.
The retained mutation admits the original bytes but replaces their typed cause.
The unchanged real LocalStream invocation property must reject that reassignment;
restored production preserves the explicit policy value, original receipt and
replay, co-committed consumed cursor and checkpoint across close and reopen.

The existing `actor_codecs` fuzz family calls this same decoder. Its production
writer seeds cover every cause variant, including full-width policy values.
`capture --only-actors --extend-goldens` retains those writer bytes and generates
the authoring declarations through the same Actors capture owner.

Canonical decoding still performs one JSON parse and one serialization comparison.
Both publications retain their existing one serialization per stored cause; a
failure record consumes the resulting String's bytes without copying. No wait,
timer, syscall, durability barrier or history-dependent work is introduced.
