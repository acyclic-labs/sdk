# @acyclic-labs/inference changelog

## 0.2.0 - Unreleased

- Align the unified SDK candidate with the breaking Objects v2 public package transition.

## Unreleased

- Reject idle-KV responses to legacy retain/renew requests; recovered inspection remains compatible with either retention mode.

- Add paid idle KV policy and verified-use evidence to the canonical v1 wire
  contract, with Rust and TypeScript retention and renewal helpers.
- Renewal changes timeout from the prior verified-use/initial-pin baseline;
  it does not reset idle time. No capacity, throughput or latency guarantee.
- Validate policy binding, exclusive legacy/idle fields, paired Run use evidence,
  and checked deadlines through the shared Rust/WASM contract.

## 0.1.5 - 2026-09-25

- Aligns inference clients with the qualified SDK 0.1.5 release.

## 0.1.4 - 2026-09-25

- Aligns inference clients with the qualified SDK 0.1.4 release.

## 0.1.3 - 2026-09-25

- Adds the OpenAI-compatible model adapter and OpenRouter dialect.

## 0.1.2 - 2026-09-25

- Aligns the inference contracts and client with the 0.1.2 SDK release.

## 0.1.1 - 2026-09-24

- First coordinated SDK release of immutable inference contexts and
  recoverable, event-streamed generation runs.
- Includes explicit warm commitments and generated protocol types.
