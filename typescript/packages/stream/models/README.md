# Real TypeScript model controls

The Rust models replay external event orders against the packaged TypeScript
and WASM implementation. Follow the preparation instructions in
`CONTRIBUTING.md`, then select the package with
`cargo nextest run --locked -p acyclic-stream-typescript-models`.

Required response fields use one checked accessor. An absent field must fail
with its name and original response, including fields whose expected value is
JSON `null`. Existing semantic negative controls require their original assertion
messages, so preparation errors cannot satisfy them.

The retained producer mutation removes `siblingError` from the real deadline
driver. With it applied, this original positive control must fail with
`InvalidData` and the missing field name:

```sh
git apply typescript/packages/stream/models/mutations/omit-sibling-error.patch
cargo test --locked -p acyclic-stream-typescript-models --test main session_model::deadline_cancels_a_blocked_subscription_write -- --exact
git apply -R typescript/packages/stream/models/mutations/omit-sibling-error.patch
```
