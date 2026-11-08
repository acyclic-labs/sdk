# Stream session fuzz contracts

Both targets inherit workspace lint policy. `native_session` uses the shared
Rust wire and follow-data oracles; `session` sends every input to the actual
TypeScript decoder and validator through the same process driver used by the
transport models. Prepare the real generated bindings and WASM artifacts as
described in [CONTRIBUTING.md](../../../../CONTRIBUTING.md).

The process driver returns contextual I/O errors for a lost producer, failed
write, invalid envelope or refused action. Its original stdin owns the model
process lifetime. The retained model controls exercise owner death, producer
loss, crash detection, exact process-wide budgets and renewal between runs.
The finite model step budget does not restrict fuzz input length.

`mutations/session-input-cutoff.patch` restores the old silent input cutoff.
With a frame longer than 4096 bytes and the existing real `startupCrash` fault,
the mutated target falsely succeeds without starting the model. Restoring the
target must report the actual producer-loss error from that same input. Normal
corpus replay then uses the unmodified TypeScript decoder and validator.

Check both retained targets with:

```sh
cargo clippy --locked -p acyclic-stream-typescript-fuzz --bins --no-deps -- -D warnings
```
