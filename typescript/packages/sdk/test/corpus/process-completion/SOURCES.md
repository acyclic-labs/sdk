`linux-v3.json` is the exact v3 completion emitted by the shipped
`acyclic-process-host` during the real Node bridge integration control on
Linux x86_64, 2026-11-03. The command exits with status 17. The control records
the original bytes before removing its temporary receipt; no fields were
rewritten. This corpus covers successful physical retirement of a nonzero exit.
Producer source: `45cdb2d75075e3604f7751a4b20c24a7a7a24eea`.
SHA-256: `9b18dc4a3e4d3063d98d645bc45563ebd7489dd64d0829229eec6755a12b6fb0`.

Replay and measure decoder coverage with:

```sh
node --test --experimental-test-coverage test/process-completion.test.mjs
```

The retained fuzz entry consumes arbitrary bytes, mutates original fields and
checks terminal/binding invariants. Its negative control runs the production
decoder with invocation binding removed and requires the same oracle to fail.
Native refusal and cancellation wire captures remain additional corpus work;
the real-binary integration control independently exercises those paths.
