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
checks terminal/binding invariants. Replay requires every original field to
survive decoding. Its negative controls remove each of eighteen production
admission guards separately, then plant refusal of a valid original receipt;
each mutation must fail the same replay/fuzz oracle.

The five `linux-{refused,exit,input,eof,failed}-v3.json` samples are exact bytes
from the production writer exercised by
`original_host_phases_preserve_root_status_and_control_custody` on Linux x86_64,
2026-11-03. Capture source: `7498a743c7bae4e531feee7db1d4c7e7a0848cc4`;
the production writer is unchanged from the producer revision above. The
control prints the byte array read from the original file before decoding it.
These cover original missing-command refusal, status 124, control input/EOF
cancellation with signal 9, and control-read failure with original OS error 21.
No fields, byte order or line endings were rewritten.

| Sample | SHA-256 |
| --- | --- |
| refused | `83ce315823443eec431f653efff31021efb8788b3d32dc5e233c890dee6632b1` |
| exit | `8adf254273d74fc851f93c77c88fed08d0e64bc57f210a3ff8c1f0e7b19f02f5` |
| input | `2f25322bcb9b33974a05865e0b664055531e6657bb648f9e520f903fe54a489b` |
| eof | `f360684ce49a07b654790fec7f434d478afeaeceffd964e472f787a7b6c6a888` |
| failed | `5a258a546433c041f1cdfbf8e86e10170e9e7fc3ea37b4dd3697aad2cc390481b` |

The replay now removes eighteen production admission guards independently,
including the failure phase, error kind and OS-error guards, and rejects a
nineteenth defect that refuses valid original receipts. Broader platform and
state-machine qualification remains separate from this bounded byte campaign.

`windows-v3.json` is the exact terminal written during the real Windows x86_64
`selected Windows owner requires its original bound physical completion`
control on 2026-11-04. The original Node root exits with status 17. Its bytes
were retained before the invocation/custody corruption controls; no field or
line ending was rewritten. This is a new v3 capture, separate from prior v2
records. Producer executable SHA-256:
`290ffd46a5d062449a6396e8462891a09062976cbeea90d2b6b1b146666be430`.
Its original compilation receipt binds `external_host/receipt.rs` to
`e0a1ee0441115dda2c173e43798e9ecefd5325fab35c48369f1aa034b960b6a9`
and `external_host/arguments.rs` to
`0ad316c18685d7d3a5fca3be22f6d4ec41f5527d0a049133fa78ea851fe60f08`;
both match the captured producer source. Sample SHA-256:
`5c7e337a7e423117e8c846621c8796a04c8fa410e661ee05e91f0eb29ca59735`.

The shared decoder also compares the original host close when supplied. The
retained oracle detects omission of this comparison using every original
receipt, while the real Windows consumer control checks both launch paths,
unsigned native status, missing/wrong/incomplete completion and root refusal.
These are component controls, not qualification of installed host journeys.
