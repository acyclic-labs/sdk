# Admitted change decoding cost

```sh
node rust/proofs/qualify.mjs changes-decode-cost
```

The existing decoder property uses the actual v1 protocol golden and v2
`AppendRecord::changes` output at its original entry counts. It compares decoded
values with those producers and measures the production decoder directly.
Typed decoding retains every allocated byte in its output rather than growing
and discarding intermediate storage. Opaque header indexes cannot change the
allocation cost. Invalid input has the exact refusal and cost of the production
validator, including its original decoding-error allocation.

The retained mutation removes preallocation from the actual decoder. The same
physical allocation assertion must fail; byte-identical restoration must pass.
No historical decoder copy supplies an oracle. This synchronous component check
does not qualify transport progress, partial projection or full-platform costs.
