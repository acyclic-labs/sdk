# Projection cost

```sh
python3 rust/proofs/qualify.py projection-cost
```

The retained production property uses the actual v1 protocol golden and v2
`AppendRecord::changes` writer. It checks marker and body-pass counters and actual
heap allocations across the original malformed marker, body and index matrix.
Zero-allocation assertions cover valid records; malformed compact varints can
allocate the original Prost decoding error before rejection.
Matching, preservation and projection must agree, and delivered records retain
identity and attributes. Separate signed-provider conformance retains authority
and transport assertions.

The mutation removes the shared production whole-record ownership shortcut,
including unrestricted projection.
The unchanged allocation assertion must fail, then byte-identical source restoration
must pass the same property. No historical implementation copy supplies a result
or a cost baseline. The same property also checks partial input admission and
discarded payload custody; its separate retained control is described in
[`partial-projection-cost`](../partial-projection-cost/README.md). Complete
physical cost and asynchronous progress remain outside this component oracle.
