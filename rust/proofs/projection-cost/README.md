# Projection cost

```sh
python3 rust/proofs/qualify.py projection-cost
```

The retained production property uses the actual v1 protocol golden and v2
`AppendRecord::changes` writer. It checks marker and body-pass counters and actual
heap allocations across the original malformed marker, body and index matrix.
Matching, preservation and projection must agree, and delivered records retain
identity and attributes. Separate signed-provider conformance retains authority
and transport assertions.

The mutation removes the production unrestricted ownership-transfer shortcut.
The unchanged allocation assertion must fail, then byte-identical source restoration
must pass the same property. No historical implementation copy supplies a result
or a cost baseline. Restricted projection allocations and complete physical cost
remain open; this property does not establish every filter's cost or progress.
