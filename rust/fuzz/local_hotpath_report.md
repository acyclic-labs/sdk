# Original report encoding control

The native benchmark's observation control serializes its original retained
sample and receipt backing. The fixture includes nested identity objects and
strings, built before measuring direct encoding. It requires zero allocations
and compares the encoded receipts and counters with the original backing.

Run from the repository root, without calibration or population setup:

```sh
cargo test --locked -p acyclic-conformance --features local-hotpath --bench local_hotpath -- --check-observations
```

The retained `clone-identity.patch` changes the actual receipt serializer to clone
each identity graph. Apply it with
`git apply rust/fuzz/local_hotpath_report/clone-identity.patch` and run the same
control. The unchanged direct-encoding oracle must reject the allocation. Restore
the serializer with `git apply -R rust/fuzz/local_hotpath_report/clone-identity.patch`
before the positive check. This is an encoder allocation control, not a decoder
fuzzer or a deployed physical-work measurement.
