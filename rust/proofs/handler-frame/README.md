The handler deadline adapter lives inside the original delivery future, without
allocating a second frame around the platform timer. The existing LocalStream
deadline property measures construction of that production adapter, then runs
three explicit deadlines through the public runtime, original invocation receipt
and physical reopen under virtual time. It does not infer the platform timer's
allocation or registration cost from adapter construction.

`python3 rust/proofs/qualify.py handler-frame` restores the former boxed adapter.
The same property must reject its physical allocation, then pass after exact
production source restoration. The platform deadline remains the only completion
producer; its original registration/producer errors retain their typed mapping.
No timer is registered until the original delivery polls its deadline.
