Run the production path admission control from the repository root:

```sh
python3 rust/proofs/agent-path/run.py
```

The existing ancestry property compares direct-child admission with the original
parent operation across shallow and maximum-depth canonical paths, including
foreign roots, self and deeper descendants. Allocation counters cover 1, 100 and
1000 calls. The runner first restores the original copying operation and requires
the unchanged zero-allocation assertion to fail, then restores exact source bytes
and requires the same property to pass. Admission has no waits or IO and compares
at most the bounded canonical path width, independently of the agent population.
