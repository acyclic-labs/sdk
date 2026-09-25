# acyclic-harness-machines

Explicit bridge from Harness sandbox and checkpoint references to a replaceable `MachinesProvider`.

```sh
cargo add acyclic-harness-machines
```

`MachinesHost` also implements Harness's fork admission verifier: it checks each selected process checkpoint with the exact Machines provider and refuses missing or non-forkable checkpoints before the parent publishes a fork. The adapter preserves provider identities and operation recovery; it does not upgrade a simulator into a hosted isolation boundary. Select a Machines provider whose stated assurance matches your deployment. See the [adapter API](https://docs.rs/acyclic-harness-machines/latest/acyclic_harness_machines/), [Machines guide](https://docs.rs/acyclic-machines/latest/acyclic_machines/), and [Harness guide](https://docs.rs/acyclic-harness/latest/acyclic_harness/).
