# Machines adapter

The `machines` feature bridges sandbox and checkpoint references to a
`MachinesProvider`. `MachinesHost` captures selected forkable checkpoints and
verifies their retained provider identity before admission.

`MachinesExecution` requires registered task builds, matching spawner/state
identities and access attestation. Admission retains the selected environment and
route; recovery uses that route. The supplied spawner and state owner implement
dispatch, observation, cancellation and reconciliation.
