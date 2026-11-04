# Sealed read retention

`logical_objects::sealed::sealing_preserves_the_epoch_roots_retention`
uses the public logical store with the real memory Objects provider. It checks
scalar and batch plaintext views against their physical retention evidence,
including ordinary and native original work, collection and final release.

`mutations/sealed_read_retention.patch` restores the production defect: after
opening the seal, recompute custody from the shorter plaintext view. The ordinary
read must reject that result because the complete physical seal remains live.

Run the observation with `cargo test -p acyclic-fs --no-default-features
--features local,memory,distributed --test main
logical_objects::sealed::sealing_preserves_the_epoch_roots_retention -- --exact`.
A successful positive run alone does not qualify the production mutation.
