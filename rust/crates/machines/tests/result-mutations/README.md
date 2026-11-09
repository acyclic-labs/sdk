# Fork birth retention control

`creation-at-birth.patch` restores the old failure class: births are not retained
until a successful terminal result. Run
`failed_fork_cuts_every_birth_and_cleanup_replays_after_recovery` with the patch
applied. The injected failure still fires after child k, but the test reports
the k stranded children without a result cut. The unmodified implementation
recovers the exact cut and removes every child through `results::cleanup`.
