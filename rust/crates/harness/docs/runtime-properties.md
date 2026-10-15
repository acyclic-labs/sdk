# Runtime contracts

Task, workflow, execution and conversation journals own admission and recovery.
Signed owner scopes and current lease fences govern fresh dispatch and publication.
Historical read custody does not authorize writes. Cancellation retains uncertain
effects and their reservations until reconciliation.

The `native-execution` adapter runs explicitly approved processes through these
journals. Approval covers executable path, arguments, environment, stdin, working
directory, allowances and volume manifest. The host maintains executable integrity
and supplies credentials and publication permits separately.

Recovery verifies retained request and result artifacts and receipt identity.
Failed or uncertain publication cannot become a successful task completion.
