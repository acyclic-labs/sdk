# Decisions

- Keep further SDK latency work on `c6-msdk-machines-client-latency` while
  reviewed lane PRs merge. Do not move their heads or poll them.
- Reconnect once immediately after authoritative pending inspection. Retain
  the existing delay for repeated interruptions and failed inspections; never
  resubmit an admitted mutation.
- Replace serialized fork-child reads with an ordered stream of at most eight
  concurrent reads. The public admission bound is 1,024, so unconstrained
  fan-out would trade latency for a large burst of RPCs. Keep existing identity
  validation and fail the result on any failed child read.
- Run the scoped check and changed tests once immediately before opening.
  Run the paired create/exec proof once in a `proof-` labelled remote job.
  These changes remain unverified and unmeasured until those jobs run.
- Bound the SDK's post-exit EOF drain to one second. Receipt of the terminal
  exit code stops the command deadline; the independent completion bound does
  not restart on a late poll. Expiry returns EOF and drops the underlying RPC.
- Apply the command deadline locally while awaiting exec response headers,
  including an uncooperative peer that does not honor the timeout header.
  Preserve indeterminate command custody; never redispatch it automatically.
- Keep the durable-operation observation wait open-ended. Production channels
  bound RPC headers to 30 seconds, and watch silence triggers reconciliation
  after 60 seconds. The fork stream holds at most eight inspections and the
  checked admission already bounds the final vector to 1,024 children. The
  existing retry classifiers have distinct watch and inspection semantics;
  retain them instead of merging away the unknown-inspection ambiguity bound.
