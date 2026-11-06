# Checkpoint policy settlement

Run `python3 rust/proofs/qualify.py checkpoint-policy` from the repository root.
The retained mutation restores the production early return for a checkpoint
that exceeds an explicit manifest policy. The unchanged native-provider oracle
must reject its missing terminal publication before waiting for the response.
Exact source restoration must then pass the same property.

The property uses the public invocation entry point and the real LocalStream
for nine actors: no policy and every positive policy from one through eight
bytes, around a seven-byte candidate. Accepted candidates publish their response
and checkpoint together. Refused candidates preserve the previous checkpoint
and publish their typed cause with the consumed cursor. Exact reconciliation
returns the original failure receipt. Closing and reopening the provider must
preserve every committed checkpoint and cursor.

The check adds no wait primitive. The existing invocation terminal is produced
by the same actor state commit. Policy admission is one optional-limit comparison,
with no allocation, syscall, durability barrier or round trip on the successful
path. Refusal uses the existing failed-input publication and its original
conditional commit. This does not establish storage of a logical checkpoint
larger than one physical record; that remains a separate contract obligation.
