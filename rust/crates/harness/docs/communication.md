# Durable task communication

`communication.rs` is the host-neutral validation boundary for task messages
and waits. `DurableTaskHost` remains the owner of journals, capabilities,
content residency, and recovery. The adapter never creates an in-memory
mailbox or a second durable ledger.

Messages carry a sender, recipient, operation identity, target relation, and a
versioned `FileRef`. A target must be a direct parent or direct child according
to the two owner-retained admissions. A caller cannot grant itself a sibling or
session-wide recipient by putting a task list in the request. Cross-branch
delivery needs a future owner-attested capability and must be added as a new
contract, not inferred from a model-visible target field. The endpoint digest
binds both task identities to the caller's operation identity for journal
diagnostics.

Inbox observations are bounded and must be contiguous from the supplied cursor.
Every item must name the requested task, use a canonical non-nil operation
identity, and carry a valid content reference. The adapter validates host
output again so a faulty provider cannot make a gap or duplicate look like a
delivered message.

Waits have one explicit target: direct child outcomes, the caller's own inbox,
or an absolute deadline. They have a stable operation identity, an optional
absolute timeout, and an optional cancellation identity. `WaitRecord` is the
durable state machine: only `pending` can advance, and completion, cancellation,
and timeout remain distinct terminal states. `DurableCommunication::wait`
composes that contract with owner-host observations, preserving task order and
returning typed cancellation or timeout outcomes. Deadline waits are persisted
through the host's idempotent timer operation, so process restart resumes the
same wait identity.
