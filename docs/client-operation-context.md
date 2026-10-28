# One caller budget across transport stages

`acyclic_auth::operation::Context` binds an absolute deadline to the host's expiry future and caller cancellation once. Clone that context across credential resolution, connection, response reading and any explicitly classified retry. A ready stop is checked before polling another stage. Actors HTTP `Client::with_context` consumes this contract around its complete operation; it does not start another deadline for each stage.

The host supplies the timer for the stated absolute deadline. The context has no clock, background task, task-local state or credential cache. `Stopped::Deadline` and `Stopped::Cancelled` describe the caller's wait. Neither establishes that a remote mutation was cancelled or failed to commit.

Transport owners retain their typed refusal and settlement rules. A typed nonretryable refusal stops with zero retries. A lost mutation response retains the original request identity and unknown outcome; it cannot become a successful response or an uncertain resubmission. A safe retry, where the owning protocol permits one, uses the same context and original request bytes/key. No automatic retry policy is added to Actors HTTP. Stream and Machines adoption must preserve their owning protocol's settlement rules.

## Qualification plan

Compare the original and adapted transports using their actual generated descriptors and captured provider output. Record request/response bytes, error stages, unknown enum values, uint64 boundaries and applied/accepted/durable watermarks. Measure credential callbacks, physical requests and complete operation latency under injected RTT; record allocations and time to first yielded record separately from complete-page consumption. Compilation alone establishes no parity or performance result.

The negative controls must show that an invalid cursor stays invalid after a later append, changed bytes under the same idempotency key refuse, typed nonretryable errors trigger zero retries, a lost response retains Unknown, and an error after a streamed prefix fails the iteration. Expiry during credential resolution must prevent transport work; cancellation during an attempt must prevent another attempt under a clone. Run these controls at the actual transport seam before claiming runtime equivalence.
