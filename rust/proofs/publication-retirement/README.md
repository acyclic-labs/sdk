# Cancelled publication drains

Run `python3 rust/proofs/qualify.py publication-retirement` from the repository
root. One scoped library invocation selects the two existing properties below.
The retained mutation restores the original collection implementation; both
properties must reject it at their respective named assertions before exact
corrected source is restored and both pass.

The public local filesystem writes through its original Objects account and
Stream authority. Its original publication is paused after retaining content.
Eight collection attempts reach that publication's actual lock waiter and are
dropped. Only the first may switch the publication generation, in memory or in
the original durable epoch pointer. Dropping the pause then releases the same
producer; the final collection must finish and the published content must remain
readable, including after closing and reopening the actual store. There is no
readiness retry or timeout.

Safety: collection marks and releases roots only after the pending publication
drains. Progress: the live original publication owns the lock releasing the
drain; withdrawing a collector preserves that pending lock for its successor.
Cost: one pending generation replaces the vector of cancelled attempts. A
successor drains it before reading or persisting another epoch, so cancelled
attempts add no generations, history scans or epoch writes.

The existing collection admission property withdraws its first drain and checks
that withdrawal retires its active admission registry before resuming. Its
negative control uses the same original production mutation. This checks the
second cancellation branch without creating another fixture or compilation.
No persisted format or decoder changes.
