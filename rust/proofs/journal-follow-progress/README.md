# Journal observation and conditional cancellation progress

Run `node rust/proofs/qualify.mjs journal-follow-progress` for original
observation retirement, or
`node rust/proofs/qualify.mjs journal-cancellation-progress` for conditional
commit cancellation.

Both use the shipped Wasm JournalStream and SqliteJournal over an actual
file-backed SQLite database with full synchronization. The conditional oracle
opens two members and creates two immutable forks. It holds the original
storage acknowledgement after the real transaction commits, then drops the
original conditional commit future. The already parked public follow must be
woken, return Unavailable and terminate before that acknowledgement is released.
Later observers must refuse, and reopening must recover the original transaction
and retry identity without duplicating it. An unstarted future and a refused
condition must leave the authority live.

The original cancellation mutation removes the entire started-attempt
retirement. The independent revision mutation preserves the failed bit and
removes only closure of the original AppliedRevisions. This distinguishes
rejecting new calls from waking an observer already parked before cancellation.
Both must fail immediately at the original registered-follow wake assertion.
The qualifier restores exact production source before checking the same oracle.

The follow oracle also independently removes the original terminal notification
from AppliedRevisions::close while preserving its retained closed bit and
revision-watch publication. The actual parked follow must fail its registered
wake assertion. This distinguishes retaining terminal state and dispatching a
ready notification from producing the event that wakes the original observer.

No new example test, replacement producer, polling loop or timeout is added.
The original storage acknowledgement is the single held event, and its producer
is released on the same path. Production operations and their allocation,
copy, syscall, round-trip and durability-barrier counts are unchanged. This
does not prove completion of live I/O that never replies or arbitrary browser
event-loop execution.
