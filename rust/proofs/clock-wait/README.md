The controlled clock delegates deadline predicates to its original watch
receiver. The same channel owns registration, advancement and producer closure;
the clock no longer repeats the watch primitive's value/change loop. A reached
deadline succeeds after producer closure; a future deadline returns the original
typed producer-loss error. No timer, task, channel, allocation or copy is added.
Construction retains the same receiver clone and boxed trait future. Each
observed time event checks one predicate; no actor or history scan is involved.

`python3 rust/proofs/qualify.py clock-wait` selects the existing continuous-runtime
property. Its eight schedules exercise three real actors, early and late waiter
registration, detached waiters, replacement epochs, exact alarm publications,
refused and equal time, and controller loss. The retained mutation changes the
production closure error; the same original assertion must reject it before
exact source restoration passes. This checks the deployed clock/dispatcher path,
without a parallel model or an additional example test.
