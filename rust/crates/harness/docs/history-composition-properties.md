# History and checkpoints

Canonical Stream events remain authoritative. Context selection and compaction
change model input without deleting history. Pinned cursors exclude later appends
and charge each page against explicit event and byte allowances.

Cold restoration authenticates checkpoints, locators, canonical events and their
atomic binding. Missing or corrupt retained artifacts fail explicitly. Retiring
resident prefixes preserves logical revision and indexed retry identity.

Active and uncertain effects remain resident until reconciliation. Terminal
archives retain their original identity and result. Snapshots require the original
issuer's authentication; content references require owner-authorized grants.

Cold restoration and archived effect observations have explicit finite input
allowances. Bounded checkpoint chunks and proof reads do not establish bounded
whole-process memory or activate a finite terminal cache by default. Native
fixtures do not substitute for browser qualification on the final source.
