# Temporal source policy

This Node package owns the shared source lexer, timer and loop classification,
JavaScript dependency graph, exact fingerprints and disposition validation.
It performs no file, Git or network I/O. It conservatively reports candidates;
an exact disposition records why a caller deadline, transport backoff, finite
operation or producer event is valid. Changing the source or its dependencies
reopens that decision. This is a source review rule, not a liveness proof.

From an SDK checkout at a pinned revision, run
`npm pack ./scripts/source-policy --ignore-scripts` and install the resulting
archive in the consuming Node project. The package declares its parser runtime
dependency and needs no TypeScript build or Rust artifact.

```js
import {supportedSource, pollingUnit} from '@acyclic-labs/source-policy';
import {newProjectPollingSites} from '@acyclic-labs/source-policy/project';

const inScope = file => supportedSource(file) && !file.startsWith('vendor/');
// The caller reads both snapshots from its own immutable baseline and current
// source, and filters each Map with the same repository scope.
const failures = newProjectPollingSites(before, after, dispositions, {inScope});
```

Snapshots are `Map<string, string>` values using repository-relative POSIX paths.
The analyzer resolves local JavaScript imports only against those maps; it never
loads a consumer's filesystem. Missing current dependencies and malformed
current source fail closed. A malformed predecessor authorizes no current site.
`projectPollingSites(sources)` inventories candidates;
`pollingUnit(file, source)` exposes tokens and sites for additional repository
policy. Supported inputs include JavaScript, TypeScript, Rust, C/C++, C#,
PowerShell, Python, shell, YAML and common configuration formats.

A disposition has exactly `file`, `fingerprint`, `classification`, `producer`,
`terminal` and `negativeControl`. Both source scope and policy remain explicit:
the library has no SDK directory assumptions or named repository exemptions.
Callers own their Git revision selection, inventory ratchet, diagnostics and
reviewed dispositions. Old inventory hashes from another analyzer do not grant
approval here; regenerate candidates against the immutable predecessor.

The existing source controls and byte-oriented fuzz target invoke these same
exports. Classification reads source once per unit; dependency closure visits
each graph edge a fixed number of times, independent of runtime history. No
timer, wait, worker or readiness request is introduced.
