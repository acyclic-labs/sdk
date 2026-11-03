# Git layout capture and path mutations

`git-layout` is unchanged stdout from Git 2.46.0.windows.1 on Windows x86_64:

```
git --no-replace-objects rev-parse --is-shallow-repository --git-path info/grafts
```

Captured 2026-11-03T03:55:43.050582Z through 03:55:43.322091Z. Exit 0,
no signal, empty stderr; stdout 23 bytes, SHA-256
`e1ea4881c22208dc9b22a84578979d6c34853497581c37918d21c798b4aa7370`.
The original process output reports complete ancestry and `.git/info/grafts`.
The shared property and fuzzer invoke the same production library parser used
by source-frontier admission. Accepted paths drive CRLF, shallow, extra-record,
missing/empty-path and invalid-UTF-8 mutations; these are derived inputs, not
additional process captures. Generated path variants retain the original field
identity oracle, and arbitrary bytes exercise the parser directly.

Other seeds are pre-existing path/source-selection mutations without recorded
external capture provenance. They are not evidence of a process run. Removed
fixed-head, valid-head and manifest seeds covered only test-scaffold parsers.
Path identity now calls the shared library directly. Source selection reuses its production source directly. Its shared identity
oracle checks the complete physical path, logical path and digest set, including
repeated entries and independently varied logical paths/digests at one physical
path. These are typed domain mutations of the path seeds, not captured external
wire records. The retained loss/logical/digest patches mutate the real production
deduplicator. The property and fuzzer share this oracle; source-selection branch
coverage remains open.
This corpus and planted-defect property do not establish an instrumented fuzz
campaign, full branch coverage, Git process loss, shallow-repository topology,
or complete source-frontier admission.
