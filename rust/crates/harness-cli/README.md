# acyclic-harness-cli

The native CLI composition of the harness: process completion custody, host
lifecycle hook manifests, and the shared usage footer.

- `completion` keeps the original native report port with the operation and
  shutdown event until the operation returns and its failure delivery settles.
- `host_hooks` describes the Codex and Claude lifecycle hooks an installed
  executable registers. It never executes tools or starts an agent.
- `usage` formats an already-folded usage projection for the terminal footer.
  It performs no source reads, counts or price selection.

Hosts retain their own tools, permissions and loop. `acyclic-harness` re-exports
these modules at their original public paths.
