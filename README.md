# acyclic

Checkpoint every agent action, rewind exactly, see the blast radius.

`acyclic` is a local state engine for coding agents. It snapshots your working
tree around every edit and command an agent makes, including what git cannot
give back: untracked files, gitignored artifacts, and whatever a shell step
wrote. History survives across sessions and is linked to the conversation turn
that caused it. Forks let several agents try a task in parallel; you keep the
one that wins.

## Install

```sh
npm i -g @acyclic-labs/plugin
cd your-repo
acyclic init                 # starts the daemon, takes the first snapshot
acyclic install claude-code  # or: codex, cursor, claude-desktop, agents-md
```

`curl -fsSL https://raw.githubusercontent.com/acyclic-labs/sdk/main/plugin/scripts/install.sh | sh`
installs the binary without npm on macOS and Linux.

## Where things are

- [`plugin/README.md`](plugin/README.md): the product. Hosts, configuration,
  rewind, timeline, forks, safe mode, and how the releases are built.
- `rust/crates/*` and `typescript/packages/*`: the SDK the plugin is built on.
  [ARCHITECTURE.md](ARCHITECTURE.md) has the dependency graph.
- [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), and
  [`provenance/README.md`](provenance/README.md) before you send a change.

Apache-2.0.
