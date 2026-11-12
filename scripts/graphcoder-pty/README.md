# Interactive PTY driver

Install this directory's pinned `@xterm/headless` dependency, and put GraphCoder
on PATH (or pass `--binary /absolute/path/graphcoder` to start).

```sh
node scripts/graphcoder-pty/drive.mjs start --name sdk --cols 120 --rows 40 -- --continue --workspace /scratch/sdk
node scripts/graphcoder-pty/drive.mjs record sdk
node scripts/graphcoder-pty/drive.mjs keys sdk 'Explain this repository' Enter
node scripts/graphcoder-pty/drive.mjs wait sdk --contains 'repository'
node scripts/graphcoder-pty/drive.mjs screen sdk
node scripts/graphcoder-pty/drive.mjs wait sdk --settled
node scripts/graphcoder-pty/drive.mjs stop sdk
```

The persistent owner uses `Processes.startTerminal`, `TerminalFrames`, and the
shared headless screen. Linux sockets and their scratch directories are private
to the owner. `E2E_SCRATCH_ROOT` selects the scratch parent. Existing owner locks
are never stolen. Names are ASCII letters, digits, `_` and `-`.

Commands return JSON containing logical screen text, visible rows and cursor
coordinates. `screen --raw` adds styled cells. Literal arguments concatenate;
named keys include Enter, Esc, Tab, arrows, Home, End, Delete, Insert, PageUp,
PageDown, Shift-Tab, and Ctrl-A through Ctrl-Z. Prefix `text:` to disambiguate a
literal key name. `wait` takes exactly one of `--contains`, `--absent`,
`--settled`, or `--exited`. It observes frame/state events, with a 120 second
protective failure deadline. It never treats output silence as settlement. After a submission,
settlement sends a correlated application query through the terminal input
decoder, ordered after the preceding keys. The application captures the root's
existing `wait_current_input` fence and answers in a synchronized frame after
that fence settles. Empty Enter and local controls need no new inbox cursor.
The asynchronous query leaves the UI responsive to approvals and interrupts.
It requires no transcript export, status file watcher or cached status feed.
Private DSR `CSI ? 7727 ; ID n` requests the fence; OSC 777
`acyclic;settled;ID;PHASE_JSON` is the application response. Both use the shared
terminal decoder and frame writer, including when a child is selected.

`record` captures the initial styled screen once, then timestamped logical
frames and an asciicast v2 output capture. ANSI output retains subsequent styling;
`screen --raw` supplies the current styled cells. It returns both recording paths.
`start` reports first-paint latency; `keys`
reports transport write latency (not input echo latency). Compare frame
timestamps to measure echo and redraw. No credentials or environment are saved
by the driver. Provider credentials must stay in a host relay, outside guest
processes.

The deterministic real-binary regression is:

```sh
node scripts/graphcoder-e2e/run.mjs /path/graphcoder /path/harness-mock-model pty-driver
```

Windows uses the same ConPTY adapter and requires `E2E_PROCESS_HOST` and
`E2E_CONPTY_HOST`. Native Windows runtime coverage follows Linux qualification.
The deterministic e2e may adopt the driver later; this change keeps its current
action runner and shares its transport and frame implementation.

For continuous dogfooding, attach to the existing repo session with its owner
state directory and `--continue`/`--agent`. Do not create another orchestrator.
Claims and patch handoffs follow the configured work queue and review policy.

Executable completion leaves the driver and its last screen readable until
`stop`. `wait --exited` returns that screen with an `exit` object containing the
process's code and signal. The final screen retains styled cells even after
alternate-screen restoration. Input and settlement queries require a running
executable; screen reads and recording remain available after completion.
