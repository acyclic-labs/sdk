# Acyclic terminal

Build `acyclic` with `cargo build -p acyclic-plugin` for persistent local sessions
and native processes. Library consumers select the harness's `terminal` feature
for presentation playback and `native-execution` for native processes.

`acyclic run --workspace PATH` opens the configured filesystem and Stream
providers and ordinary task runtime. Prompt text admits a task; `/run TASK` starts
its fenced worker. Inspecting or reopening a session does not dispatch work.
Native publication requires configured approval and authority.
