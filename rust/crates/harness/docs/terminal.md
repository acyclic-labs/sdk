# GraphCoder terminal

Build `graphcoder` with `terminal,filesystem-local` for persistent local sessions.
Add `native-execution` for native processes and root writeback. `terminal` alone
supports presentation playback.

`graphcoder --session CONFIG.json` opens the configured filesystem and Stream
providers and ordinary task runtime. Prompt text admits a task; `/run TASK` starts
its fenced worker. Inspecting or reopening a session does not dispatch work.
Native publication requires configured approval and authority.
