# Pi native session coordination

Install the extension with `acyclic install pi`, start GraphCoder outside the
native host, then launch Pi with explicit native options:

```sh
acyclic host pi --
```

The extension provides spawn/send/wait/cancel/archive/resume. It calls the same
coordination shim as Codex and Claude. Each managed child is a separate native
Pi CLI process whose cwd is the admitted view inside workspace/.volumes/agents.
Pi owns discovery, tools, providers and its agent loop. Native built-in subagents
retain their own behavior and are not harness forks.

The launcher preserves native arguments and inherited process restrictions.
Capture follows agent end, session shutdown and reaped child process trees;
there are no per-tool write markers. Resume passes the saved native session
reference to Pi in the recreated view. The extension only obtains that opaque
reference from Pi's session manager; Pi reads its own transcript.

Merge captured changes with `acyclic fs merge agents/<reference>`.
