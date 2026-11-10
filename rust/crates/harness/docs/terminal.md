# GraphCoder sessions

`graphcoder` starts the shared local server when needed and opens a new session.
The terminal is a client of that server. `/quit` detaches; closing the terminal
leaves the agent running. Ctrl-D offers stop or detach, and Ctrl-S opens the
paged session switcher. `/resume` opens the same switcher, and `/new` creates
another independent root.

Use `graphcoder -c` to continue the latest session for this checkout,
`graphcoder -r` for the picker, or `graphcoder -r SESSION_ID` to attach directly.
A canonical agent path also works. `-r NAME` filters the visible picker pages
by title. Advancing to another page is an explicit user action.

`graphcoder new [TITLE]` creates a session. In a terminal it opens the new
conversation; with a dumb terminal it prints the root identity. `graphcoder
sessions` returns a page of independent roots and their projected titles and
states. `graphcoder -r SESSION_ID rename TITLE` conditionally writes `ui/title`.
`graphcoder -r SESSION_ID archive` sends the ordinary closing input, refusing
later messages and releasing owned resources through the closing actor.
The switcher also offers rename and archive.

Titles are authored files. An absent title is projected from the first prompt.
Directory membership, root records and original status streams supply the other
session data; there is no separate session metadata database or transcript copy.
Session creation, directory membership and fresh or forked volumes share one
filesystem commit. Session count and history are unbounded; page size and byte
credit only divide the work a client requests.

Any number of terminals can follow one session. Prompts retain their operation
identities and committed inbox order; attaching never dispatches a model call.
An existing local server supplies its own client capability, so reattaching does
not reload its provider credentials. `--endpoint` uses the same authenticated
client protocol for an explicitly served endpoint.

Headless invocations can select a served session with `-c`, `-r SESSION_ID` or
`--agent AGENT_PATH`, or create one with `--new-session`. Without a session
selection, ordinary `-p` and JSONL executions retain their process-owned harness.
`--operation` retains the caller's message identity across interrupted requests.

Embedding applications use `LocalHarness::connect_sessions()` for an explicit
owner connection to the session directory. `LocalHarness::connect()` keeps its
single-tree credential boundary. Both use the same serving implementation and
actor runtime. A directory credential requires the placement owner's original
unbound whole-volume admission; a tree credential cannot select another root.
