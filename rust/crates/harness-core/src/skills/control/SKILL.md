---
name: control
description: Steer or stop running child agents.
---

Steer a running child with `send {agent, message}`; it receives the message at its next turn boundary. Stop a child and all of its descendants with `archive {agent}`; its fork stays inspectable with `fs diff agents/<name>` until you merge or abandon it. Bound a wait with `timeout_ms` instead of waiting indefinitely, then decide whether to steer, wait again or archive.

Cancelling a turn stops its open calls and settles the agent idle; nothing in its files is lost.
