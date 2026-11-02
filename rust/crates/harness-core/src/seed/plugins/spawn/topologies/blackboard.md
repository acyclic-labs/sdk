# Blackboard

Use shared, versioned files when contributors refine common knowledge. Choose
whether a key is replaceable or single-assignment. Read the current version
once and condition the write on that version; a conflicting write is a visible
decision point, not permission to overwrite a peer.

```ts
import { read, write } from "acyclic";

const path = ".volumes/shared/board/design.md";
const current = await read(path);
const contribution = "A proposed design with its supporting evidence.\n";
await write(path, contribution, current?.version ?? "absent");
```

For a single-assignment decision, write with `"absent"`. For several dependent
keys, use one `atomically` operation with checks on every input version and the
new values. Decide how to reconcile a conflict from the competing evidence;
avoid a retry loop. Watch only the keys a participant actually depends on.

Control: two contenders using the same old version cannot both replace that
version. A failed check changes none of the dependent outputs. Different keys
remain independent.
