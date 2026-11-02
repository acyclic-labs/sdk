# Service

Use an after-hook for lasting reactions to a granted log. Enable its manifest
at the service owner. A shared-path hook runs once for that enabling instance;
inheritance does not create one shared-log consumer per descendant.

`plugin.ts`:

```ts
import type { PluginManifest } from "acyclic";

export default {
  name: "receipt", description: "Records accepted request positions.",
  after: { requests: { path: ".volumes/shared/service/requests.jsonl", from: "start" } },
} satisfies PluginManifest;
```

`hooks/after-requests.ts`:

```ts
import type { AfterHook } from "acyclic";

export default (({ delivery, key }) => {
  if (delivery.kind !== "log") return [];
  return delivery.added.map((_request, index) => ({
    append: ".volumes/shared/service/accepted.jsonl",
    record: { version: 1, delivery: key, index },
  }));
}) satisfies AfterHook;
```

The hook cursor and returned appends commit together. This example records
acceptance only: it is not a call-completion protocol. An actual service uses
the original caller-owned `calls/<id>` path for completion, including errors.
External side effects require their original idempotent call boundary; a hook
replay alone cannot make them exactly once.

One invocation processes only its delivered page. A malformed request blocks
or produces the service's typed refusal; it must not fabricate success. A lost
executor settles through the shared call terminal, without an observer loop.

Control: replay after a failed commit produces one acceptance per source
record. Removing the cursor/effects atomic fence must expose duplication.
