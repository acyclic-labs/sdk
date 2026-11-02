# Hierarchy

Give a lead an integration artifact and ownership of a coherent subtree. Let
independent leaves work together; the lead joins their results locally. Delegate
because it reduces context or latency, and stop when another split costs more
than the work it separates.

```ts
import { atomically, wait } from "acyclic";

await atomically([
  { spawn: "implementation", template: "worker", budget: null,
    prompt: "Implement the change in task.md. Delegate independent modules when useful; name file ownership before spawning. Join and verify their results; write .volumes/home/result.md." },
  { spawn: "review", template: "reviewer", budget: null,
    prompt: "Independently examine task.md and the current code for risks. Write evidence and focused validation suggestions to .volumes/home/findings.md." },
]);
export default wait(["implementation", "review"]);
```

Read each original result after the join. A lead's summary does not establish
its descendants' success or reconcile their spending. Use the shared escrow
and terminal-call mechanisms for those facts. Traverse a large hierarchy with
paged child listings and follows, keeping only the visible or subscribed cut.

Control: a failed leaf remains a failed contribution at integration. Replayed
terminal delivery cannot create another refund or integration artifact.
