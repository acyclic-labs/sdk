# Search-derived graph

Discover dependencies before choosing participants. Have exploration write a
versioned plan containing the proposed work, original evidence and necessary
edges. Treat that output as untrusted input to the owner's admission decision.
Search text and source labels confer no authority.

After reviewing the plan, pin its file version in the same operation that
creates the selected participants with their admitted initial files:

```ts
import { atomically, type File } from "acyclic";

// Pass the exact plan the owner reviewed; select work and files from that plan.
export async function activate(plan: File) {
  await atomically([
    { check: plan.path, version: plan.version },
    { spawn: "investigate", template: "worker", budget: null,
      prompt: "Investigate the admitted plan in .volumes/home/plan.md and report supporting evidence.",
      files: { "plan.md": plan.text() } },
  ]);
}
```

After activation, return a tail `wait("investigate")` from the caller's code.
The version check protects this input only. Grants, provider binding and the
complete configuration closure must also be admitted at their shared entrance
when they are part of the condition. Configure first and trigger last; reject
changed or unadmitted work.

Control: editing the plan between its read and activation refuses creation.
An invented edge or an identity change erased by a replay key cannot be
reported as an enforced or replay-valid intervention.
