# Mesh

Use independent participants when each can contribute without waiting for all
the others. Give them a shared append-only observation log and an explicit
output artifact. They read original records, retaining their authenticated
producer and source position. A textual `author` field is not an authority.

```ts
import { atomically, send, wait } from "acyclic";

await atomically(["correctness", "performance", "usability"].map((name) => ({
  spawn: `review/${name}`, template: "reviewer", budget: null,
  prompt: `Review design.md for ${name}. Append evidence to .volumes/shared/review/observations.jsonl; read peers' original observations when useful. Write your conclusions to .volumes/home/findings.md.`,
})));
await send("review", "Conclude with your strongest finding and its evidence.", "idle");
export default wait("review");
```

The group is the `review/` subtree. Use a durable read when an observation is a
real dependency; an idle broadcast does not establish that anyone consumed it.
Integrate findings once the chosen completion condition holds. Do not broadcast
every new record back to all writers, including its author.

Control: an outsider cannot append or register a read without the corresponding
grant. A participant that fails must not silently become an empty review.
