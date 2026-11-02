# Debate ring

Order participants when each challenge should address its predecessor's actual
argument. For example: proposal, counterexample, repair, assessment. Give each
participant the original predecessor artifact and a distinct output path.
Choose the number of rounds or a stopping condition explicitly.

```ts
import { spawn, wait } from "acyclic";

const proposer = await spawn("propose", {
  template: "advisor", budget: null,
  prompt: "Propose a solution to problem.md in .volumes/shared/debate/1-proposal.md.",
});
export default wait(proposer);
```

Inspect the original terminal state and proposal before starting the challenger.
Repeat that join for repair and assessment. Stop on a failed predecessor or
missing argument; a read for an absent file alone cannot observe producer loss.
Close a cycle only with an explicit decision to start the next round, carrying
the preceding round's source references. A participant
must distinguish its predecessor's new argument from its own inherited text.
Keep round state at the current cut; do not rescan all preceding rounds.

Control: an argument for a different round or from a skipped predecessor cannot
advance the ring. A quiescent participant with no argument is a failed edge.
