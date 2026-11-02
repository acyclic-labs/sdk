# Pipeline

Give each stage one predecessor artifact and one output. Start independent
preparation together; let dependent stages consume the predecessor's committed
output. Preserve its source identity rather than relabeling a coordinator's
forwarded message as a peer message.

Start the first stage and return its original wait:

```ts
import { spawn, wait } from "acyclic";

const extract = await spawn("extract", {
  template: "worker", budget: null,
  prompt: "Extract claims from source.md into .volumes/shared/claims.md.",
});
export default wait(extract);
```

On completion, inspect its status and claims. If valid, start a verifier with
that original artifact as input and return its wait. Repeat for the report
stage. Failure or a missing output ends this attempt before the dependent
stage starts. Waiting only for a file to appear loses the producer's failure
event and is insufficient. Each phase persists through the original code call;
there is no separate pipeline checkpoint or polling loop.

For a lasting pipeline, use the [service](service.md) reaction at each edge.
Keep the original request identity through every stage. Publish the final call
result through its existing `calls/<id>` owner; intermediate artifacts are not
another call journal.

Control: an ungranted or reversed edge must be refused before its dependent
work activates. A missing predecessor output must not be counted as success.
