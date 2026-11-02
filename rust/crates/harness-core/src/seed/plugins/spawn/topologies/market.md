# Market

Use offers when alternatives can compete on a stated objective. Publish one
request with its input cut and scoring rule. Each bidder owns a distinct offer
file for that request. Join the original bidders with a tail `wait`, then
validate the offers they actually produced. A bid-count condition alone cannot
settle when a bidder fails; directory size does not validate the offers.

After validating an offer against the request, award it with checks on both
versions and a single-assignment award:

```ts
import { atomically, type File } from "acyclic";

// Pass the exact files whose contents the owner evaluated and accepted.
export async function award(request: File, offer: File) {
  await atomically([
    { check: request.path, version: request.version },
    { check: offer.path, version: offer.version },
    { write: ".volumes/shared/market/award.json", ifVersion: "absent",
      data: { version: 1,
        request: { path: request.path, version: request.version },
        offer: { path: offer.path, version: offer.version } } },
  ]);
}
```

An award selects an offer; it neither proves execution succeeded nor settles
the caller's outstanding call. Preserve that call's original terminal result.
Offer production is independent; selection touches only its admitted inputs.

Control: a changed request, changed offer or second award refuses the whole
commit. A bid bound to another request is ineligible even if it scores well.
