# Follow digest scratch ownership

```sh
node rust/proofs/qualify.mjs follow-digest-cost
```

The existing signed-capability provider-hop control calls the production
`FollowFilter::digest`. Repeating one actual admitted clause at unchanged
encoded size must not grow its physical allocation count or allocated bytes.
Empty digests allocate nothing; a single clause needs at most one allocation
bounded by its encoded size. These retain the former comparator's absolute
bounds directly on production work.
The retained mutation discards the production scratch buffer between clauses;
the unchanged allocation assertion must fail. The qualifier restores identical
source and runs the same control.

The independent v1 digest framing oracle remains: matching two calls to the
same implementation would not establish format stability. The same test keeps
real signed authority ceilings, unrestricted zero-allocation admission,
MemoryStream append/read, typed projection, malformed refusal and the transport
round trip. It removes the copied former authorization builder and its
allocation comparisons.

No product operation changes. Digest work still hashes every encoded clause;
this control establishes scratch reuse at fixed maximum clause size, not
constant total hashing time or complete authorization-operation cost. It adds
no wait, syscall, durability barrier or round trip.
