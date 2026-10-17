# Bounded conversation pagination

`PageWindow` retains ordered message metadata while the caller loads pages from
one pinned generation. Loading older messages evicts the newest edge when
`maxItems` is reached; loading newer messages evicts the oldest edge. Reversing
direction reloads the messages adjacent to the retained window.

A page can supply independent `before` and `after` cursors for its first and last
items. Each cursor is optional. The loader must interpret a supplied cursor as an
exclusive boundary in the requested direction. Cursors can be opaque and
direction-specific; item identity keys are only used for deduplication.

## Cursors inside a page

If the item limit can retain only part of a page, supply the fourth constructor
argument, `cursorForItem(item, direction)`. It must return a provider-valid
exclusive cursor for that item and direction. This includes an authoritative
snapshot containing more than `maxItems` items: `reset` keeps its newest items,
and the callback anchors loading the discarded older messages.

The conversation sequence is already an exact `bigint` cursor:

```ts
import {
  PageWindow,
  type ConversationMessage,
  type PageLoader,
} from "@acyclic-labs/harness";

declare const loadPage: PageLoader<ConversationMessage, bigint>;

const window = new PageWindow<ConversationMessage, bigint>(
  message => message.id,
  loadPage,
  200,
  message => message.sequence,
);
```

The reference composition in
`typescript/packages/harness/examples/client-demand.ts` supplies this callback.
Paging continues to fetch only metadata; message bodies remain demand-loaded.

## Compatibility and request fencing

The callback is optional when every retained boundary has a usable provider-supplied
cursor for the required direction. Existing callers in that case reuse the page's
original cursor, including opaque cursors. If eviction requires a cursor that neither
the page nor the callback supplies, `load` rejects or `reset` throws with a `RangeError`
identifying `cursorForItem` before changing the window. This includes cuts inside a
page and page-aligned cuts whose required directional cursor was omitted. The previous
items, cursors, generation and exhaustion flags remain unchanged. Supply the callback
when the loader cannot provide every retained boundary cursor.

Evicting an edge makes `hasMoreBefore` or `hasMoreAfter` true for that edge,
including when it was previously exhausted. An opposite-direction request
started before that edge moved is fenced: its result contributes no items.
A subsequent request uses the current retained boundary, and same-direction
requests still share one in-flight promise. Resetting the generation continues
to fence every older request.

The window retains cursor anchors only alongside retained items. It does not keep
evicted message bodies, a history of previous pages, or an unbounded cursor list.
