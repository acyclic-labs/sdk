# @acyclic-labs/stream changelog

## Unreleased

- Check monotonic tail before direct HTTP reads so concurrent appends cannot hide invalid empty-read cursors. Share native and WASM HTTP response projection, including atomic Commit envelopes.
- Bind the default browser fetch receiver and qualify HTTPS, follow and multi-path Commit in real Chrome.
- Project an absent retry observation as `undefined` consistently in memory and HTTP.

## 0.1.5 - 2026-09-25

- Aligns stream clients with the qualified SDK 0.1.5 release.

## 0.1.4 - 2026-09-25

- Aligns stream clients with the qualified SDK 0.1.4 release.

## 0.1.3 - 2026-09-25

- Rebuilds the stream contracts and client from the qualified SDK 0.1.3 source.

## 0.1.2 - 2026-09-25

- Aligns the stream contracts and client with the 0.1.2 SDK release.

## 0.1.1 - 2026-09-24

- First coordinated SDK release of hierarchical append-only streams.
- Includes explicit cursors, conditional appends, forks, and coordinated
  commits with typed provider contracts.
