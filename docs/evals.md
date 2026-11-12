# Evaluation files

Evaluation cases are computed from original settled turns. They have no database,
background index or separate publication. `evals::StreamProvider` composes the
authenticated conversation provider with the filesystem's existing `Computed`
adapter. Bind that source at the private `evals/` route, outside the project cwd.

The root is a paged directory. Names are the canonical twenty-digit conversation
sequence followed by `.json`. Listing uses the pinned tail; it does not decode
turns or load their bodies. An exact lookup/open prepares only that turn. A file
range serializes directly into the requested output interval.

The version-one JSON case contains:

- `cut`: original conversation stream and exclusive prefix before the decision;
- `source`: original sequence, origin and publication identity, including forks;
- `decision`: admitted inbox position, typed input and steering transcript;
- `outcome`: settled status, original output FileRef and tool-result directory.

Human corrections and overrides retain their exact authored input. Approval
inputs retain their request identity. No classifier guesses acceptance from prose.
The output reference retains the original answer and its cost metadata; cases do
not create a second cost total. Resolve source files through current content
authority and their original immutable versions.

A selected cut excludes later appends. Every demand, including directory-only
requests, authenticates the retained tail through the current stream provider.
References and cursors confer no authority. Expired cuts fail rather than
substituting current history. A subscription starts one conversation follow;
there is no follow or task for an idle projection. Notifications are events and
retirement releases the original subscription owner.

Host credentials and provider configuration are outside the case input types.
Secret custody remains at the original execution boundary; exports must select
case/context/outcome sources explicitly and must not package an entire private
volume or host configuration. Text matching is not a substitute for custody.

Agents may pull cases, inspect original decisions and outcomes, and propose an
ordinary `skills/<name>/SKILL.md` write for review through `ui/`. Cases are never
automatically inserted into model context.
