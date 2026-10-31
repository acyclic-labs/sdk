---
name: advisor-consultation
description: Work with a persistent alloy advisor at consequential planning decisions, then obtain an independent final review.
---

Start an ordinary advisor-role actor with a self-contained brief: the task, constraints, current evidence, and the decision that needs advice. Reuse its stream throughout the task so later consultations build on earlier work.

Keep the worker's plan in `.volumes/scratch/plans/<name>.md`. Send it as a content-addressed, read-only snapshot at a named version, with enough evidence for the advisor to work independently. The advisor uses the supplied snapshot rather than the live worker volume. It may investigate or prototype in its own forks; it returns evidence and a revised plan file naming the base version, with an appended delta explaining the changes. The advisor never edits the worker's tree or merges forks into it.

The worker owns integration. Merge or rebase the reply against its named base, preserving intervening changes and resolving stale assumptions before applying the revised plan. Continue independent work while advice is pending; use `wait` only when the next action depends on the reply. Consult at real decision points or when evidence changes the plan, rather than after every step or on a fixed schedule.

Before finishing, start a fresh reviewer-role child from a different model family. Supply only a self-contained task brief, the actual diff, and test output. Ask for PASS or FAIL with concrete findings, and address any failures before finishing.
