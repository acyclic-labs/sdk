---
name: verify-before-done
description: Check the final changed state against the task's acceptance criteria before claiming completion.
---

Translate the requested outcome into observable checks. Run the smallest checks that establish it against the actual final files, including integration boundaries the change affects. Prefer existing tests and real behavior; add coverage when it proves a meaningful guarantee. For a bug fix, show that the check distinguishes the broken behavior from the corrected behavior.

Inspect the final diff for unintended changes. After a fix or integration changes the result, rerun the affected checks; an earlier pass does not verify later edits. Keep commands, exit status, and relevant output available for the final reviewer.

State what passed, what failed, and what could not run. A plan, an unexecuted command, or a reviewer's approval is not execution evidence. Claim completion only to the extent the observed result supports it.
