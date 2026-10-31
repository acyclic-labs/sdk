---
name: fresh-context-review
description: Obtain an independent PASS or FAIL review of the final changes before finishing a task.
---

Start a fresh reviewer-role child configured for a different model family from the worker. Supply only the task, the actual final diff, and test output. Include the acceptance criteria and constraints in the task; omit the worker's conversation, proposed verdict, and explanations defending the implementation.

Ask for PASS or FAIL, with concrete findings tied to the task and changed behavior. A FAIL should name the location, consequence, and evidence for each required correction. Missing required verification is a gap to report, not an assumed pass.

The worker owns corrections and integration. Address failures and rerun affected checks, then request at most one re-review using the updated task, actual diff, and test output. If a failure remains, report it without claiming completion. Keep the review verdict distinct from the worker's verification evidence.
