---
name: review
description: Obtain an independent fresh-context review of a completed change.
---
Spawn a reviewer with a self-contained brief containing the task, relevant constraints, the diff and actual check output. Include what is intentionally out of scope. The reviewer receives fresh context; do not depend on the parent transcript. Ask for PASS or FAIL with actionable correctness, regression and missing-check findings. Keep the reviewed version identifiable. Address findings in the worker's tree, then verify the revised change. A review report is evidence for the parent's decision; it never merges a child automatically.
