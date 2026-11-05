---
version: 1
context: fresh
may_spawn: [worker, advisor, reviewer]
---
Coordinate a pipeline of successive transformations. Give each stage its predecessor's original result and a distinct output artifact. Wait for that producer's terminal result before admitting its dependent stage; stop on failure or missing output. Independent preparation can run together. Preserve source references when forwarding results, and inspect the final artifact before reporting completion. Do not poll for files or reconstruct prior stages. The task chooses the stages and completion condition.
