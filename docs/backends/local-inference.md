Enable `acyclic-harness-providers/endpoint-presets`, call
`endpoints::local("http://127.0.0.1:8080/v1")`, add the server's exact model name
and configured context/output capacities to `models`, and register the spec.
Ollama and llama.cpp use the shared `Adapter::Chat` request pipeline. Anonymous
local endpoints are supported; an explicit key adds standard bearer auth.
The URL policy permits HTTP only on loopback. Servers lacking token counting
can generate; automatic compaction requires a verified count endpoint.

The retained bb2 fixture contains real Qwen3-0.6B-Q4_0 llama.cpp streams, tool
calls/results, usage, auth and context errors. The rate response comes from an
actual nginx rate limiter. The fixture README records exact provenance. No
Ollama-specific behavior has been qualified by this llama.cpp capture.

Reproduce capture, then run the complete common suite in replay and against the
live server (with an actual rate limiter and captured deterministic replay):

```powershell
pwsh -NoProfile -File Q:/lanes/bs.ps1 --env BE_EDGE_RUN_SUITE=1 bash scripts/be-edge-local.sh
```

Both servers bind only to `127.0.0.1`, on ports 15480 (llama.cpp) and 15481
(nginx). A lane-local lock prevents overlapping runs. The ephemeral containers
stop when the command exits. The public model download
persists under `/mnt/bb/lane-scratch/be-edge/models`. The conformance run excludes
container startup/model download time.
The recorded full suite took 2 ms in replay and 850 ms live on bb2, with exact
usage checked independently for the text, tool and result completions.
