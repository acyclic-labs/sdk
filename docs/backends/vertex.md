Enable `endpoint-presets`, register `endpoints::vertex(project, location)` with
exact model capacities, and supply a short-lived OAuth access token through
explicit credentials. Vertex's first-party `endpoints/openapi/chat/completions`
route uses the shared Chat adapter. Refresh the access token outside the adapter
and resolve the immutable selection again. Supported tools/models are limited to
Vertex's compatible route. Configure a verified counter for automatic compaction.

Live steps (coordinator only): obtain a token for a project authorized for Vertex
prediction; capture every common inference case at the configured project/location
route with a supported `google/<model>` selection. Retain requests, statuses,
headers, raw SSE and body SHA-256 hashes. Declare exact usage in the common
manifest and run `inference-qualify` unchanged. No cloud pass is claimed without
the corresponding captured-real family.

Reference: https://cloud.google.com/vertex-ai/generative-ai/docs/multimodal/call-vertex-using-openai-library
