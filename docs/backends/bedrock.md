Qualification: **unqualified** against AWS Bedrock. The shared Chat adapter passes
the complete real llama.cpp suite; no Bedrock cloud pass is claimed.

Enable `endpoint-presets` and register `endpoints::bedrock("us-east-1")` with
explicit model capacities and a Bedrock bearer API key. This uses AWS's first-party
OpenAI-compatible Chat route, with the same shared adapter as other Chat servers.
It supports the models/features offered on that route in the selected region;
Converse/EventStream and automatic IAM credential discovery are outside this
configuration. Automatic compaction requires a verified counting endpoint.

Live steps (coordinator only): select a Chat-compatible model in the target region
and capture all common inference cases from
`https://bedrock-runtime.us-east-1.amazonaws.com/openai/v1/chat/completions`, using
`Authorization: Bearer <Bedrock API key>`. Retain raw response bytes, statuses,
headers and requests with their SHA-256 hashes, and exact usage in a common
manifest; run `inference-qualify` unchanged. No cloud pass is claimed until this
captured-real family is available. Every live Anthropic run must select
`claude-haiku-5-5`.

Reference: https://docs.aws.amazon.com/bedrock/latest/userguide/inference-chat-completions.html
