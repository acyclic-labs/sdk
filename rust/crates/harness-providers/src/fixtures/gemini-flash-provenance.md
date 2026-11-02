# Recorded Flash responses

These unmodified HTTP response bodies were captured on 2026-11-02 from
`gemini-3.8-flash` while running GraphCoder at source
`f2a8def7d7411353485c847bc1f8a85b4a0a6162` on Linux. Authentication was
optional at the selected HTTP route; upstream credentials stayed in the
transport. No credential or private route is included here.

| File | Original response | SHA-256 |
|---|---|---|
| gemini-flash-shell.sse | Successful streamed shell call, `call_81218`, including its original thought signature | `87f70330ba1528b23b29d422bc86e667c3718a0ca95e0ade863a179099eea1b8` |
| gemini-flash-answer.sse | Successful streamed reply from the same model | `187508e155d51a1cb25420fa2444cef2a5b72cc657e7ffec87fc6e1fab16de4e` |
| gemini-flash-count.json | Successful token-count response from the shell session | `e582dc1953c31ca6c7fd868bd8bd675298ec1c1bade8c8f68249299504d0c822` |
| gemini-flash-missing-signature.body | Original HTTP 400 refusing the unsigned shell continuation | `1d7a6ad41f0e542a88c2098ddb296f68d6bc6b198b8afffb6d2a1d535d9a8f18` |

`gemini-flash-catalog.json` retains the actual run's model, capacity, feature,
authentication and counter configuration. Its private route is replaced with
a public loopback placeholder and JSON whitespace is regenerated. It is loaded
through `Registry::load_catalog`, including the absence of a native tool profile
in that original catalog; it does not substitute the built-in catalog defaults.

The shell regression uses the original user prompt and command. HTTP delivery
is replayed; the registry, native host, shell execution, durable conversation
and request projection are production paths. The reply and count are recorded
delivery inputs, not new provider evaluations of the regression's request.
The original failed continuation omitted the signature and received HTTP 400.
The replay selects that original refusal when the emitted continuation loses
the signature, rather than answering an invalid request successfully.
