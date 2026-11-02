# Captured Gemini responses

These are unchanged response bodies from a real `gemini-3.8-flash` session
and its later HTTP 400 tool-continuation refusal, captured on 2026-11-02.
The producing SDK source was `f2a8def7d7411353485c847bc1f8a85b4a0a6162`;
the consuming application source was `868602b489`.

| Body | SHA-256 |
|---|---|
| `gemini-captured-session.sse` | `187508e155d51a1cb25420fa2444cef2a5b72cc657e7ffec87fc6e1fab16de4e` |
| `gemini-captured-refusal.json` | `1d7a6ad41f0e542a88c2098ddb296f68d6bc6b198b8afffb6d2a1d535d9a8f18` |

The original response headers were not retained. The real-socket regression
adds local HTTP framing to replay these bodies; that framing is not observed
provider evidence. No request, credential or reconstructed response is included.
The regression exercises ordinary registry selection, HTTP dispatch, stock
execution, journal replay and the shared usage projection. Fresh-executor
replay over the same in-process storage does not qualify process restart,
durable reopen, native execution or distributed recovery.
