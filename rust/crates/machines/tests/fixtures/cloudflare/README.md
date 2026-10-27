# Native local Container snapshot capture

`native-snapshot-20261027.json` contains actual HTTP response bodies captured on
bb2 at 2026-10-27T09:11:56–09:11:58Z. The source was signed commit
`07e558968fa4926b9262562885b5b48be2620de0`, with Wrangler 4.148.0 and
Cloudflare Sandbox 1.0.3. The endpoint was account-free loopback workerd with
native local Containers, not a simulated response server.

Capture command, with the local Worker running:

```sh
node scripts/capture-machines-cloudflare.mjs http://127.0.0.1:15440 OUTPUT.json 07e558968fa4926b9262562885b5b48be2620de0
```

The original JSON was extracted without editing from the capture markers in
bb2 job `1793091808662-142237-afe1e7af`, durable log
`/home/var/log/buildbox/1793091777813-142237-e0de4a9f.log`.
Its SHA-256 is `f25c60e849bf159462bc78eac564ce6e760c79053c128d5096f0cd8e761b8ba9`.
All eleven response body hashes were checked after extraction. Request bodies
are probe inputs; response bodies are original bytes, base64 encoded. Fetch
exposes decoded bodies, so recorded content-encoding headers do not imply that
the recorded bytes are compressed wire bytes. No credentials were used.

The child restored `before\n` while its running parent read `after\n`.
Both observed machines were stopped. The Worker API has no snapshot deletion:
snapshot data remains under native provider retention, not physical retirement.
The independent unchanged lifecycle and shell suites passed in 14.88 seconds in
this same job. This capture is a snapshot protocol/provenance record, not a replay
qualification of either complete suite or the hosted Cloudflare service.
