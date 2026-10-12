# Global identity JSON goldens

`golden.json` records Cloud's identity handler-test JSON at the commit in
`source.commit`. Each case names its handler and test file; `source.files`
records their Git blob hashes. The device/session tests supply the verified
`oauth-user`, website origin, PKCE flow, seconds-based intervals and null display
name. The org tests supply the team/owner result. The agent tests supply the
22-bit challenge, hashcash request and agent registration. The mint test supplies
the birth/certificate wire encoding and deterministic test signing seeds.

The responses were captured from those pinned handlers using test transactions
and the same test inputs. Random identifiers, tokens and signatures are frozen
once; client-generated proofs use the named request placeholders. The session
timestamp is an ISO string, while the agent challenge and certificate expiry
are Unix seconds. There is no parallel schema or live API dependency.

The signup fake substitutes the CLI's fresh public key into the golden
certificate and signs it with the deterministic root from Cloud's mint test.
Every other registration field stays identical to the golden response.
The challenge fake rebases its ten-minute expiry to runtime while retaining the
recorded expiry in the golden. A regression checks dates beyond that expiry.

Refresh these fixtures together with the SDK's Cloud contract pin, after reading
the new handler tests. Keep their source file, commit and blob records current.
Do not replace server JSON with SDK-shaped aliases or local identity projections.

`cargo test -p acyclic-plugin --test identity_contract` builds the real CLI and
runs the isolated fake-Global process tests. The signup case requires c5d's
retained-key agent registration implementation in addition to #361's MCP base.
