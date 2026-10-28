# Shared credential snapshots

`acyclic_auth::credential::CredentialProvider::new` resolves opaque credentials
afresh. Sources that own trustworthy revision and expiry data can instead share
one `CredentialCache` across their product clients:

```rust,ignore
let cache = CredentialCache::new(
    move |rejected: Option<CredentialLease>| source.refresh(rejected),
    move || CredentialValidity {
        now_unix_seconds: clock.now(),
        revision: source_state.current_revision(),
    },
);
let credential = CredentialProvider::from(cache.clone());
// Supply credential to the existing product credential-provider constructor.
```

Refresh returns `CredentialSnapshot::new(token, revision, expires_at)`. The
custodian supplies validity; decoding an unverified token does not establish it.
Partition each cache by source, account, audience and transport authentication
context. The validity observer must be nonblocking and do no file/network IO.
An external source watcher must publish changed/unavailable revision state and
call `invalidate`; polling a credential file on every observation defeats the
warm path. Unknown expiry uses the raw callback instead of a fabricated TTL.

Native warm reads load an atomic snapshot and check revision and half-open
absolute expiry without acquiring the refresh mutex. Browser ownership uses Rc
and the same validity/refresh rules. Refresh has one shared future per cohort,
including its original error. Cancelling one waiter leaves other waiters alive;
dropping the last releases source IO. Invalidation aborts an obsolete flight.
There is no background task, retry policy or stale credential fallback.

Transports using snapshots directly retain the lease used for each request.
On a server rejection, `cache.reject(&lease)` invalidates only that exact current
snapshot. A later refresh receives its rejected lease and must replace the
bearer; returning the same rejected bytes refuses. A delayed rejection for an
older snapshot cannot evict a published replacement. Custodian source changes
use `invalidate`, including any account/audience/policy change that invalidates
the credential itself. Credential revisions do not certify product policy.

The caller owns the absolute operation deadline and cancellation across resolve,
connect and response consumption. Check expiry again at the transport boundary;
holding a lease never extends authority. This milestone does not change product
channel pools, TLS verification, pre-buffer accounting, header construction or
Codex file watching. Those consumers remain explicit follow-up work.

Validation vectors cover cold/warm cohorts, shared failures, exact expiry, source
revision changes, unavailable source refusal before callback IO, cancellation,
invalidated/stale completion, exact-snapshot rejection and redacted Debug. They
use synthetic source credentials, not provider-response fixtures. The current
qualification is compile-only; no runtime or performance result is claimed.

Measurement plan: compare the baseline and adopted source pins at 1/8/64/100000
concurrent warm reads and refresh storms. Count callback starts separately from
file opens/parses/locks and issuer/token requests; count prepare/count/generate
and actual HTTP requests per turn. Exercise external CLI/account replacement,
expiry, 401, revision/audience change, shared failure and one/last waiter cancel.
Bypassing the cache is the control for per-call source work. Transport adoption
must also count cold dial/TLS and member reuse and verify absolute deadline and
charges before allocation/decode against the original operation budget.
