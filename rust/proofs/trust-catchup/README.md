# Trust catch-up boundary

Run `python3 rust/proofs/qualify.py trust-catchup` from the repository root.

The existing `trust_feed::tests::losing_one_account_follow_fences_everything`
control runs the production `TrustFeed` over original `MemoryStream` records
signed by `Issuer` and `AccountRoot`. After a real follow ends, it appends a
revocation and reconnects from the retained offset. The original record is held
behind a single-use channel: its presence in the observed tail must prevent
serving stale trust. Releasing it must apply the revocation; dropping its
producer must terminate the run with `StreamError::Unavailable`.

The retained mutation changes the production `Event::Opened` catch-up decision
to serve immediately. The unchanged test must fail its typed trust query before
the held record arrives. No record or alternate trust driver is fabricated.

The hold stores one receiver per selected follow and consumes it once, without
a timer or readiness retry. This establishes the reconnect serving boundary and
the fixture's producer-loss path. It does not certify all trust-feed complexity,
native transport failure modes or the separate full lossy schedule in Auth.
