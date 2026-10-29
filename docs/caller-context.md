# Caller context

`acyclic_auth::contract::CallerContext` is the shared public projection for direct
and cross-account fetch and private RPC, including protected rollout. Its
`caller` is the original verified customer. Its `audience` is the exact recipient
account/namespace/id, independently of the customer account. `delegation` lists
the platform authorities acting for the customer, in forwarding order. Each
authority retains its own expiry, every signing-key identity/epoch and every
capability/certificate-parent block revocation dependency. Effective expiry is
the minimum across all authorities. Identity components remain exact bytes.

The host supplies this context per invocation beside the canonical fetch/RPC
context. Guest request headers and environment variables cannot populate it.
An invocation-scoped `capability_handle` is the only capability material exposed
to the guest. The verified capabilities and all signing, account and refresh
secrets stay with the host. The existing Actors `CallerCapability` transports a
host handle; the dispatch producer must attach this structured context when it
adopts the contract. The older `AdmittedCaller.accountId` describes the recipient
account and must not stand in for the original customer across accounts.

Decoding validates representation, not authenticity. The dispatch producer must
authenticate the original customer's capability, each platform authority and the
delegation binding over the exact dispatch payload and audience before guest
entry. Two unrelated valid credentials do not prove delegation. Cross-account
dispatch retains the original identity and checks each account independently.

Every privileged handle use resolves its invocation owner and checks current
trust, all exact revocation dependencies, exclusive expiry, audience, original
delegated scope, requested command/resource and current resource-owner policy.
Missing trust or delegation produces a typed refusal. A saved allow never
authorizes another use. The original `auth::operation::Context` supplies deadline
and cancellation. Dropping the handle/dispatch owner must settle pending uses
with a typed unavailable/cancelled result through that owner's completion event.

Projection and decoding take work proportional to supplied identity and
delegation/certificate bytes. Accessing the admitted fields is O(1), with no
syscalls, durability barriers or network requests. This contract adds no cache,
timer, worker or history. Actual dispatch/handle lookup and guest-read controls
remain obligations of the host producer; current revocation index costs remain
separately qualified. Codec controls reject missing identity/dependencies,
malformed block ids, unsupported versions, absent handles and a widened expiry.
Producer controls must additionally substitute customer, delegate, namespace,
audience and payload; revoke or expire either account; cancel or drop the owner;
and show that unrelated-key revocation preserves the original capability.
