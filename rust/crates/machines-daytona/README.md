# Protected Daytona Machines adapter

The adapter uses the host's shared CredentialResolver::bind for every origin.
DaytonaConfig contains a canonical SecretRef, immutable snapshot enrollment,
platform and network-policy commitment; it never contains a credential value.
The sandbox's toolboxProxyUrl must bind successfully before toolbox I/O.

The v0 toolbox supplies whole-file download and multipart upload-v2. Native
range reads, range writes and selected-volume attachment are Unsupported. The
shared Machines file seam composes whole-file transfer for callers with exclusive
file ownership. It does not claim native errno or positional-write atomicity.

Create uses an acyclic-{key} name and versioned request/contract labels. Lost
replies retain uncertainty; explicit recover uses the original key label. There
is no readiness polling or automatic redispatch. Lifecycle events, operations,
checkpoint/fork, usage, destroy receipts and binary duplex exec are Unsupported.

Hosted copy-in/commit-out is incomplete. Opening a shell workspace without an
admitted physical selected-volume binding continues to refuse. This adapter
is not yet sufficient for Cloud Swarm shell/file workspace admission.
