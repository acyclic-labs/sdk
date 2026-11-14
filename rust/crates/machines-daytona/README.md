# Protected Daytona Machines adapter

The adapter uses the host's shared CredentialResolver::bind for every origin.
DaytonaConfig contains a canonical SecretRef, immutable snapshot enrollment,
platform and network-policy commitment; it never contains a credential value.
The sandbox's toolboxProxyUrl must bind successfully before toolbox I/O.

The v0 toolbox supplies whole-file download and multipart upload-v2. Native
range reads, range writes and selected-volume attachment are Unsupported. The
shared Machines file seam composes whole-file transfer for callers with exclusive
file ownership. It does not claim native errno or positional-write atomicity.
Downloads remain lazy HTTP byte streams under the original file deadline. The
shared range reader discards prefix chunks with bounded memory and cancels the
unread suffix as soon as its byte credit is filled. A whole-file-only provider
still transfers the prefix; it does not acquire a native seek guarantee.

Create uses an acyclic-{key} name and versioned request/contract labels. Lost
replies retain uncertainty; explicit recover uses the original key label. There
is no readiness polling or automatic redispatch. Lifecycle events, operations,
checkpoint/fork, usage, destroy receipts and binary duplex exec are Unsupported.

Shell execution selects the textual request/response API and labels its output
as combined text. Binary stdin and binary duplex exec refuse explicitly. Lost
terminal replies remain indeterminate and are never redispatched.

The shared workspace layer copies the original admitted logical volume at bind
time whenever attachment is Unsupported. It publishes file changes through the
existing workspace transaction facade before a turn settles, using that turn's
original identity. Native attaching providers keep their physical binding.
