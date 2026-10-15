# Model requests and prefixes

`PreparedModelRequest` binds canonical bytes to model and tool revisions, schemas,
roles, messages and complete ordered tool exchanges. Input and output have
separate admitted bounds. Replay validates the retained request before completion
or reconciliation.

Fork publication freezes a completed exchange before child activation. Children
share an immutable parent prefix and retain their local suffix. Reconstruction
checks descriptors, canonical bytes, grants, counts, cycles and ordered digests;
a reference alone does not grant read authority.
