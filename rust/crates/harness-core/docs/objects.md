# Objects adapter

The `objects` feature supplies `ObjectContentStore`, binding an Objects bucket and
Harness volume to a trusted provider identity and signed owner authority.
Content-addressed references verify descriptor, bytes and metadata. Operation-bound
upload intents preserve retry identity; references alone grant no read access.

Uploads seal content, its binding record and its operation intent once, then
publish them in one dependency-ordered Objects batch. Each outcome must verify
before the original root retain makes the complete closure durable. This uses
two provider calls instead of three serial puts and a retain; physical requests
and durability barriers depend on the provider. The borrowed input still needs
one owned payload copy. Batching adds no task or separate flush.

`ObjectContentMountResolver` routes references across registered owner-authorized
stores. Durable services must retain accepted objects and reconcile pending
intents; this adapter does not establish indefinite residency.
