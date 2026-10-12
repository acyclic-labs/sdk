# Objects adapter

The `objects` feature supplies `ObjectContentStore`, binding an Objects bucket and
Harness volume to a trusted provider identity and signed owner authority.
Content-addressed references verify descriptor, bytes and metadata. Operation-bound
upload intents preserve retry identity; references alone grant no read access.

`ObjectContentMountResolver` routes references across registered owner-authorized
stores. Durable services must retain accepted objects and reconcile pending
intents; this adapter does not establish indefinite residency.
