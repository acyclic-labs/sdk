//! Count the immutable-object seam without replacing the caller's work ledger,
//! decoded cache, native allocations, publication hold or collection owner.
use crate::AsyncObjectStore;
use acyclic_stream::testing::{Counted, RequestClass};

impl<P: AsyncObjectStore> AsyncObjectStore for Counted<P> {
    crate::async_storage::forward_object_store!(self => self.inner();
        requires_child_first_publication,
        maximum_reference_bytes,
        prepare_hashed,
        supports_decoded_cache,
        decoded_cache_lookup,
        collection,
        collection_sweeps,
        put_stored { self.record(RequestClass::Data); },
        read_many_stored { self.record(RequestClass::Data); },
        read_stored { self.record(RequestClass::Data); },
        put { self.record(RequestClass::Data); },
        put_hashed { self.record(RequestClass::Data); },
        put_many { self.record(RequestClass::Data); },
        put_many_hashed { self.record(RequestClass::Data); },
        flush_before_publish { self.record(RequestClass::Data); },
        historical_generation_evidence { self.record(RequestClass::Data); },
        read { self.record(RequestClass::Data); },
        read_many { self.record(RequestClass::Data); },
        contains { self.record(RequestClass::Head); },
    );
}
