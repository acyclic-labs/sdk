//! The same downstream suite over actual browser authority and object storage.
use acyclic_fs::{
    EmbeddedCapabilities, StreamAuthorityStore,
    browser::IndexedDbObjectStore,
    conformance::{Fixture, bench},
};
use acyclic_stream::{
    BrowserStream, BrowserStreamLimits,
    testing::{Counted, RequestLedger},
};
use std::{error::Error, sync::Arc};

type BrowserFixture =
    Fixture<StreamAuthorityStore<Counted<BrowserStream>>, Counted<IndexedDbObjectStore>>;

async fn fresh(name: &str) -> Result<BrowserFixture, Box<dyn Error>> {
    let authority_name = format!("acyclic-fs-authority/{name}");
    for database in [name, &authority_name] {
        super::delete_database(database).await.map_err(|error| {
            std::io::Error::other(format!("delete browser fixture {database}: {error:?}"))
        })?;
    }
    // Validate/create the object schema before opening the authority journal,
    // matching the ordering of browser::open_authority without erasing Counted.
    let objects = IndexedDbObjectStore::open(name, 1024 * 1024).await?;
    let stream = BrowserStream::open(&authority_name, BrowserStreamLimits::default()).await?;
    let requests = RequestLedger::new();
    let authority =
        StreamAuthorityStore::new(Arc::new(Counted::with_ledger(stream, requests.clone())));
    let objects = Counted::with_ledger(objects, requests.clone());
    Ok(Fixture::new(
        acyclic_fs::Fs::new(authority, objects, EmbeddedCapabilities { durable: true }),
        requests,
    ))
}

async fn backend() -> Result<BrowserFixture, Box<dyn Error>> {
    fresh("acyclic-fs-public-conformance").await
}
acyclic_fs::conformance::suite!(browser_public_conformance, backend);

#[wasm_bindgen_test::wasm_bindgen_test]
async fn browser_public_conformance_benchmark() -> Result<(), Box<dyn Error>> {
    let backend = fresh("acyclic-fs-public-conformance-bench").await?;
    let measured = bench(&backend).await?;
    assert_eq!(measured.reads, 256);
    assert_eq!(measured.requests.control(), 0);
    Ok(())
}
