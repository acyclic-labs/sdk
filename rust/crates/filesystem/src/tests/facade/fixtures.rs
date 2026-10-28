use super::*;

pub(super) type TestVolume = Volume<
    crate::StreamAuthorityStore<acyclic_stream::MemoryStream>,
    crate::LogicalObjectStore<acyclic_objects::MemoryObjects>,
>;

pub(super) type TestCheckout = Checkout<
    crate::StreamAuthorityStore<acyclic_stream::MemoryStream>,
    crate::LogicalObjectStore<acyclic_objects::MemoryObjects>,
>;

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

pub(super) async fn fixture(
    config: VolumeConfig,
    cancel: &CancellationToken,
) -> TestResult<(TestVolume, TestCheckout)> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let volume = Fs::memory().create_volume(config, B, cancel).await?.value;
    let checkout = open_checkout(&volume, writable_pinned(), cancel).await?;
    Ok((volume, checkout))
}

pub(super) async fn create(
    checkout: &mut TestCheckout,
    name: &str,
    bytes: OwnedBytes,
    cancel: &CancellationToken,
) -> TestResult<FileId> {
    Ok(checkout
        .create(
            AuthoredMutation::CreateFile {
                path: path(name)?,
                bytes,
                metadata: FileMetadata::default(),
            },
            B,
            cancel,
        )
        .await?
        .value)
}

pub(super) async fn commit<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    checkout: &mut Checkout<A, O>,
    cancel: &CancellationToken,
) -> TestResult {
    checkout
        .commit(
            OperationId::new(),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            B,
            cancel,
            allow::<FsError>,
        )
        .await?;
    Ok(())
}

pub(super) async fn read(
    checkout: &mut TestCheckout,
    id: FileId,
    offset: u64,
    length: u64,
    cancel: &CancellationToken,
) -> TestResult<OwnedBytes> {
    Ok(checkout
        .read_file_range(id, ByteRange { offset, length }, B, cancel)
        .await?
        .value
        .bytes)
}

pub(super) async fn open_checkout<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>,
    mode: CheckoutMode,
    cancel: &CancellationToken,
) -> Result<Checkout<A, O>, OperationFailure<FsError>> {
    volume
        .checkout(GenerationSelector::Head, mode, B, cancel)
        .await
        .map(|receipt| receipt.value)
}
