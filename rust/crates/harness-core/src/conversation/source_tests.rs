use super::*;
use acyclic_stream::OwnedBytes;
use futures::{Stream, channel::mpsc};

// A real channel owns both readiness and producer termination. An explicit
// terminal item permits EOF; closing the producer without it refuses staging.
struct Producer {
    input: mpsc::UnboundedReceiver<Option<OwnedSourceChunk>>,
    finished: bool,
    original: Option<NativeMaterializationWork>,
}

impl Producer {
    fn poll_chunk(
        &mut self,
        context: &mut Context<'_>,
        cancellation: &CancellationToken,
    ) -> Poll<std::io::Result<Option<OwnedSourceChunk>>> {
        if cancellation.is_cancelled() {
            return Poll::Ready(Err(std::io::ErrorKind::Interrupted.into()));
        }
        if self.finished {
            return Poll::Ready(Ok(None));
        }
        match Pin::new(&mut self.input).poll_next(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Some(chunk))) => Poll::Ready(Ok(Some(chunk))),
            Poll::Ready(Some(None)) => {
                self.finished = true;
                Poll::Ready(Ok(None))
            }
            Poll::Ready(None) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "content producer ended without its terminal item",
            ))),
        }
    }
}

impl ContentSource for Producer {
    fn poll_read(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        _destination: &mut [u8],
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(if self.finished {
            Ok(0)
        } else {
            Err(std::io::Error::other("producer requires owned reads"))
        })
    }

    fn poll_read_owned(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        _maximum: usize,
        _budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Poll<std::io::Result<Option<OwnedSourceChunk>>> {
        self.get_mut().poll_chunk(context, cancellation)
    }

    fn poll_read_owned_with_native_work(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        _maximum: usize,
        original: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Poll<Result<Option<OwnedSourceChunk>, FsError>> {
        let producer = self.get_mut();
        if !producer
            .original
            .as_ref()
            .is_some_and(|expected| expected.shares_allocation_context(original))
        {
            return Poll::Ready(Err(BlobBuildError::NativeSourceUnavailable.into()));
        }
        producer
            .poll_chunk(context, cancellation)
            .map(|result| result.map_err(|cause| FsError::from(BlobBuildError::Source(cause))))
    }
}

#[tokio::test]
async fn source_staging_preserves_chunks_and_rejects_lost_producer()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_fs::model::{CheckoutMode, GenerationSelector, Lifecycle, VolumeConfig};
    let cancellation = CancellationToken::new();
    let volume = acyclic_fs::Fs::memory()
        .create_volume(
            VolumeConfig::portable(Lifecycle::Ephemeral),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let checkout = volume
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let (sender, input) = mpsc::unbounded();
    let bytes = OwnedBytes::from_static(b"original streamed content");
    sender.unbounded_send(Some(OwnedSourceChunk::claimed(
        bytes.clone(),
        bytes.len() as u64,
    )))?;
    sender.unbounded_send(None)?;
    drop(sender);
    let mut source = Producer {
        input,
        finished: false,
        original: None,
    };
    let mut adapter = ContentSourceAdapter::new(Pin::<&mut Producer>::new(&mut source));
    let staged = checkout
        .stage_content(&mut adapter, 64, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let mut direct = std::io::Cursor::new(bytes);
    let expected = checkout
        .stage_content(&mut direct, 64, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    assert_eq!(staged, expected);

    let (sender, input) = mpsc::unbounded();
    sender.unbounded_send(Some(OwnedSourceChunk::claimed(
        OwnedBytes::from_static(b"incomplete"),
        10,
    )))?;
    drop(sender);
    let mut lost = Producer {
        input,
        finished: false,
        original: None,
    };
    let mut adapter = ContentSourceAdapter::new(Pin::<&mut Producer>::new(&mut lost));
    assert!(
        checkout
            .stage_content(&mut adapter, 64, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .is_err()
    );

    // The native projection retains the original backing, work and cancellation.
    // A foreign ledger refuses before consuming the same producer's next chunk.
    let work = NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 8 * 1024 * 1024),
        &cancellation,
    )
    .map_err(|(cause, _)| cause)?;
    let foreign = NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 8 * 1024 * 1024),
        &cancellation,
    )
    .map_err(|(cause, _)| cause)?;
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let bytes = io.encode(&"native source", 64, false)?;
    let address = bytes.as_ptr();
    let retained = bytes.retained_capacity().unwrap_or(0) as u64;
    let (sender, input) = mpsc::unbounded();
    sender.unbounded_send(Some(OwnedSourceChunk::intrinsic(bytes, retained)))?;
    let mut native = Producer {
        input,
        finished: false,
        original: Some(work.clone()),
    };
    let mut adapter = ContentSourceAdapter::new(Pin::<&mut Producer>::new(&mut native));
    let mut destination = [0; 8];
    assert!(matches!(
        adapter
            .read_with_native_work(&mut destination, &work, &cancellation)
            .await,
        Err(FsError::BlobBuild(BlobBuildError::NativeSourceUnavailable))
    ));
    assert!(
        adapter
            .read_owned_with_native_work(64, &foreign, &cancellation)
            .await
            .is_err()
    );
    let chunk = adapter
        .read_owned_with_native_work(64, &work, &cancellation)
        .await?
        .ok_or("native chunk was not transferred")?;
    assert_eq!(chunk.bytes.as_ptr(), address);
    assert!(chunk.bytes.matches_meter(&work.runtime_meter()));
    cancellation.cancel();
    assert!(
        adapter
            .read_owned_with_native_work(64, &work, &cancellation)
            .await
            .is_err()
    );
    Ok(())
}
