//! The public fork/create contract across the primitives sharing provenance.
use acyclic_machines::{
    CreateMachine, ForkFidelity, ForkSource, Image, MachinesProvider, MutationOutcome,
    SimulatedMachines,
};
use acyclic_stream::{
    AppendOutcome, IdempotencyKey, LocalStream, MemoryStream, Provenance, Stream, StreamError,
    StreamPath, StreamProvider,
};
use futures::TryStreamExt as _;
use std::{num::NonZeroU32, sync::Arc};

async fn streams_and_actors<P: StreamProvider + ?Sized>(
    provider: Arc<P>,
) -> Result<(), Box<dyn std::error::Error>> {
    let parent = Stream::new(provider.clone(), "uniform/parent")?;
    parent.append("parent", IdempotencyKey::unique()).await?;
    let key = IdempotencyKey::unique();
    let forked = parent
        .fork("uniform/forked", Some(0), Vec::new(), key.clone())
        .await?;
    let original = forked
        .fork_receipt()
        .ok_or(StreamError::InvalidResponse)?
        .clone();
    assert_eq!(original.forked_at, 0);
    assert_eq!(original.destination, *forked.path());
    let created = Stream::new(provider.clone(), "uniform/created")?;
    assert!(created.fork_receipt().is_none());
    for stream in [created, forked] {
        assert!(matches!(
            stream
                .append_at("private", 0, IdempotencyKey::unique())
                .await?,
            AppendOutcome::Committed(_)
        ));
        assert_eq!(stream.tail().await?, 1);
        let head = stream.head().await?;
        let records = stream.read(0, 2).await?.try_collect::<Vec<_>>().await?;
        let [record] = records.as_slice() else {
            return Err(StreamError::InvalidResponse.into());
        };
        assert_eq!(record.value.as_slice(), b"private");
        assert_eq!(&record.origin, stream.path());
        assert_eq!(head.last.as_ref(), Some(record));
        assert_eq!(stream.clone().fork_receipt(), stream.fork_receipt());
    }
    // The child can advance, but its retained original fork receipt cannot.
    let retry = parent
        .fork("uniform/forked", Some(0), Vec::new(), key.clone())
        .await?;
    assert_eq!(retry.fork_receipt(), Some(&original));
    assert!(matches!(
        parent
            .fork("uniform/forked", Some(1), Vec::new(), key)
            .await,
        Err(StreamError::IdempotencyMismatch)
    ));
    let origin: Provenance<_, _> = retry
        .head()
        .await?
        .origin
        .ok_or(StreamError::InvalidResponse)?;
    assert_eq!(&origin.source, parent.path());
    assert_eq!(origin.inherited.forked_at, 0);
    assert_eq!(origin.inherited.commit_id, original.commit_id);

    acyclic_actors::conformance::fork_uniform(provider, StreamPath::new("uniform/actors")?).await?;
    Ok(())
}

#[tokio::test]
async fn fork_uniform() -> Result<(), Box<dyn std::error::Error>> {
    streams_and_actors(Arc::new(MemoryStream::default())).await?;
    let directory = tempfile::tempdir()?;
    let local = LocalStream::open(directory.path(), Default::default()).await?;
    streams_and_actors(Arc::new(local)).await?;

    let machines = SimulatedMachines::default();
    let key = |n: u8| {
        acyclic_machines::IdempotencyKey::parse(&format!(
            "00000000-0000-0000-0000-0000000000{n:02x}"
        ))
    };
    let MutationOutcome::Created(parent) = machines
        .create(CreateMachine::new(
            key(1)?,
            Some(Image::custom([7; 32])?),
            [8; 32],
        ))
        .await?
    else {
        return Err("create did not return a Machine observation".into());
    };
    let MutationOutcome::Forked {
        provenance,
        machines: children,
    } = machines
        .fork_machine(parent.id, NonZeroU32::MIN, key(2)?)
        .await?
    else {
        return Err("fork did not return Machine observations".into());
    };
    let expected: Provenance<_, _> = Provenance {
        source: ForkSource::Machine(parent.id),
        inherited: ForkFidelity::MemoryAndDisk,
    };
    assert_eq!(provenance, expected);
    let [child] = children.as_slice() else {
        return Err("fork changed the child count".into());
    };
    for machine in [&parent, child] {
        assert_eq!(
            machines.inspect_machine(machine.id).await?.contract,
            parent.contract
        );
        assert_eq!(&machines.inspect_machine(machine.id).await?, machine);
    }
    assert_ne!(parent.id, child.id);
    Ok(())
}
