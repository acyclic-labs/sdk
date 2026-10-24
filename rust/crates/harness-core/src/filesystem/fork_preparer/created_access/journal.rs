//! One parsed fork report pinned to one authenticated preparation generation.
use super::*;

pub(super) struct CreationJournal {
    workspace: WorkspaceRef,
    generation: GenerationRef,
    pub(super) seed: ForkSeed,
    pub(super) binding: [u8; 32],
}

impl CreationJournal {
    pub(super) async fn bind<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        host: &FilesystemHost<A, O>,
        request: &ForkRequest,
        maximum: u64,
        io: &PreparationIo,
    ) -> Result<Self> {
        let workspace = preparation_journal(&host.provider, request.operation_id, Some(io))?;
        let opened = host.open(&workspace, Some(io)).await?;
        let head = io.sdk(opened.head(io, io.cancellation()).await)?;
        let generation = bounded_io::generation_ref(&host.provider, head.id(), Some(io))?;
        let claim: ForkRequest =
            read_pinned(host, &workspace, &generation, "/request.json", maximum, io).await?;
        if claim != *request {
            return Err(crate::executor::stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Conflict,
                "fork operation is claimed by another request",
            ));
        }
        let report: ForkReport =
            read_pinned(host, &workspace, &generation, "/report.json", maximum, io).await?;
        report.validate(&mut Some(&mut io.history_io()))?;
        if report.request != *request {
            return Err(crate::executor::stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Conflict,
                "fork report does not match its claimed request",
            ));
        }
        let seed = report.into_seed(&mut Some(&mut io.history_io()))?;
        let binding = seed_digest(&seed, maximum, Some(io))?;
        Ok(Self {
            workspace,
            generation,
            seed,
            binding,
        })
    }

    pub(super) async fn verify<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        host: &FilesystemHost<A, O>,
        io: &PreparationIo,
    ) -> Result<()> {
        // A content-addressed generation seals both records at the same cut.
        // Reopen through this host on every use; neither a different store nor
        // an advanced journal may borrow the parsed report's authority.
        let opened = host.open(&self.workspace, Some(io)).await?;
        let head = io.sdk(opened.head(io, io.cancellation()).await)?;
        let generation = bounded_io::generation_ref(&host.provider, head.id(), Some(io))?;
        if generation != self.generation {
            return Err(crate::executor::stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Unauthorized,
                "original creation journal generation changed",
            ));
        }
        Ok(())
    }
}

async fn read_pinned<A, O, T>(
    host: &FilesystemHost<A, O>,
    workspace: &WorkspaceRef,
    generation: &GenerationRef,
    path: &str,
    maximum: u64,
    io: &PreparationIo,
) -> Result<T>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    T: bounded_io::PreparationRecord,
{
    let bytes = host
        .read(workspace, Some(generation), path, maximum, Some(io))
        .await?;
    let held = io.guard_bytes(&bytes)?;
    let value = bounded_io::retain_decoded(io.decode(
        std::marker::PhantomData::<T>,
        &bytes,
        crate::wire::ErrorCode::Storage,
    )?)?;
    drop(bytes);
    drop(held);
    Ok(value)
}
