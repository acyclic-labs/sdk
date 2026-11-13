//! Prefix selection keeps producer positions in memory, never in public rows.
use super::*;
use acyclic_fs::{
    LazyDirectoryCursor, LazySnapshotRef,
    kernel::{KernelAccounting, NativeAccounting, Retained},
};

pub(in crate::filesystem) struct DirectoryPrefixes<'io> {
    positions: Vec<DirectoryPosition>,
    _custody: DecodeReservation<'io>,
    cut: Retained<LazySnapshotRef>,
    config: acyclic_fs::VolumeConfig,
}

impl DirectoryPrefixes<'_> {
    pub(in crate::filesystem) fn encoded_len(
        &self,
        index: usize,
        name: &str,
        path: &str,
    ) -> Option<usize> {
        let directory = path.len().checked_add(1)?;
        let bytes = match self.positions.get(index)? {
            DirectoryPosition::Source(source) => {
                LazyDirectoryCursor::source_encoded_len(&self.cut, directory, source)
            }
            DirectoryPosition::Authored => {
                LazyDirectoryCursor::authored_encoded_len(&self.cut, directory, name.len())
            }
            DirectoryPosition::Unavailable | DirectoryPosition::Removed => return None,
        };
        cursor::payload_encoded_len(bytes)?.checked_add("lazy:".len())
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(in crate::filesystem) fn directory_prefixes<'io>(
        &self,
        generation: &GenerationRef,
        positions: Vec<DirectoryPosition>,
        io: &'io PreparationIo,
    ) -> Result<DirectoryPrefixes<'io>> {
        let custody = io.reserve_decode(position_bytes(positions.capacity()))?;
        let positions = (positions, custody);
        let view = self
            .private_view
            .as_ref()
            .ok_or_else(|| io.invalid("private source binding is unavailable"))?;
        if generation.as_resource().provider() != &self.provider
            || generation.as_resource().version() != Some(super::super::private_view::FORMAT)
        {
            return Err(io.invalid("directory prefix belongs to another view"));
        }
        let owner = io.account(NativeAccounting::take)?;
        let mut ctx = KernelAccounting::NativeOriginal {
            work: &owner.work,
            cancellation: io.cancellation(),
        };
        let result = ctx
            .decode_json::<LazySnapshotRef>(generation.as_resource().key())
            .map_err(|error| match error {
                acyclic_fs::kernel::MetadataDecodeError::Work(error) => Error::from(error),
                acyclic_fs::kernel::MetadataDecodeError::Codec(_) => {
                    io.invalid("invalid lazy generation")
                }
            });
        drop(ctx);
        io.account(|ctx| owner.restore(ctx))?;
        let cut = result?;
        if cut.workspace_id != view.workspace_id() {
            return Err(io.invalid("directory prefix belongs to another workspace"));
        }
        Ok(DirectoryPrefixes {
            positions: positions.0,
            _custody: positions.1,
            cut,
            config: view.config(),
        })
    }

    pub(in crate::filesystem) fn seal_directory_prefix<'io>(
        &self,
        prefixes: &DirectoryPrefixes<'_>,
        index: usize,
        name: &str,
        volume: &VolumeRef,
        path: &str,
        io: &'io PreparationIo,
    ) -> Result<(String, DecodeReservation<'io>)> {
        let owner = io.account(NativeAccounting::take)?;
        let native = match PreparationIo::from_native_work(&owner.work) {
            Ok(native) => native,
            Err(error) => {
                io.account(|ctx| owner.restore(ctx))?;
                return Err(error);
            }
        };
        let result = (|| {
            let requested = crate::executor::with_history_io(Some(&native), |work| {
                crate::executor::preparation_format(format_args!("/{path}"), work)
            })?;
            let requested_hold = native.guard_owned(requested.capacity() as u64)?;
            let requested = (requested, requested_hold);
            let mut ctx = KernelAccounting::NativeOriginal {
                work: &owner.work,
                cancellation: io.cancellation(),
            };
            let cursor = match prefixes.positions.get(index) {
                Some(DirectoryPosition::Source(source)) => LazyDirectoryCursor::after_source(
                    prefixes.cut.clone(),
                    &requested.0,
                    source.clone(),
                    prefixes.config,
                    &mut ctx,
                )?,
                Some(DirectoryPosition::Authored) => {
                    let mut bytes = native.buffer(name.len())?;
                    bytes.0.copy_from_slice(name.as_bytes());
                    let name = LogicalName::new(NameEncoding::Utf8, bytes.0)
                        .map_err(|_| native.invalid("invalid directory name"))?;
                    let cursor = LazyDirectoryCursor::after_authored(
                        prefixes.cut.clone(),
                        &requested.0,
                        &name,
                        prefixes.config,
                        &mut ctx,
                    );
                    drop(name);
                    drop(bytes.1);
                    cursor?
                }
                _ => return Err(native.invalid("producer has no interior directory position")),
            };
            let encoded = cursor.encode(&owner.work);
            cursor.retire(&mut ctx)?;
            drop(ctx);
            drop(requested);
            let (bytes, allocation) = encoded?;
            native.absorb_native_custody(allocation)?;
            let sealed = cursor::seal_payload(volume, path, &bytes, &native)?;
            let length = bytes.capacity() as u64;
            crate::executor::stock_retire(bytes, length, Some(&native))?;
            let token = crate::executor::with_history_io(Some(&native), |work| {
                crate::executor::preparation_format(format_args!("lazy:{}", sealed.0), work)
            })?;
            drop(sealed);
            let bytes = token.capacity() as u64;
            let allocation = native.into_native_custody(bytes)?;
            io.accept_native_output(&owner, allocation, bytes)?;
            Ok::<_, Error>((token, bytes))
        })();
        io.account(|ctx| owner.restore(ctx))?;
        let (token, bytes) = result?;
        Ok((token, io.guard_owned(bytes)?))
    }
}
