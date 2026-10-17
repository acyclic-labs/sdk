async fn apply_mutation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    transaction: &mut Transaction<A, O>,
    value: wire::Mutation,
) -> Result<(), Status> {
    use wire::mutation::Mutation;
    let cancellation = CancellationToken::new();
    match required(value.mutation, "mutation")? {
        Mutation::CreateFile(value) => {
            transaction
                .create_file(
                    &value.path,
                    Bytes::from(value.contents),
                    metadata(value.metadata)?,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
        }
        Mutation::CreateDirectory(value) => {
            transaction
                .create_directory(&value.path, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(|failure| status(&failure.error))?;
            transaction
                .set_metadata(
                    &value.path,
                    metadata(value.metadata)?,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
        }
        Mutation::CreateSymbolicLink(value) => {
            transaction
                .create_symbolic_link(
                    &value.path,
                    Bytes::from(value.target),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map_err(|failure| status(&failure.error))?;
            transaction
                .set_metadata(
                    &value.path,
                    metadata(value.metadata)?,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
        }
        Mutation::Remove(value) => {
            transaction
                .remove(&value.path, WorkBudget::UNBOUNDED, &cancellation)
                .await
        }
        Mutation::Rename(value) => {
            transaction
                .rename(
                    &value.source,
                    &value.destination,
                    value.replace,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
        }
        Mutation::HardLink(value) => {
            transaction
                .hard_link(
                    &value.source,
                    &value.destination,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
        }
        Mutation::Write(value) => {
            transaction
                .write_range(
                    &value.path,
                    value.offset,
                    Bytes::from(value.contents),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
        }
        Mutation::Resize(value) => {
            transaction
                .resize(
                    &value.path,
                    value.logical_bytes,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
        }
        Mutation::ZeroRange(value) => {
            let range = required(value.range, "range")?;
            transaction
                .zero_range(
                    &value.path,
                    ByteRange {
                        offset: range.offset,
                        length: range.length,
                    },
                    value.allocated,
                    value.extend,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
        }
        Mutation::Preallocate(value) => {
            let range = required(value.range, "range")?;
            transaction
                .preallocate(
                    &value.path,
                    ByteRange {
                        offset: range.offset,
                        length: range.length,
                    },
                    value.keep_size,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
        }
        Mutation::CloneRange(value) => {
            transaction
                .clone_range(
                    &value.source,
                    value.source_offset,
                    &value.destination,
                    value.destination_offset,
                    value.length,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
        }
        Mutation::SetMetadata(value) => {
            let mut updated = metadata(value.metadata)?;
            let path =
                crate::workspace::customer_path(&value.path, transaction.checkout.volume_config())
                    .map_err(|error| status(&error))?;
            let current = transaction
                .checkout
                .read_metadata(&path, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(|failure| status(&WorkspaceError::from(failure.error)))?
                .value;
            // Wire metadata has no authority to replace named attributes,
            // including the root's persisted policy.
            updated.named_attributes = current.named_attributes;
            updated.acl = current.acl;
            updated.security_descriptor = current.security_descriptor;
            transaction
                .set_metadata(&value.path, updated, WorkBudget::UNBOUNDED, &cancellation)
                .await
        }
        Mutation::CreateDirectories(value) => {
            transaction
                .create_dir_all(&value.path, WorkBudget::UNBOUNDED, &cancellation)
                .await
        }
        Mutation::PutFile(value) => {
            transaction
                .write(
                    &value.path,
                    Bytes::from(value.contents),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
        }
        Mutation::CopyFile(value) => {
            transaction
                .copy(
                    &value.source,
                    &value.destination,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
        }
    }
    .map(|_| ())
    .map_err(|failure| status(&failure.error))
}
