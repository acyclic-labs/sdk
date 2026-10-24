use super::*;

// The tool facade and follow worker consume the same request/claim/ready/result
// files. Neither path can bypass the retained response or the owner's fence.
pub(crate) async fn dispatch<P: StreamProvider>(
    volume: &TaskVolume<dyn StreamProvider>,
    owner: &TaskJournalOwner<P>,
    operation: crate::OperationId,
    input: Option<&SearchRequest>,
    mut resolve: impl FnMut() -> Result<Option<Arc<dyn SearchProvider>>>,
    work: NativeWork<'_>,
) -> Result<Option<OwnedBytes>> {
    if !volume.shares_provider(&owner.stream()) {
        return Err(crate::rejection(
            work,
            "search volume uses another task provider",
        ));
    }
    // Two Arc<str> components; format both identities on the stack first.
    let _names = work
        .map(|work| {
            work.claim(
                71 + 4 * std::mem::size_of::<usize>() as u64,
                2,
                acyclic_fs::WorkCounters {
                    bytes_copied: 71,
                    ..Default::default()
                },
            )
        })
        .transpose()?;
    let mut task = [0; 32];
    hex::encode_to_slice(owner.task_id().into_bytes(), &mut task)
        .map_err(|_| super::super::rejection(work, "invalid search task identity"))?;
    let mut id = [0; 39];
    id[..7].copy_from_slice(b"search-");
    hex::encode_to_slice(operation.into_bytes(), &mut id[7..])
        .map_err(|_| super::super::rejection(work, "invalid search request identity"))?;
    let task = Component::new(
        std::str::from_utf8(&task)
            .map_err(|_| super::super::rejection(work, "invalid search task identity"))?,
    )
    .map_err(storage)?;
    let id = Component::new(
        std::str::from_utf8(&id)
            .map_err(|_| super::super::rejection(work, "invalid search request identity"))?,
    )
    .map_err(storage)?;
    let key = FileKey::Task {
        task: task.clone(),
        file: TaskFile::Request(id.clone()),
    };
    let bytes = input.map(|input| encode(input, work)).transpose()?;
    loop {
        attempt(work)?;
        let saved = volume
            .snapshot(&Query::File(key.clone()), 1)
            .await
            .map_err(storage)?;
        if let Some(file) = saved.files.first() {
            if bytes.as_ref().is_some_and(|bytes| *bytes != file.bytes) {
                return Err(crate::rejection(work, "search request identity changed"));
            }
            break;
        }
        let Some(bytes) = &bytes else {
            return Ok(None);
        };
        match volume
            .create_if(
                saved.generation,
                &FileValue::new(key.clone(), bytes.clone()),
                condition(owner, work, false).await?,
                None,
                work,
            )
            .await
        {
            Ok(_) => break,
            Err(crate::task_volume::Error::Conflict(_)) => continue,
            Err(error) => return Err(storage(error)),
        }
    }
    let authorize = |settlement| condition(owner, work, settlement);
    settle(
        volume,
        &task,
        &id,
        owner.task_binding().1,
        &mut resolve,
        &authorize,
        work,
    )
    .await?;
    let saved = volume
        .snapshot(&Query::File(file(&task, &id, true)), 1)
        .await
        .map_err(storage)?;
    Ok(saved.files.into_iter().next().map(|file| file.bytes))
}
