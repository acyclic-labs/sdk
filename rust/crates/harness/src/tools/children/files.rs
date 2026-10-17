use super::*;
use crate::runtime::OwnedTaskMailbox;

pub(super) async fn write<P: StreamProvider + ?Sized>(
    files: &TaskFiles<P>,
    reference: &Component,
    mut file: FileValue,
    limits: Limits,
    owner: Option<&dyn OwnedTaskMailbox>,
    reconcile: bool,
) -> Result<bool> {
    if file.bytes.len() as u64 > limits.file_bytes {
        return Err(invalid("child file exceeds the calling scope"));
    }
    if matches!(
        file.key,
        FileKey::Child {
            file: ChildFile::Request,
            ..
        }
    ) {
        file.requester = owner.map(OwnedTaskMailbox::task_id);
    }
    let snapshot = files
        .inner
        .snapshot_files(
            &[
                file.key.clone(),
                FileKey::Child {
                    reference: reference.clone(),
                    file: ChildFile::Request,
                },
            ],
            &Query::Child(reference.clone()),
        )
        .await
        .map_err(storage)?;
    crate::task_volume::authorize_write(&file.key, file.bytes.len() as u64, files.maximum_bytes())?;
    if reconcile {
        return Ok(snapshot.files.contains(&file));
    }
    if !matches!(
        file.key,
        FileKey::Child {
            file: ChildFile::Request,
            ..
        }
    ) && !snapshot.files.iter().any(|file| {
        matches!(
            file.key,
            FileKey::Child {
                file: ChildFile::Request,
                ..
            }
        )
    }) {
        return Err(Error::NotFound(format!("agents/{}", reference.as_str())));
    }
    // Reuse the publication reducer and owner-revalidation path already modeled
    // for independent task-file writers; this adapter owns no retry protocol.
    crate::task_volume::publish(&files.inner, snapshot.generation, &file, || async {
        match owner {
            Some(owner) => {
                let (condition, deadline) = owner.file_publication_condition().await?;
                Ok::<_, Error>((vec![condition], deadline))
            }
            None => Ok((Vec::new(), None)),
        }
    })
    .await?;
    Ok(true)
}

pub(super) async fn follow<P: StreamProvider + ?Sized>(
    files: &TaskFiles<P>,
    reference: &Component,
    limits: Limits,
    wait: bool,
    settled: bool,
) -> Result<Option<Value>> {
    let maximum = usize::try_from(limits.render_bytes).map_err(invalid)?;
    let snapshot = files
        .inner
        .child_lifecycle(Some(reference))
        .await
        .map_err(storage)?;
    if files
        .inner
        .child_input_count(reference)
        .await
        .map_err(storage)?
        == 0
    {
        return Err(Error::NotFound(format!("agents/{}", reference.as_str())));
    }
    let mut changes = files
        .inner
        .follow(Query::Child(reference.clone()), snapshot.cursor)
        .await
        .map_err(storage)?;
    let mut values = snapshot.files;
    loop {
        let inputs = files
            .inner
            .child_input_count(reference)
            .await
            .map_err(storage)?;
        let result = super::super::agent_files::status_at(reference, &values, maximum, inputs)?;
        let ready = if settled {
            result["settled"] == true
        } else {
            !result["execution"].is_null()
        };
        if ready {
            return Ok(Some(result));
        }
        if !wait {
            return Ok(None);
        }
        let change = changes
            .next()
            .await
            .ok_or_else(|| Error::Conflict("child follow ended".into()))?
            .map_err(storage)?;
        fold(&mut values, change);
    }
}

fn fold(values: &mut Vec<FileValue>, change: crate::task_volume::Change) {
    values.retain(|file| !change.removed.contains(&file.key));
    for file in change.file.into_iter().chain(change.files) {
        if matches!(
            file.key,
            FileKey::Child {
                file: ChildFile::Request | ChildFile::Inbox(_),
                ..
            }
        ) {
            continue;
        }
        if let FileKey::Child {
            reference,
            file: ChildFile::Result(turn),
        } = &file.key
        {
            values.retain(|old| !matches!(&old.key, FileKey::Child { reference: r, file: ChildFile::Result(n) } if r == reference && n < turn));
        }
        values.retain(|previous| previous.key != file.key);
        values.push(file);
    }
}

pub(super) fn bounded(mut value: Value, limits: Limits) -> Result<Value> {
    if serde_json::to_vec(&value).map_err(invalid)?.len() as u64 > limits.render_bytes {
        if let Some(agents) = value.get_mut("agents").and_then(Value::as_array_mut) {
            for agent in agents {
                if let Some(result) = agent.get_mut("result").and_then(Value::as_object_mut) {
                    result.remove("outcome");
                }
            }
        }
        if serde_json::to_vec(&value).map_err(invalid)?.len() as u64 > limits.render_bytes {
            return Err(invalid(
                "child result references exceed the original render bound",
            ));
        }
    }
    Ok(value)
}
