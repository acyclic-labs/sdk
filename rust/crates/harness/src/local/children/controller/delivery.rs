use super::*;

pub(super) async fn frontier<P: StreamProvider + ?Sized>(
    files: &TaskFiles<P>,
    reference: &Component,
    child: TaskId,
) -> Result<u64> {
    let key = FileKey::Child {
        reference: reference.clone(),
        file: ChildFile::Delivered,
    };
    let snapshot = files
        .inner
        .snapshot_files(&[key.clone()], &Query::File(key))
        .await
        .map_err(storage)?;
    match snapshot.files.first() {
        None => Ok(0),
        Some(file) => {
            let (task, delivered): (TaskId, u64) =
                serde_json::from_slice(&file.bytes).map_err(storage)?;
            if task != child {
                return Err(Error::Conflict(
                    "child delivery frontier changed identity".into(),
                ));
            }
            Ok(delivered)
        }
    }
}

pub(super) async fn acknowledge<P: StreamProvider + ?Sized>(
    files: &TaskFiles<P>,
    reference: &Component,
    child: TaskId,
    delivered: u64,
) -> Result<()> {
    let file = FileValue::new(
        FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Delivered,
        },
        serde_json::to_vec(&(child, delivered)).map_err(storage)?,
    );
    let snapshot = files
        .inner
        .snapshot_files(&[], &Query::File(file.key.clone()))
        .await
        .map_err(storage)?;
    crate::task_volume::publish(&files.inner, snapshot.generation, &file, || async {
        Ok::<_, Error>((Vec::new(), None))
    })
    .await?;
    Ok(())
}

pub(super) async fn suffix<const RETAIN: bool, P: StreamProvider + ?Sized, F, Fut>(
    files: &TaskFiles<P>,
    reference: &Component,
    child: TaskId,
    through: u64,
    mut deliver: F,
) -> Result<()>
where
    F: FnMut(FileValue) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut delivered = frontier(files, reference, child).await?;
    if delivered > through {
        return Err(Error::Conflict(
            "delivery frontier exceeds acknowledged inputs".into(),
        ));
    }
    while delivered < through {
        let file = files
            .inner
            .child_input(reference, delivered)
            .await
            .map_err(storage)?
            .ok_or_else(|| Error::Conflict("acknowledged child input is absent".into()))?;
        deliver(file).await?;
        #[cfg(all(test, not(acyclic_harness_loom)))]
        super::super::cuts::stop("input-sent", OperationId::from_bytes(child.into_bytes()))?;
        delivered += 1;
        if RETAIN {
            acknowledge(files, reference, child, delivered).await?;
        }
    }
    Ok(())
}

impl LocalHarness {
    pub(super) async fn deliver_child_inputs(
        &self,
        files: &Files,
        reference: &Component,
        child: TaskId,
        request: &FileValue,
        through: u64,
    ) -> Result<()> {
        let sender = request
            .requester
            .ok_or_else(|| Error::Unauthorized("child request has no sender".into()))?;
        let publisher = self.input_publisher_with_limit(self.limits.file_bytes)?;
        suffix::<true, _, _, _>(files, reference, child, through, |file| {
            let publisher = &publisher;
            async move {
                let operation =
                    admission::child_operation(files.volume(), &file.key.path(), b"mail")?;
                let prompt;
                let body = if matches!(
                    file.key,
                    FileKey::Child {
                        file: ChildFile::Request,
                        ..
                    }
                ) {
                    prompt = ChildRequest::parse(&file.bytes, self.limits.file_bytes)?.prompt;
                    prompt.as_bytes()
                } else {
                    file.bytes.as_ref()
                };
                let (payload, _) = publisher
                    .stage_once(
                        operation,
                        &format!("inputs/agents/{child}/{operation}.md"),
                        body,
                        "text/plain",
                        "message.md",
                        None,
                        None,
                    )?
                    .await?;
                self.runtime
                    .task_host()
                    .send(sender, child, operation, payload)
                    .await?;
                Ok(())
            }
        })
        .await
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
