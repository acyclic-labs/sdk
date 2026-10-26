use super::*;
use crate::task_volume::ChildFile;
use serde::Deserialize;

/// Envelope carried as the text of an ordinary parent `send` message.
/// Its sender comes from the immutable inbox file's authenticated requester.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ParentDecision {
    approval_decision: FileValue,
}

async fn publish_attention(
    files: &crate::task_volume::TaskFiles<LocalStream>,
    value: FileValue,
) -> Result<()> {
    let snapshot = files.snapshot(&Query::File(value.key.clone()), 1).await?;
    if value.bytes.as_ref() == b"null" && snapshot.files.is_empty() {
        return Ok(());
    }
    crate::task_volume::publish(&files.inner, snapshot.generation, &value, || {
        std::future::ready(Ok::<_, Error>((Vec::new(), None)))
    })
    .await?;
    crate::task_volume::publish_durable(&files.inner, &value, None).await?;
    Ok(())
}

pub(super) async fn clear_attention(inbox: &Inbox) -> Result<()> {
    publish_attention(
        &inbox.files,
        FileValue::new(
            FileKey::Child {
                reference: inbox.reference.clone(),
                file: ChildFile::ApprovalAttention,
            },
            b"null".to_vec(),
        ),
    )
    .await
}

pub(super) async fn attention(
    stream: &Arc<LocalStream>,
    task: TaskId,
    request: &FileValue,
    inbox: &Inbox,
    principal: &str,
) -> Result<()> {
    let files = &inbox.files;
    if !files.inner.shares_provider(stream) {
        return Err(Error::Unauthorized(
            "parent attention uses another provider".into(),
        ));
    }
    let reference = &inbox.reference;
    let mirror = FileValue::new(
        FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::ApprovalRequest(request_id(request)?.clone()),
        },
        request.bytes.clone(),
    );
    // The attention publication's barrier covers this preceding request on
    // the same provider. Both exact durable proofs are checked before return.
    acyclic_stream::deferring_provider_durability(&**stream, async {
        crate::task_volume::publish(&files.inner, files.inner.head().await?, &mirror, || {
            std::future::ready(Ok::<_, Error>((Vec::new(), None)))
        })
        .await
    })
    .await?;
    let attention = serde_json::json!({"task":task,"request":request_id(request)?,
        "request_sha256":fingerprint(request)?,"expected_version":1,"principal":principal,
        "subject":serde_json::from_slice::<serde_json::Value>(&request.bytes).map_err(storage)?});
    publish_attention(
        files,
        FileValue::new(
            FileKey::Child {
                reference: reference.clone(),
                file: ChildFile::ApprovalAttention,
            },
            serde_json::to_vec(&attention).map_err(storage)?,
        ),
    )
    .await?;
    crate::task_volume::publish_durable(&files.inner, &mirror, None).await?;
    Ok(())
}

impl LocalHarness {
    pub(super) async fn service_parent_approval(
        &self,
        task: TaskId,
        parent: TaskId,
        request: &FileValue,
        inbox: Option<&Inbox>,
    ) -> Result<bool> {
        let inbox = inbox.ok_or_else(|| {
            Error::Unauthorized("parent approval requires the retained child inbox".into())
        })?;
        let Inbox {
            files, reference, ..
        } = inbox;
        let attention_key = FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::ApprovalAttention,
        };
        attention(
            &self.runtime.task_host().workflow_stream(),
            task,
            request,
            inbox,
            &format!("parent:{parent}"),
        )
        .await?;
        // Observing completion before taking the inbox snapshot fences the
        // denial behind every message the parent committed before completing.
        // If completion races this read, the next service pass takes a new
        // snapshot instead of denying from the older cut.
        let (parent_done, snapshot) =
            completed_snapshot(&inbox.parent_done, files.inner.child_input_count(reference)).await;
        let count = snapshot?;
        for delivered in 0..count {
            let Some(message) = files.inner.child_input(reference, delivered).await? else {
                continue;
            };
            if !matches!(
                message.key,
                FileKey::Child {
                    file: ChildFile::Inbox(_),
                    ..
                }
            ) {
                continue;
            }
            let Ok(envelope) = serde_json::from_slice::<ParentDecision>(&message.bytes) else {
                continue;
            };
            if message.requester != Some(parent) {
                return Err(Error::Unauthorized(
                    "approval inbox sender is not the retained parent".into(),
                ));
            }
            let FileKey::Task {
                task: target,
                file:
                    TaskFile::ApprovalDecision {
                        request: target_request,
                        ..
                    },
            } = &envelope.approval_decision.key
            else {
                return Err(Error::Unauthorized(
                    "parent submitted another file kind".into(),
                ));
            };
            if target.as_str() != task.to_string() || target_request != request_id(request)? {
                continue;
            }
            self.admit_approval_file(
                task,
                &envelope.approval_decision,
                &format!("parent:{parent}"),
            )
            .await?;
            publish_attention(files, FileValue::new(attention_key, b"null".to_vec())).await?;
            return Ok(true);
        }
        // A completed parent can already have sent its authenticated decision.
        // Consume that retained inbox first; completion only refuses requests
        // for which no such decision exists.
        if parent_done {
            self.settle_approval(
                task,
                request,
                &format!("parent:{parent}"),
                false,
                Some("parent completed without deciding this request".into()),
            )
            .await?;
            publish_attention(files, FileValue::new(attention_key, b"null".to_vec())).await?;
            return Ok(true);
        }
        Ok(false)
    }
}

// The completion observation must precede the provider's snapshot cut.
async fn completed_snapshot<T>(
    done: &acyclic_fs::CancellationToken,
    snapshot: impl std::future::Future<Output = T>,
) -> (bool, T) {
    let completed = done.is_cancelled();
    (completed, snapshot.await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_snapshot_cannot_deny_a_committed_parent_decision() {
        futures::executor::block_on(async {
            for stale_completion in [false, true] {
                let done = acyclic_fs::CancellationToken::new();
                let inbox = std::cell::Cell::new(None);
                let read = async {
                    let snapshot = inbox.get();
                    // The parent commits its decision after this snapshot cut,
                    // then completes before the reader observes the result.
                    inbox.set(Some(true));
                    done.cancel();
                    snapshot
                };
                let (completed, decision) = if stale_completion {
                    // Negative control restores the old read-before-fence order.
                    let decision = read.await;
                    (done.is_cancelled(), decision)
                } else {
                    completed_snapshot(&done, read).await
                };
                assert_eq!(completed && decision.is_none(), stale_completion);
                // A service pass that saw a racing completion must requery.
                let (completed, decision) = completed_snapshot(&done, async { inbox.get() }).await;
                assert!(completed);
                assert_eq!(decision, Some(true));
            }
        });
    }
}
