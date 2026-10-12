use super::commit::observe_commit;
use super::*;

/// Accepts and funds a completion before its physical action.
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub(crate) async fn prepare_materialization_commit<P: StreamProvider>(
    owner: &crate::durable_host::TaskJournalOwner<P>,
    mut template: crate::filesystem::NativePreparationCompletionTemplate,
    original: &acyclic_fs::NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'static, acyclic_fs::MaterializationJournal>> {
    use acyclic_stream::{CommitCondition, CommitOutcome};
    use std::mem::size_of;
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(original)?,
    );
    let mut work = Some(&mut io as &mut dyn HistoryIo);
    if !template.matches_work(original) {
        return Err(Error::NativeBoundary {
            code: wire::ErrorCode::Unauthorized,
            reason: "native completion differs from its original observer and custody",
        });
    }
    let (_, output, maximum_bytes) = template.codec_parts();
    if maximum_bytes == 0
        || maximum_bytes > acyclic_stream::MAX_RECORD_BYTES as u64
        || (output.len() as u64) < maximum_bytes
    {
        return Err(Error::NativeBoundary {
            code: wire::ErrorCode::Storage,
            reason: "native completion lacks its fixed prepaid output",
        });
    }
    let reservation = original.reserve_work(acyclic_fs::WorkCounters {
        backend_read_operations: 1,
        backend_write_operations: 1,
        authority_records_read: 1,
        authority_records_appended: 1,
        authority_bytes_read: maximum_bytes,
        authority_bytes_written: maximum_bytes,
        bytes_encoded: maximum_bytes,
        bytes_copied: maximum_bytes,
        items_examined: maximum_bytes + 24,
        ..acyclic_fs::WorkCounters::default()
    })?;
    template.verify_owner(owner, original)?;
    let client = owner.stream();
    let owner_condition = owner.condition(JournalWrite::Fresh, &mut work).await?;
    let bounds = operations::bounds_with_work(&client, template.path(), &mut work)
        .await?
        .map_err(|error| crate::store::operations::stream_error_with_work(error, &work))?;
    if bounds.tail != template.expected_revision() {
        return Err(guarded_boundary(
            &work,
            wire::ErrorCode::Conflict,
            "native completion Preparing record changed before acceptance",
        ));
    }
    let target = CommitCondition::Tail {
        path: template.path().clone(),
        expected: template.expected_revision(),
    };
    if !matches!(&owner_condition, CommitCondition::Tail { .. }) {
        return Err(Error::NativeBoundary {
            code: wire::ErrorCode::Unauthorized,
            reason: "native completion requires its original owner tail",
        });
    }
    let pool_bytes = (size_of::<Error>() as u64 + crate::executor::json::sdk_json_diagnostic_bytes())
        .max(size_of::<acyclic_fs::MaterializationJournal>() as u64);
    let (request, custody) = {
        let mut prepared = commit_request(template.key().clone(), 0, &mut work);
        prepared.1.reserve(2, &mut prepared.0.conditions)?;
        prepared.0.conditions.push(owner_condition);
        prepared.0.conditions.push(target);
        add_record(
            &mut prepared,
            template.path().clone(),
            acyclic_stream::OwnedBytes::new(),
        )?;
        if let Some(io) = prepared.1.work().as_deref_mut() {
            io.admit_owned(pool_bytes)?;
            io.retain_owned(pool_bytes)?;
        }
        prepared.1.adopt(pool_bytes)?;
        let retained = prepared.1.retained();
        let custody = prepared
            .1
            .work()
            .as_deref_mut()
            .expect("checked native observer")
            .take_native_custody(retained)?;
        prepared.1.transfer();
        let (request, quota) = prepared;
        drop(quota);
        (request, custody)
    };
    let observer =
        match crate::store::history::AcceptedCompletionIo::new(original, reservation, custody) {
            Ok(observer) => observer,
            Err(error) => {
                drop((request, template));
                return Err(error);
            }
        };
    let prepared = async {
        let (mut prepared, sdk_custody) = native_response(
            original,
            client
                .native_prepare_commit(request, maximum_bytes as usize, original.runtime_meter())
                .await,
        )?;
        let Some(recovery) = prepared.take_recovery_future() else {
            drop(prepared);
            return Err(Error::NativeBoundary {
                code: wire::ErrorCode::Invalid,
                reason: "native completion lacks its pre-action same-key recovery",
            });
        };
        Ok((prepared, recovery, sdk_custody))
    }
    .await;
    let (sdk, sdk_recovery, sdk_custody) = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            drop(template);
            return Err(completion_error(error, observer.into_custody()));
        }
    };
    drop(work);
    drop(io);
    // Unpolled payloads drop before their funding.
    let pending = (template, sdk, sdk_recovery, sdk_custody, observer);
    crate::NativeFutureBundle::new(original, || async move {
        let state = pending;
        let counters = state.4.completion_work()?;
        let (mut template, sdk, sdk_recovery, mut sdk_custody, mut observer) = state;
        let operation = OperationId::from_bytes(template.task_binding().0.into_bytes());
        let finished = async {
            template.set_completed_work(counters)?;
            let (value, output, maximum_bytes) = template.codec_parts();
            // The accepted reservation covers every write within this fixed buffer.
            let mut output = std::io::Cursor::new(&mut output[..maximum_bytes as usize]);
            let serialized = serde_json::to_writer(&mut output, value);
            let written = output.position() as usize;
            let charged = observer.record_completed(acyclic_fs::WorkCounters {
                bytes_encoded: written as u64,
                bytes_copied: written as u64,
                ..acyclic_fs::WorkCounters::default()
            });
            serialized.map_err(|source| Error::NativeJson {
                code: wire::ErrorCode::Storage,
                source,
                custody: None,
            })?;
            charged?;
            let (journal, output, mut custody) = template.into_completed_parts();
            if let Err(cause) = sdk_custody.absorb(&mut custody) {
                drop(output);
                return Err(Error::NativeWork {
                    cause,
                    custody: Some(custody),
                });
            }
            let output = output.freeze(written).map_err(|output| {
                drop(output);
                Error::NativeBoundary {
                    code: wire::ErrorCode::Storage,
                    reason: "native completion exceeds its original initialized output",
                }
            })?;
            observer.admit_acknowledgment(output.len() as u64, 1)?;
            let original = observer
                .original_native_work()
                .expect("accepted completion ledger")
                .clone();
            let mut optional = Some(&mut observer as &mut dyn HistoryIo);
            let mut quota = CodecQuota::from_retained(&mut optional, 0);
            let committed = match native_response(&original, sdk.finish(output).await) {
                Ok(received) => {
                    observe_commit(&received.0, true, None, &mut quota)?;
                    matches!(&received.0, CommitOutcome::Committed(_))
                }
                Err(Error::NativeStream {
                    source: StreamError::Unavailable,
                    ..
                }) => {
                    let inspected = native_response(&original, sdk_recovery.await).map_err(
                        |error| match error {
                            Error::NativeStream { .. } => Error::Indeterminate(operation),
                            error => error,
                        },
                    )?;
                    match inspected.0.as_ref().map(|observation| &observation.outcome) {
                        Some(IdempotencyOutcome::Commit(outcome)) => {
                            observe_commit(outcome, false, None, &mut quota)?;
                            matches!(outcome, CommitOutcome::Committed(_))
                        }
                        None => return Err(Error::Indeterminate(operation)),
                        Some(_) => {
                            return Err(guarded_boundary(
                                quota.work(),
                                wire::ErrorCode::Conflict,
                                "commit recovery identity reused",
                            ));
                        }
                    }
                }
                Err(error) => return Err(error),
            };
            quota
                .work()
                .as_deref_mut()
                .expect("accepted completion observer")
                .check_cancellation()?;
            if !committed {
                return Err(Error::NativeBoundary {
                    code: wire::ErrorCode::Conflict,
                    reason: "native materialization completion changed before its fenced commit",
                });
            }
            Ok(journal)
        }
        .await;
        drop(sdk_custody);
        let mut custody = observer.into_custody();
        let mut journal = match finished {
            Ok(journal) => journal,
            Err(error) => return Err(completion_error(error, custody)),
        };
        let output_bytes = size_of::<acyclic_fs::MaterializationJournal>() as u64;
        if let Err(cause) = custody.substitute(output_bytes) {
            drop(journal);
            return Err(Error::NativeWork {
                cause,
                custody: Some(custody),
            });
        }
        match journal.custody.as_mut() {
            Some(guard) => {
                if let Err(cause) = guard.absorb(&mut custody) {
                    drop(journal);
                    return Err(Error::NativeWork {
                        cause,
                        custody: Some(custody),
                    });
                }
            }
            None => journal.custody = Some(custody),
        }
        Ok(journal)
    })
}

#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
fn completion_error(mut error: Error, incoming: acyclic_fs::NativeAllocation) -> Error {
    let custody = match &mut error {
        Error::NativeWork { custody, .. }
        | Error::NativeJson { custody, .. }
        | Error::NativeStream { custody, .. } => custody,
        _ => return error,
    };
    if custody.is_none() {
        *custody = Some(incoming);
    }
    error
}

fn native_response<T>(
    original: &acyclic_fs::NativeMaterializationWork,
    response: acyclic_stream::NativeStreamResult<T>,
) -> Result<(T, acyclic_fs::NativeAllocation)> {
    let output = crate::store::history::native_sdk_result(original, response)?;
    let (value, allocation) = output.into_parts();
    match acyclic_fs::NativeAllocation::from_runtime_allocation(original, allocation) {
        Ok(allocation) => Ok((value, allocation)),
        Err((cause, allocation)) => {
            drop(value);
            drop(allocation);
            Err(Error::NativeWork {
                cause,
                custody: None,
            })
        }
    }
}
