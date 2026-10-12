use super::commit::{
    PreparedNativeRecovery, receive_native_commit, reserve_prepared_vector, resolve_commit,
};
use super::*;

/// Accepts and funds a completion before its physical action.
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub(crate) async fn prepare_materialization_commit<P: StreamProvider>(
    owner: &crate::durable_host::TaskJournalOwner<P>,
    mut template: crate::filesystem::NativePreparationCompletionTemplate,
    original: &acyclic_fs::NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'static, acyclic_fs::MaterializationJournal>> {
    use acyclic_stream::{CommitCondition, CommitConflict, CommittedMutation, Record};
    use std::mem::size_of;
    let mut io = crate::store::history::native_work_io(original)?;
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
        .map_err(|error| sdk_error(error, &work))?;
    if bounds.tail != template.expected_revision() {
        return Err(coordinator_boundary(
            &work,
            wire::ErrorCode::Conflict,
            "native completion Preparing record changed before acceptance",
        ));
    }
    let target = CommitCondition::Tail {
        path: template.path().clone(),
        expected: template.expected_revision(),
    };
    let owner_path_bytes = match &owner_condition {
        CommitCondition::Tail { path, .. } => path.as_str().len() as u64,
        CommitCondition::Absent { .. } => {
            return Err(Error::NativeBoundary {
                code: wire::ErrorCode::Unauthorized,
                reason: "native completion requires its original owner tail",
            });
        }
    };
    let path_bytes = template.path().as_str().len() as u64;
    let key_bytes = template.key().as_bytes().len() as u64;
    // SDK paths, retry keys and the completion record are bounded before this arithmetic.
    let append_metadata = (4 * size_of::<CommittedMutation>() + 4 * size_of::<Record>()) as u64;
    let conflict_metadata = (4 * size_of::<CommitConflict>()) as u64;
    let append_source = append_metadata + path_bytes + maximum_bytes;
    let conflict_source = conflict_metadata + owner_path_bytes + path_bytes;
    let pool_bytes = (append_source.max(conflict_source)
        + key_bytes
        + size_of::<acyclic_stream::IdempotencyObservation>() as u64
        + size_of::<Error>() as u64
        + crate::contract::sdk_json_diagnostic_bytes()
        + size_of::<(
            crate::filesystem::NativePreparationCompletionTemplate,
            acyclic_stream::PreparedNativeCommit,
            PreparedNativeRecovery,
            acyclic_fs::NativeAllocation,
            crate::store::history::AcceptedCompletionIo,
        )>() as u64)
        .max(size_of::<acyclic_fs::MaterializationJournal>() as u64);
    let (request, custody) = {
        let mut prepared = commit_request(template.key().clone(), 0, &mut work);
        reserve_prepared_vector(&mut prepared.0.conditions, 2, &mut prepared.1)?;
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
        let prepared = native_call_result(
            original,
            client
                .native_prepare_commit(request, maximum_bytes as usize, original.runtime_meter())
                .await,
        )?
        .map_err(|error| sdk_error(error, &None))?;
        let (mut prepared, incoming) = prepared.into_parts();
        let sdk_custody =
            match acyclic_fs::NativeAllocation::from_runtime_allocation(original, incoming) {
                Ok(incoming) => incoming,
                Err((cause, incoming)) => {
                    drop(prepared);
                    drop(incoming);
                    return Err(Error::NativeWork {
                        cause,
                        custody: None,
                    });
                }
            };
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
            let key = template.key().clone();
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
            let committed = sdk.finish(output).await;
            let (outcome, received) = {
                let mut optional = Some(&mut observer as &mut dyn HistoryIo);
                let mut custody = CodecQuota::from_retained(&mut optional, 0);
                let committed = receive_native_commit(&original, committed, None, 0, &mut custody)?;
                resolve_commit(
                    committed,
                    &client,
                    key,
                    Some(operation),
                    None,
                    Some(sdk_recovery),
                    custody.work(),
                )
                .await?
            };
            let committed = matches!(&outcome, acyclic_stream::CommitOutcome::Committed(_));
            drop(outcome);
            observer.release_owned(received)?;
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
