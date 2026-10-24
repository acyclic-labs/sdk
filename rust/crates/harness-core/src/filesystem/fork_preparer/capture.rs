//! Durable provider capture and lost-reply reconciliation.
use super::*;

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> PreparationAttempt<'_, A, O> {
    pub(super) async fn capture(
        &self,
        journal: &WorkspaceRef,
        request: &ForkRequest,
        index: usize,
        selection: &ForkSelection,
        provider: &dyn ForkCaptureProvider,
    ) -> Result<Capture> {
        let host = &self.preparer.host;
        let io = self.io;
        let maximum = self.journal_limit(MAX_CAPTURE_BYTES);
        let (start, result) = crate::executor::with_history_io(io, |work| {
            Ok::<_, Error>((
                crate::executor::preparation_format(
                    format_args!("/capture-{index}-attempt.json"),
                    work,
                )?,
                crate::executor::preparation_format(
                    format_args!("/capture-{index}-result.json"),
                    work,
                )?,
            ))
        })?;
        let capture = read_record::<A, O, Capture>(host, journal, &result, maximum, io).await?;
        let prior = read_record::<A, O, Uuid>(host, journal, &start, maximum, io).await?;
        if let Some(capture) = capture {
            if prior.is_none() {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "fork capture result has no durable start",
                ));
            }
            validate_capture(selection, &capture)?;
            return Ok(capture);
        }
        let won = if prior.is_some() {
            false
        } else {
            let attempt = crate::identity::fresh_uuid();
            let key = IdempotencyKey::new(crate::executor::with_history_io(io, |work| {
                crate::executor::preparation_format(
                    format_args!(
                        "fork:{}:capture-attempt:{index}:{attempt}",
                        request.operation_id
                    ),
                    work,
                )
            })?)?;
            write_record(
                host,
                journal,
                &start,
                &attempt,
                maximum,
                &key,
                io,
                self.task_preparation,
            )
            .await?;
            read_record::<A, O, Uuid>(host, journal, &start, maximum, io)
                .await?
                .ok_or(Error::Indeterminate(request.operation_id))?
                == attempt
        };
        let captured = if won {
            self.verify_task_lease(false).await?;
            provider.capture(request, selection).await?
        } else {
            provider
                .reconcile(request, selection)
                .await?
                .ok_or(Error::Indeterminate(request.operation_id))?
        };
        validate_capture(selection, &captured)?;
        let key = IdempotencyKey::new(crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(
                format_args!("fork:{}:capture-result:{index}", request.operation_id),
                work,
            )
        })?)?;
        let committed = write_once(
            host,
            journal,
            &result,
            &captured,
            maximum,
            &key,
            request.operation_id,
            io,
            self.task_preparation,
        )
        .await?;
        read_record::<A, O, Uuid>(host, journal, &start, maximum, io)
            .await?
            .ok_or_else(|| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "fork capture result has no durable start",
                )
            })?;
        validate_capture(selection, &committed)?;
        if committed != captured {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "fork capture has another committed result",
            ));
        }
        Ok(committed)
    }
}

use crate::executor::stock_boundary;
