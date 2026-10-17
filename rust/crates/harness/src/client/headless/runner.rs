//! One headless filesystem contract for every client backend.
use super::HeadlessOutcome;
use crate::{OperationId, Result, task_volume::FileValue};
use futures::Stream;
use std::{future::Future, io::Write};

/// A backend consuming request files and emitting snapshot/cursor-follow JSONL.
pub trait HeadlessClient {
    /// Runs one prompt while publishing its followed records.
    fn prompt<W: Write>(
        &mut self,
        command: FileValue,
        target: OperationId,
        output: &mut W,
    ) -> impl Future<Output = Result<HeadlessOutcome>>;
    /// Runs bounded JSONL input alongside the same followed records.
    fn input<W: Write>(
        &mut self,
        target: OperationId,
        input: impl Stream<Item = Result<String>> + Unpin,
        output: &mut W,
    ) -> impl Future<Output = Result<HeadlessOutcome>>;
}

/// Submits through the client and follows its exact operation to settlement.
pub async fn run_headless<C: HeadlessClient>(
    client: &mut C,
    command: FileValue,
    target: OperationId,
    output: &mut impl Write,
) -> Result<HeadlessOutcome> {
    client.prompt(command, target, output).await
}

/// Reads this client's canonical JSONL input and drains followed output before exit.
pub async fn run_jsonl<C: HeadlessClient>(
    client: &mut C,
    target: OperationId,
    input: impl Stream<Item = Result<String>> + Unpin,
    output: &mut impl Write,
) -> Result<HeadlessOutcome> {
    client.input(target, input, output).await
}
