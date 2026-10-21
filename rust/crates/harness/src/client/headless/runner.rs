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
#[inline(never)]
pub fn run_headless<'a, C: HeadlessClient + 'a>(
    client: &'a mut C,
    command: FileValue,
    target: OperationId,
    output: &'a mut (impl Write + 'a),
) -> impl Future<Output = Result<HeadlessOutcome>> + 'a {
    // Construct the child on the heap outside the caller's poll frame. Polling
    // and synchronous drop remain owned by that caller, including in replay.
    Box::pin(client.prompt(command, target, output))
}

/// Reads this client's canonical JSONL input and drains followed output before exit.
#[inline(never)]
pub fn run_jsonl<'a, C: HeadlessClient + 'a>(
    client: &'a mut C,
    target: OperationId,
    input: impl Stream<Item = Result<String>> + Unpin + 'a,
    output: &'a mut (impl Write + 'a),
) -> impl Future<Output = Result<HeadlessOutcome>> + 'a {
    Box::pin(client.input(target, input, output))
}

#[cfg(test)]
mod tests;
