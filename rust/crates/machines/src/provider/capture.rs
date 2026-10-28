//! Shared capture for consumers that need a bounded command result.
use super::*;

/// Exact command bytes and the producer's terminal outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturedExec {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: Option<i32>,
    pub stop: Option<ExecStop>,
}

/// Pulls the original execution stream without dispatching, polling or spawning
/// a reader. Dropping this future drops the original stream and its RPC.
pub async fn capture_exec(
    mut output: ExecStream,
    maximum_bytes: u64,
) -> Result<CapturedExec, ProviderError> {
    let mut captured = CapturedExec {
        stdout: Vec::new(),
        stderr: Vec::new(),
        exit_code: None,
        stop: None,
    };
    let mut received = 0_u64;
    while let Some(frame) = output.next().await {
        match frame? {
            ExecOutput::Data(bytes) | ExecOutput::Stderr(bytes) if bytes.is_empty() => {
                return Err(ProviderError::Rejected(
                    "exec emitted empty data; recovery: omit empty producer frames".into(),
                ));
            }
            ExecOutput::Data(bytes) => {
                append(&mut captured.stdout, &bytes, &mut received, maximum_bytes)?
            }
            ExecOutput::Stderr(bytes) => {
                append(&mut captured.stderr, &bytes, &mut received, maximum_bytes)?
            }
            ExecOutput::ExitCode(code) => {
                captured.exit_code = Some(code);
                return Ok(captured);
            }
            ExecOutput::Stopped(stop) => {
                captured.stop = Some(stop);
                return Ok(captured);
            }
        }
    }
    Err(ProviderError::Rejected("exec ended without a terminal result; recovery: reconcile the original command instead of redispatching".into()))
}

fn append(
    destination: &mut Vec<u8>,
    bytes: &[u8],
    received: &mut u64,
    maximum: u64,
) -> Result<(), ProviderError> {
    *received = received.checked_add(bytes.len() as u64)
        .filter(|value| *value <= maximum)
        .ok_or_else(|| ProviderError::Rejected("exec producer exceeded its output allowance; recovery: stop the original command and repair producer accounting".into()))?;
    destination.extend_from_slice(bytes);
    Ok(())
}
