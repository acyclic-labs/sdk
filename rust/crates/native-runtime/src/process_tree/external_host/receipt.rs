//! Versioned projection of the original host terminal into its reserved sink.

use super::*;
use serde::Serialize;

#[derive(Serialize)]
struct Receipt<'a> {
    schema: &'static str,
    invocation: &'a str,
    platform: &'static str,
    host_exit_code: i64,
    root_status: Option<RootStatus>,
    cancelled: bool,
    process_tree_retired: bool,
    lease_wait_seconds: Option<f64>,
    phases: Phases,
    failure: Option<Failure>,
}

#[derive(Serialize)]
struct RootStatus {
    native: i64,
    code: Option<i64>,
    signal: Option<i32>,
}

impl From<ExitStatus> for RootStatus {
    fn from(status: ExitStatus) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt as _;
            Self {
                native: i64::from(status.into_raw()),
                code: status.code().map(i64::from),
                signal: status.signal(),
            }
        }
        #[cfg(windows)]
        {
            use std::os::windows::process::ExitStatusExt as _;
            Self {
                native: i64::from(status.into_raw()),
                code: status.code().map(native_code),
                signal: None,
            }
        }
    }
}

fn native_code(code: i32) -> i64 {
    #[cfg(windows)]
    return i64::from(u32::from_ne_bytes(code.to_ne_bytes()));
    #[cfg(unix)]
    i64::from(code)
}

#[derive(Serialize)]
struct Phases {
    root_admitted_seconds: Option<f64>,
    root_exit_observed_seconds: Option<f64>,
    retired_seconds: Option<f64>,
    control: Option<Control>,
}

#[derive(Serialize)]
struct Control {
    seconds: f64,
    outcome: &'static str,
    failure: Option<NativeError>,
}

#[derive(Serialize)]
struct NativeError {
    #[serde(serialize_with = "error_kind")]
    kind: io::ErrorKind,
    raw_os_error: Option<i32>,
}

fn error_kind<S: serde::Serializer>(
    kind: &io::ErrorKind,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_str(&format_args!("{kind:?}"))
}

#[derive(Serialize)]
struct Failure {
    phase: &'static str,
    error: NativeError,
}

fn phase_name(phase: ProcessHostPhase) -> &'static str {
    match phase {
        ProcessHostPhase::Arguments => "arguments",
        ProcessHostPhase::ReceiptReservation => "receipt-reservation",
        ProcessHostPhase::CustodyAdmission => "custody-admission",
        ProcessHostPhase::RootAdmission => "root-admission",
        ProcessHostPhase::ControlAdmission => "control-admission",
        ProcessHostPhase::RootExit => "root-exit",
        ProcessHostPhase::Retirement => "retirement",
        ProcessHostPhase::Control => "control",
        ProcessHostPhase::ReceiptPublication => "receipt-publication",
    }
}

impl Completion {
    pub(super) fn terminal(
        &mut self,
        binding: &str,
        code: i32,
        progress: &ProcessHostPartialProgress,
        error: Option<&io::Error>,
    ) -> io::Result<()> {
        let file = self.0.as_mut().ok_or(io::ErrorKind::InvalidInput)?;
        let receipt = Receipt {
            schema: "acyclic.process-custody-completion.v3",
            invocation: binding,
            platform: std::env::consts::OS,
            host_exit_code: native_code(code),
            root_status: progress.root_status.map(RootStatus::from),
            cancelled: progress.cancelled,
            process_tree_retired: progress.retired.is_some(),
            lease_wait_seconds: progress.lease_wait.map(|elapsed| elapsed.as_secs_f64()),
            phases: Phases {
                root_admitted_seconds: progress.root_admitted.map(|elapsed| elapsed.as_secs_f64()),
                root_exit_observed_seconds: progress
                    .root_exit_observed
                    .map(|elapsed| elapsed.as_secs_f64()),
                retired_seconds: progress.retired.map(|elapsed| elapsed.as_secs_f64()),
                control: progress.control.map(|control| {
                    let (outcome, failure) = match control.outcome {
                        ProcessHostControl::Input => ("input", None),
                        ProcessHostControl::Eof => ("eof", None),
                        ProcessHostControl::Failed { kind, raw_os_error } => {
                            ("failed", Some(NativeError { kind, raw_os_error }))
                        }
                    };
                    Control {
                        seconds: control.elapsed.as_secs_f64(),
                        outcome,
                        failure,
                    }
                }),
            },
            failure: error.map(|error| Failure {
                phase: phase_name(progress.phase),
                error: NativeError {
                    kind: error.kind(),
                    raw_os_error: error.raw_os_error(),
                },
            }),
        };
        // One buffer for the fixed metadata plus the admitted identifier. This
        // is capacity, not a bound on an invocation's identity or output.
        let capacity = binding
            .len()
            .checked_add(1024)
            .ok_or(io::ErrorKind::InvalidInput)?;
        let mut encoded = Vec::with_capacity(capacity);
        serde_json::to_writer(&mut encoded, &receipt).map_err(io::Error::other)?;
        encoded.push(b'\n');
        file.write_all(&encoded)?;
        file.sync_all()
    }
}
