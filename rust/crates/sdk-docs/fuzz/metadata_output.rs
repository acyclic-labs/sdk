use super::child;
use cargo_metadata::{Error, Metadata};
use std::process::{ExitStatus, Output};

/// Require the production output decoder to preserve typed fields and failures.
///
/// # Panics
///
/// Panics when the decoded result disagrees with the original output or Cargo's
/// maintained metadata schema.
pub fn check(bytes: &[u8]) {
    #[cfg(unix)]
    use std::os::unix::process::ExitStatusExt;
    #[cfg(windows)]
    use std::os::windows::process::ExitStatusExt;

    for success in [true, false] {
        #[cfg(unix)]
        let failure = 101 << 8;
        #[cfg(windows)]
        let failure = 101;
        let output = Output {
            status: ExitStatus::from_raw(if success { 0 } else { failure }),
            stdout: if success { bytes.to_vec() } else { Vec::new() },
            stderr: if success { Vec::new() } else { bytes.to_vec() },
        };
        let result = child::decode_metadata_output(output);
        // Compare the complete original wire to Cargo's maintained typed decoder.
        // Never repair/re-encode fuzz input before the production admission.
        let text = std::str::from_utf8(bytes);
        let json = text
            .as_ref()
            .ok()
            .and_then(|text| text.lines().find(|line| line.starts_with('{')));
        let preserved = match result {
            Ok(metadata) => {
                success
                    && json.is_some_and(|json| {
                        serde_json::from_str::<Metadata>(json).is_ok_and(|expected| {
                            matches!(
                                (serde_json::to_value(metadata), serde_json::to_value(expected)),
                                (Ok(actual), Ok(expected)) if actual == expected
                            )
                        })
                    })
            }
            Err(Error::CargoMetadata { stderr }) => !success && stderr.as_bytes() == bytes,
            Err(Error::ErrUtf8(error)) => !success && text.is_err() && error.as_bytes() == bytes,
            Err(Error::Utf8(error)) => success && text.err() == Some(error),
            Err(Error::NoJson) => success && text.is_ok() && json.is_none(),
            Err(Error::Json(_)) => {
                success && json.is_some_and(|json| serde_json::from_str::<Metadata>(json).is_err())
            }
            Err(Error::Io(_)) => false,
        };
        assert!(
            preserved,
            "Cargo output changed fields, failure bytes or error class"
        );
    }
}
