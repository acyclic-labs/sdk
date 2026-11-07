use super::child;
use cargo_metadata::{Error, Metadata};
use std::process::{ExitStatus, Output};

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
        match result {
            Ok(metadata) => {
                assert!(success, "Cargo output lost failure status");
                let expected: Metadata =
                    serde_json::from_str(json.expect("Cargo output admitted missing JSON"))
                        .expect("Cargo output admitted invalid metadata");
                assert!(
                    serde_json::to_value(metadata).unwrap()
                        == serde_json::to_value(expected).unwrap(),
                    "Cargo output lost metadata fields"
                );
            }
            Err(Error::CargoMetadata { stderr }) => {
                assert!(!success, "Cargo output lost success status");
                assert!(
                    stderr.as_bytes() == bytes,
                    "Cargo output lost refusal details"
                );
            }
            Err(Error::ErrUtf8(error)) => {
                assert!(!success, "Cargo output lost success status");
                assert!(
                    text.is_err() && error.as_bytes() == bytes,
                    "Cargo output lost invalid refusal bytes"
                );
            }
            Err(Error::Utf8(error)) => {
                assert!(success, "Cargo output lost failure status");
                assert_eq!(text.unwrap_err(), error);
            }
            Err(Error::NoJson) => {
                assert!(success, "Cargo output lost failure status");
                assert!(
                    text.is_ok() && json.is_none(),
                    "Cargo output lost JSON admission"
                );
            }
            Err(Error::Json(_)) => {
                assert!(success, "Cargo output lost failure status");
                assert!(
                    serde_json::from_str::<Metadata>(
                        json.expect("Cargo output lost JSON admission")
                    )
                    .is_err(),
                    "Cargo output rejected valid metadata"
                );
            }
            Err(Error::Io(_)) => panic!("Cargo output decoder performed I/O"),
        }
    }
}
