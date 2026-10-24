#![no_main]

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    use std::os::unix::process::ExitStatusExt;
    let bytes = bytes.to_vec();
    sdk_docs::with_command_executor(
        move |_| {
            Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(0),
                stdout: bytes.clone(),
                stderr: Vec::new(),
            })
        },
        || {
            let _ = sdk_docs::execute_metadata(&cargo_metadata::MetadataCommand::new());
        },
    );
});
