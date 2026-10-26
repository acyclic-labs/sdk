//! External command and `ConPTY` transport adapter over shared native custody.

fn main() {
    #[cfg(not(target_arch = "wasm32"))]
    let result = acyclic_native_runtime::run_external_process_host(std::env::args_os().skip(1));
    #[cfg(target_arch = "wasm32")]
    let result: std::io::Result<i32> = Err(std::io::ErrorKind::Unsupported.into());
    let code = match result {
        Ok(code) => code,
        Err(error) => {
            eprintln!(
                "step=process-host-admission-or-drain failed: {error}; recovery=acyclic-process-host [--lease ABSOLUTE_ROOT] [--completion ABSOLUTE_FILE] ABSOLUTE_EXECUTABLE ARGS"
            );
            125
        }
    };
    // The standalone host owns the blocking stdin-control reader's lifetime.
    std::process::exit(code);
}
