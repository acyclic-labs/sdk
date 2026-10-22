//! Exercise the public boundary on the process main thread's default stack.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let credentials = directory.path().join("identity.json");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    for command in ["whoami", "identity-mcp"] {
        let arguments = [
            command.to_owned(),
            "--endpoint".to_owned(),
            "http://127.0.0.1:1".to_owned(),
            "--credentials".to_owned(),
            credentials.display().to_string(),
            "--invalid-fixture-option".to_owned(),
        ];
        let pending = acyclic_plugin_identity::run(&arguments);
        let boundary_size = std::mem::size_of_val(&pending);
        if boundary_size != 2 * std::mem::size_of::<usize>() {
            return Err(format!(
                "identity state machine escaped its crate boundary: {boundary_size} bytes"
            )
            .into());
        }
        let Err(error) = runtime.block_on(pending) else {
            return Err("invalid command-line options must fail".into());
        };
        if !error.to_string().contains("--invalid-fixture-option") {
            return Err(format!("unexpected command error: {error}").into());
        }
    }
    println!("CLI and MCP entry boundaries passed on the default main-thread stack");
    Ok(())
}
