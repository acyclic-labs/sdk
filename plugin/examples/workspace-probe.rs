//! Embed the shared workspace owner with an explicit native capability root.
use acyclic_harness::local::{LocalHarness, LocalHarnessConfig, host_files};
use std::{path::PathBuf, sync::Arc};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args_os()
        .skip(1)
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    let [workspace, state, probe_root] = args.as_slice() else {
        return Err("expected WORKSPACE STATE NATIVE_PROBE_ROOT".into());
    };
    let probe = acyclic_fs::NativeMountProbe::new(probe_root);
    if probe.probe().available() {
        return Err("qualification requires a native probe root without the mount device".into());
    }
    let harness = Arc::new(
        LocalHarness::open(
            LocalHarnessConfig::native_host(workspace)
                .state_directory(state)
                .native_mount_probe(probe),
        )
        .await?,
    );
    let directory = host_files::directory(&harness)?;
    host_files::serve_until(
        harness,
        || {
            use std::io::Write as _;
            std::io::stdout()
                .lock()
                .write_all(
                    format!(
                        "{}\n",
                        serde_json::json!({"type": "workspace-owner-ready", "directory": directory})
                    )
                    .as_bytes(),
                )
                .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))
        },
        async {
            tokio::io::copy(&mut tokio::io::stdin(), &mut tokio::io::sink())
                .await
                .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
            Ok(())
        },
    )
    .await?;
    Ok(())
}
