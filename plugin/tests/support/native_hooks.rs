//! Real hook-process fixture shared by focused integration suites.
#[path = "sdk_client.rs"]
pub mod sdk_client;
use serde_json::Value;
use std::{path::Path, process::Stdio};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    process::{Child, Command},
};
pub type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

pub struct Fixture {
    _root: tempfile::TempDir,
    project: std::path::PathBuf,
}

impl Fixture {
    pub fn new() -> Result<Self> {
        let root = tempfile::tempdir()?;
        let project = root.path().join("project");
        std::fs::create_dir(&project)?;
        Ok(Self {
            _root: root,
            project,
        })
    }
    pub fn path(&self) -> &Path {
        &self.project
    }
    pub async fn start_owner(&self) -> Result<Child> {
        let mut owner = command(self.path())
            .args(["__service", "--signal-ready"])
            .spawn()?;
        owner
            .stdout
            .take()
            .ok_or("owner readiness missing")?
            .read_u8()
            .await?;
        Ok(owner)
    }
    pub async fn close_owner(&self, owner: Child) -> Result {
        let stopped = self.close().await;
        let output = owner.wait_with_output().await?;
        if !output.status.success() {
            return Err(
                format!("owner failed: {}", String::from_utf8_lossy(&output.stderr)).into(),
            );
        }
        stopped
    }
    pub async fn close(&self) -> Result {
        let output = command(self.path()).arg("__owner-stop").output().await?;
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
        }
        Ok(())
    }
}

pub fn command(directory: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_acyclic"));
    command
        .current_dir(directory)
        .env_remove("ACYCLIC_HARNESS_BINDING")
        .env(
            "ACYCLIC_LOCAL_OWNER_DIR",
            directory
                .parent()
                .expect("workspace parent missing")
                .join("owner"),
        )
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(windows)]
    command.creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);
    command
}

pub async fn hook(directory: &Path, host: &str, event: &str, input: Value) -> Result<Value> {
    hook_using(command(directory), host, event, input).await
}

pub async fn hook_using(command: Command, host: &str, event: &str, input: Value) -> Result<Value> {
    let child = start_hook_using(command, host, event, input).await?;
    let output = child.wait_with_output().await?;
    if !output.status.success() {
        return Err(format!(
            "{host} {event}: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

pub async fn start_hook(
    directory: &Path,
    host: &str,
    event: &str,
    input: Value,
) -> Result<tokio::process::Child> {
    start_hook_using(command(directory), host, event, input).await
}

async fn start_hook_using(
    mut command: Command,
    host: &str,
    event: &str,
    input: Value,
) -> Result<tokio::process::Child> {
    let mut child = command.args(["__hook", host, event]).spawn()?;
    child
        .stdin
        .take()
        .ok_or("hook stdin missing")?
        .write_all(&serde_json::to_vec(&input)?)
        .await?;
    Ok(child)
}

pub async fn files(
    directory: &Path,
    path: &str,
) -> Result<acyclic_harness::task_volume::TaskVolume<acyclic_stream::grpc::Client>> {
    Ok(acyclic_harness::task_volume::TaskVolume::open_existing(
        std::sync::Arc::new(client(directory).await?),
        acyclic_stream::StreamPath::new(path)?,
    )
    .await?)
}

pub async fn client(directory: &Path) -> Result<acyclic_stream::grpc::Client> {
    let root = directory.parent().unwrap().join("owner");
    sdk_client::workspace(&root, directory).await
}
