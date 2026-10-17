//! Owned qualification processes; task state is queried through the SDK.
#[path = "sdk_client.rs"]
mod sdk_client;
use super::{ACYCLIC, command, isolated_state};
use acyclic_harness::{
    external_executor::ExternalRequest,
    task_volume::{Component, FileKey, Query, TaskFile, TaskVolume},
};
use acyclic_native_runtime::ProcessTree;
use acyclic_stream::{StreamPath, StreamProvider as _};
use fs2::FileExt as _;
use futures::StreamExt as _;
use std::{
    cell::RefCell,
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tokio::{io::AsyncReadExt as _, process::Child, runtime::Runtime};

pub struct ServiceGuard {
    home: PathBuf,
    runtime: Runtime,
    owner: Option<Child>,
    process_trees: RefCell<Vec<ProcessTree>>,
    active: bool,
}

impl ServiceGuard {
    pub fn new(home: &Path) -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("qualification runtime");
        let mut spawn = command(ACYCLIC);
        isolated_state(&mut spawn, home);
        spawn
            .args(["__service", "--signal-ready"])
            .current_dir(home)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        super::bind_native_compiler(&mut spawn);
        spawn.env("ACYCLIC_WORKFLOW_TOKEN", "qualified");
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt as _;
            spawn.creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);
        }
        let owner = runtime
            .block_on(async {
                let mut owner = tokio::process::Command::from(spawn)
                    .kill_on_drop(true)
                    .spawn()?;
                tokio::time::timeout(
                    Duration::from_secs(3),
                    owner
                        .stdout
                        .take()
                        .ok_or("missing owner readiness")?
                        .read_u8(),
                )
                .await??;
                Ok::<_, Box<dyn std::error::Error>>(owner)
            })
            .expect("start owned SDK provider");
        Self {
            home: home.into(),
            runtime,
            owner: Some(owner),
            process_trees: RefCell::new(Vec::new()),
            active: true,
        }
    }

    pub fn attach_process_tree(&self, tree: ProcessTree) {
        self.process_trees.borrow_mut().push(tree);
    }

    pub fn assert_hook_service_live(&mut self) -> String {
        self.runtime
            .block_on(async {
                let root = self.home.join("owner");
                let address: serde_json::Value =
                    serde_json::from_slice(&fs::read(root.join("address.json"))?)?;
                let instance = address["instance"]
                    .as_str()
                    .ok_or("missing owner instance")?;
                let client = Arc::new(sdk_client::connect(&root, "owner").await?);
                let control =
                    TaskVolume::open_existing(client, StreamPath::new("owner/control")?).await?;
                let key = FileKey::Task {
                    task: Component::new("owner")?,
                    file: TaskFile::Request(Component::new(instance)?),
                };
                assert!(
                    control
                        .snapshot(&Query::File(key), 1)
                        .await?
                        .files
                        .is_empty()
                );
                assert!(
                    assert_service_absent(&self.home).is_err(),
                    "live owner must hold its instance lock"
                );
                Ok::<_, Box<dyn std::error::Error>>(instance.to_owned())
            })
            .expect("authenticated SDK owner query")
    }

    pub fn assert_requests_settled(&self, workspace: &Path) -> Vec<ExternalRequest> {
        self.runtime
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(3), settled(&self.home, workspace)).await?
            })
            .expect("all accepted hook requests must have settled result files")
    }

    pub fn drain(mut self) {
        self.cleanup().expect("drain owned provider");
    }

    pub fn assert_timeout_cleanup(mut self, process_tree: ProcessTree) {
        self.attach_process_tree(process_tree);
        self.assert_hook_service_live();
        self.cleanup().expect("timed-out host owner cleanup");
    }

    fn cleanup(&mut self) -> Result<(), String> {
        for tree in self.process_trees.get_mut().iter_mut() {
            tree.terminate(None).map_err(|error| error.to_string())?;
        }
        self.process_trees.get_mut().clear();
        if assert_service_absent(&self.home).is_err() {
            let mut stop = command(ACYCLIC);
            isolated_state(&mut stop, &self.home);
            stop.arg("__owner-stop").current_dir(&self.home);
            #[cfg(windows)]
            {
                use std::os::windows::process::CommandExt as _;
                stop.creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);
            }
            let stopped = self.runtime.block_on(async {
                tokio::time::timeout(
                    Duration::from_secs(3),
                    tokio::process::Command::from(stop)
                        .kill_on_drop(true)
                        .output(),
                )
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())
            })?;
            if !stopped.status.success() {
                return Err(String::from_utf8_lossy(&stopped.stderr).into_owned());
            }
        }
        if let Some(owner) = self.owner.take() {
            let output = self.runtime.block_on(async {
                tokio::time::timeout(Duration::from_secs(3), owner.wait_with_output())
                    .await
                    .map_err(|error| error.to_string())?
                    .map_err(|error| error.to_string())
            })?;
            if !output.status.success() {
                return Err(String::from_utf8_lossy(&output.stderr).into_owned());
            }
        }
        assert_service_absent(&self.home)?;
        self.active = false;
        Ok(())
    }
}

impl Drop for ServiceGuard {
    fn drop(&mut self) {
        if self.active
            && let Err(error) = self.cleanup()
        {
            if std::thread::panicking() {
                eprintln!("owner cleanup failed during unwind: {error}");
            } else {
                panic!("owner cleanup failed: {error}");
            }
        }
    }
}

pub fn assert_service_absent(home: &Path) -> Result<(), String> {
    let lock = match fs::File::open(home.join("owner/instance.lock")) {
        Ok(lock) => lock,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.to_string()),
    };
    lock.try_lock_exclusive().map_err(|error| error.to_string())
}

async fn settled(
    home: &Path,
    workspace: &Path,
) -> Result<Vec<ExternalRequest>, Box<dyn std::error::Error>> {
    let client = Arc::new(sdk_client::workspace(&home.join("owner"), workspace).await?);
    let page = client
        .children_page(acyclic_stream::ChildrenPageRequest {
            parent: Some(StreamPath::new("external/requests")?),
            after: None,
            hierarchy_version: None,
            limit: 64,
        })
        .await?;
    assert!(
        page.next_after.is_none(),
        "qualification task discovery must be complete"
    );
    let mut accepted = Vec::new();
    for child in page.children {
        let task = Component::new(
            child
                .path
                .as_str()
                .rsplit('/')
                .next()
                .ok_or("missing task")?,
        )?;
        let requests = TaskVolume::open_existing(Arc::clone(&client), child.path).await?;
        let results = TaskVolume::open_existing(
            Arc::clone(&client),
            StreamPath::new(format!("external/results/{}", task.as_str()))?,
        )
        .await?;
        let snapshot = requests
            .snapshot(&Query::Requests(task.clone()), 4096)
            .await?;
        assert!(
            snapshot.files.len() < 4096,
            "qualification request selection must be complete"
        );
        for file in snapshot.files {
            if let FileKey::Task {
                file: TaskFile::Request(operation),
                ..
            } = &file.key
            {
                accepted.push(ExternalRequest::from_file(&task, &file)?);
                let key = FileKey::Task {
                    task: task.clone(),
                    file: TaskFile::RequestResult(operation.clone()),
                };
                let mut observed = results.observe(Query::File(key.clone()), None, 1).await?;
                if observed
                    .snapshot
                    .is_some_and(|snapshot| !snapshot.files.is_empty())
                {
                    continue;
                }
                loop {
                    let change = observed
                        .changes
                        .next()
                        .await
                        .ok_or("result follow ended")??;
                    if change.file.is_some_and(|file| file.key == key) {
                        break;
                    }
                }
            }
        }
    }
    assert!(
        !accepted.is_empty(),
        "installed hooks must publish requests; an idle owner is insufficient"
    );
    Ok(accepted)
}
