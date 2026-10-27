//! Standard vhost-user-fs, composed through the SDK's local-root Bound.

#[cfg(target_os = "linux")]
mod linux {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    use acyclic_harness::{
        LocalHarness, LocalHarnessConfig, filesystem::hosting::LocalGuestEnrollment,
        local::host_owner::HostWorkspaceOwner,
    };
    use serde::Deserialize;
    use std::{io::Read, path::PathBuf, sync::Arc};

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Config {
        directory: PathBuf,
        state_directory: PathBuf,
        enrollment: LocalGuestEnrollment,
        budget: WorkBudget,
    }

    pub(super) fn main() -> Result<(), Box<dyn std::error::Error>> {
        let mut arguments = std::env::args_os().skip(1);
        let mut socket = None;
        let mut config = None;
        while let Some(option) = arguments.next() {
            match option.to_str() {
                Some("--socket") => socket = arguments.next().map(PathBuf::from),
                Some("--config") => config = arguments.next().map(PathBuf::from),
                _ => return Err("step=parse arguments; recovery: acyclic-virtiofsd --socket PATH --config LOCAL_CONFIG.json".into()),
            }
        }
        let socket = socket.ok_or("step=select socket; recovery: acyclic-virtiofsd --socket PATH --config LOCAL_CONFIG.json")?;
        let config = config.ok_or("step=select local enrollment; recovery: acyclic-virtiofsd --socket PATH --config LOCAL_CONFIG.json")?;
        let mut bytes = Vec::new();
        std::fs::File::open(&config)?
            .take(64 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 64 * 1024 {
            return Err("step=read local enrollment; recovery: reduce LOCAL_CONFIG.json to at most 65536 bytes, then rerun acyclic-virtiofsd --socket PATH --config LOCAL_CONFIG.json".into());
        }
        let config: Config = serde_json::from_slice(&bytes)?;
        // The operator supplies the finite original allowance. Missing JSON
        // fields deserialize to zero, and cannot turn into an unbounded grant.
        if config.budget == WorkBudget::UNBOUNDED {
            return Err("step=admit session budget; recovery: set finite work counters in LOCAL_CONFIG.json, then rerun acyclic-virtiofsd --socket PATH --config LOCAL_CONFIG.json".into());
        }
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async move {
                let cancellation = CancellationToken::new();
                let work = NativeMaterializationWork::new(config.budget, &cancellation)
                    .map_err(|(cause, _)| cause)?;
                let local = Arc::new(
                    LocalHarness::open(
                        LocalHarnessConfig::native_host(config.directory)
                            .volume(config.enrollment.volume.clone())
                            .state_directory(config.state_directory)
                            .hermetic(),
                    )
                    .await?,
                );
                let owner = HostWorkspaceOwner::new(local)?;
                let admission = owner.guest_session(config.enrollment, &work).await?;
                // Bind refuses an existing path; never unlink another server's
                // socket. Cleanup below compares the actual bound inode.
                let listener = tokio::net::UnixListener::bind(&socket)?;
                let cleanup = Socket {
                    path: socket.clone(),
                    identity: std::fs::symlink_metadata(&socket)?,
                };
                let connection = tokio::select! {
                    accepted = listener.accept() => accepted?.0.into_std()?,
                    signal = tokio::signal::ctrl_c() => { signal?; return Ok(()); }
                };
                drop(listener);
                let mut server =
                    tokio::task::spawn_blocking(move || admission.serve(connection, b"acyclic"));
                let retired = tokio::select! {
                    result = &mut server => result??,
                    signal = tokio::signal::ctrl_c() => {
                        cancellation.cancel();
                        let retired = server.await??;
                        signal?;
                        retired
                    }
                };
                let callback = retired.take_callback_failure();
                let transport = retired.transport();
                drop(cleanup);
                // Keep the real local owner and original work until every physical
                // callback and descriptor worker has joined.
                drop(owner);
                if let Some((cause, _work)) = callback {
                    return Err(cause.into());
                }
                transport?;
                Ok::<_, Box<dyn std::error::Error>>(())
            })
    }

    struct Socket {
        path: PathBuf,
        identity: std::fs::Metadata,
    }
    impl Drop for Socket {
        fn drop(&mut self) {
            use std::os::unix::fs::MetadataExt;
            if std::fs::symlink_metadata(&self.path).is_ok_and(|current| {
                current.dev() == self.identity.dev() && current.ino() == self.identity.ino()
            }) {
                if let Err(error) = std::fs::remove_file(&self.path) {
                    eprintln!(
                        "step=retire owned socket; error={error}; recovery: rm -- {}",
                        self.path.display()
                    );
                }
            }
        }
    }
}

fn main() {
    let recovery = std::env::args_os()
        .map(|argument| format!("'{}'", argument.to_string_lossy().replace('\'', "'\\''")))
        .collect::<Vec<_>>()
        .join(" ");
    #[cfg(target_os = "linux")]
    if let Err(error) = linux::main() {
        eprintln!("step=serve admitted virtio-fs session; error={error}; recovery: {recovery}");
        std::process::exit(1);
    }
    #[cfg(not(target_os = "linux"))]
    {
        eprintln!("step=select virtio-fs transport; run on Linux; recovery: {recovery}");
        std::process::exit(1);
    }
}
