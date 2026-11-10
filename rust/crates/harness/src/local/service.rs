//! Rendezvous for the one local `server::serve` owner. The endpoint is a
//! disposable projection, usable only while the owner's OS lock is held.
//! Startup waits on an OS lock and the native readiness pipe, never probes.
use super::{Connection, LocalHarnessConfig, storage};
use crate::{Error, Result};
use acyclic_native_runtime::{FileLock, FileLockMode, ServiceStartOutcome, private_file};
use std::{
    ffi::OsString,
    io::Write as _,
    path::{Path, PathBuf},
};

/// The checkout's private service rendezvous, outside the repository.
#[derive(Clone)]
pub struct Directory(PathBuf);

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    version: u32,
    endpoint: String,
    credential: String,
}

impl Directory {
    /// Selects a checkout's service without constructing a model configuration.
    /// Uses the owner's existing incarnation-keyed state selection.
    pub fn for_workspace(workspace: &Path, state: Option<&Path>) -> Result<Self> {
        let state = match state {
            Some(state) => state.to_owned(),
            None => {
                let checkout = super::owner::Checkout::capture(workspace)?;
                super::owner::state_directory_in(
                    &super::owner::user_state_root()?,
                    &super::LocalProject::Workspace(workspace.to_owned()),
                    Some(&checkout),
                )?
            }
        };
        Self::at(&state, Some(workspace))
    }

    /// Selects the same checkout incarnation as the harness's durable state.
    /// Does not open storage, signing custody or the agent runtime.
    pub fn for_config(config: &LocalHarnessConfig) -> Result<Self> {
        let state = config.resolved_state_directory()?;
        let checkout = match &config.project {
            super::LocalProject::Workspace(path) => Some(path.as_path()),
            super::LocalProject::Volume { .. } => None,
        };
        Self::at(&state, checkout)
    }

    fn at(state: &Path, checkout: Option<&Path>) -> Result<Self> {
        let directory = super::owner::separated_directory(state, checkout)?;
        let directory = directory.join("service");
        private_file::create_directory_all(&directory).map_err(storage)?;
        Ok(Self(directory))
    }

    fn file(&self, name: &str) -> Result<std::fs::File> {
        private_file::open_or_create(&self.0.join(name)).map_err(storage)
    }

    /// Claims the owner's lifetime before it starts serving. A competing
    /// owner is refused; client startup alone uses the waiting startup lock.
    pub fn own(&self) -> Result<Owner> {
        // Acquire publication custody first: seeing a live owner then proves
        // there is an event to wait for even if its starter has disappeared.
        let publication =
            FileLock::try_acquire(self.file("publication.lock")?, FileLockMode::Exclusive)
                .map_err(storage)?;
        let lock = FileLock::try_acquire(self.file("owner.lock")?, FileLockMode::Exclusive)
            .map_err(storage)?;
        Ok(Owner {
            directory: self.clone(),
            _lock: lock,
            publication: std::sync::Mutex::new(Some(publication)),
        })
    }

    fn running(&self) -> Result<bool> {
        let occupied =
            || match FileLock::try_acquire(self.file("owner.lock")?, FileLockMode::Exclusive) {
                Ok(unowned) => {
                    drop(unowned);
                    Ok(false)
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(true),
                Err(error) => Err(storage(error)),
            };
        if !occupied()? {
            return Ok(false);
        }
        let published = FileLock::acquire(self.file("publication.lock")?, FileLockMode::Shared)
            .map_err(storage)?;
        // Publication or owner death released the original wait. Check lifetime
        // custody before admitting the descriptor, including an old one.
        let running = occupied();
        drop(published);
        running
    }

    /// Waits for an existing owner's publication, including after its starter
    /// exited. Does not open a model provider. Call from a blocking worker.
    pub fn existing(&self) -> Result<bool> {
        let _startup = FileLock::acquire(self.file("startup.lock")?, FileLockMode::Exclusive)
            .map_err(storage)?;
        self.running()
    }

    /// Attaches to the live owner or launches it once. `arguments` select the
    /// caller's executable's serving mode; the shared readiness protocol is
    /// appended by the native runtime. Call from a blocking worker.
    pub fn start(&self, executable: &Path, arguments: &[OsString]) -> Result<()> {
        let _startup = FileLock::acquire(self.file("startup.lock")?, FileLockMode::Exclusive)
            .map_err(storage)?;
        if self.running()? {
            return Ok(());
        }
        match acyclic_native_runtime::spawn_service_process(executable, arguments)
            .and_then(acyclic_native_runtime::ServiceReadiness::wait)
            .map_err(storage)?
        {
            ServiceStartOutcome::Ready => Ok(()),
            ServiceStartOutcome::Failed(reason) => Err(Error::Storage(reason)),
            ServiceStartOutcome::Exited => Err(Error::Storage(
                "local server exited before readiness".into(),
            )),
        }
    }

    /// Opens the owner's published capability through the ordinary client.
    /// Endpoint errors are returned as errors, never reinterpreted as death.
    pub async fn connect(&self) -> Result<Connection> {
        let file = private_file::open(&self.0.join("endpoint.json"), false).map_err(storage)?;
        let descriptor: Descriptor = serde_json::from_reader(file).map_err(storage)?;
        if descriptor.version != 1 {
            return Err(Error::Unsupported(
                "local service descriptor requires version 1".into(),
            ));
        }
        let options = acyclic_harness_client::ConnectionOptions::from_env(
            Some(&descriptor.endpoint),
            Some(&descriptor.credential),
        )
        .map_err(|error| Error::Invalid(error.code().into()))?;
        Connection::connect(&options).await
    }
}

/// OS ownership retained for the entire serving lifetime, including shutdown.
pub struct Owner {
    directory: Directory,
    publication: std::sync::Mutex<Option<FileLock>>,
    _lock: FileLock,
}

impl Owner {
    /// Publishes only after `server::serve` has admitted the runtime and client
    /// capability. The startup lock and readiness event hide partial writes.
    pub fn publish(&self, endpoint: &crate::server::Endpoint) -> Result<()> {
        let observation = endpoint
            .observation
            .as_ref()
            .ok_or_else(|| Error::Invalid("local service requires a client credential".into()))?;
        let mut file = self.directory.file("endpoint.json")?;
        file.set_len(0).map_err(storage)?;
        serde_json::to_writer(
            &mut file,
            &Descriptor {
                version: 1,
                endpoint: endpoint.endpoint.clone(),
                credential: observation.credential.clone(),
            },
        )
        .map_err(storage)?;
        file.flush().map_err(storage)?;
        let publication = self
            .publication
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(publication);
        Ok(())
    }
}

/// Runs the default local owner through the same server as every other host.
/// Readiness publication errors end the owner and reach its original starter.
pub async fn serve(
    config: crate::server::ServerConfig,
    owner: Owner,
    ready: acyclic_native_runtime::ServiceReadySignal,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) => {
            let _ = ready.fail(&error.to_string());
            return Err(storage(error));
        }
    };
    let (failed, failure) = tokio::sync::oneshot::channel();
    let mut ready = Some(ready);
    let mut publication = Ok(());
    let result = crate::server::serve(
        listener,
        config,
        |endpoint| {
            let Some(ready) = ready.take() else {
                return;
            };
            match owner.publish(endpoint) {
                Ok(()) => {
                    if let Err(error) = ready.signal() {
                        publication = Err(storage(error));
                        let _ = failed.send(());
                    }
                }
                Err(error) => {
                    let _ = ready.fail(&error.to_string());
                    publication = Err(error);
                    let _ = failed.send(());
                }
            }
        },
        async move {
            tokio::select! {
                _ = shutdown => {},
                // A dropped sender means successful readiness, not shutdown.
                () = async {
                    if failure.await.is_err() { std::future::pending().await }
                } => {},
            }
        },
    )
    .await;
    if let Some(ready) = ready
        && let Err(error) = &result
    {
        let _ = ready.fail(&error.to_string());
    }
    drop(owner);
    result.and(publication)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        local::agent_conversation::{Conversation, Settlement},
        model::Model,
        test_support::{ScriptedModelProvider, ScriptedTurn, local_harness_config},
    };
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn detached_clients_share_one_owner_and_one_prompt_commit() -> Result<()> {
        let workspace = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        let (entered, entering) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let gate = Mutex::new(Some((entered, released)));
        let provider = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text("settled after detach".into())])
                .with_context_capacity(crate::context::ModelContextCapacity {
                    context_tokens: 32_768,
                    output_tokens: Some(4096),
                })
                .with_dispatch_observer(move |_| {
                    let gate = gate.lock().unwrap().take();
                    Box::pin(async move {
                        let (entered, released) =
                            gate.ok_or_else(|| Error::Invalid("duplicate model dispatch".into()))?;
                        let _ = entered.send(());
                        released.await.map_err(storage)
                    })
                }),
        );
        let config = local_harness_config(
            workspace.path(),
            (
                Model::new("scripted".into(), "test".into(), "1".into())?,
                provider.clone(),
            ),
        )?
        .state_directory(state.path())
        // This lifecycle control executes no tools. Keep its small model's
        // request free of the unrelated coding-tool catalogue.
        .without_default_tools();
        let directory = Directory::for_config(&config)?;
        assert_eq!(
            directory.0,
            Directory::for_workspace(workspace.path(), Some(state.path()))?.0
        );
        let owner = directory.own()?;
        assert!(
            directory.own().is_err(),
            "only one runtime may own the checkout"
        );
        // The original starter may already have exited. A second starter's
        // wait belongs to the owner's publication lock, not the first caller.
        let attaching = directory.clone();
        let attachment = tokio::task::spawn_blocking(move || {
            attaching.start(Path::new("nonexistent-service-executable"), &[])
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(storage)?;
        let (ready, readiness) = tokio::sync::oneshot::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let result = crate::server::serve(
                listener,
                crate::server::ServerConfig::new(config),
                |endpoint| {
                    let result = owner.publish(endpoint);
                    let _ = ready.send(result);
                },
                async {
                    let _ = stopped.await;
                },
            )
            .await;
            drop(owner);
            result
        });
        let result = async {
            readiness.await.map_err(storage)??;
            attachment.await.map_err(storage)??;
            // Reattachment observes the owner, never starts a replacement or
            // dispatches a model to decide whether that owner is ready.
            assert!(directory.existing()?);
            directory.start(Path::new("nonexistent-service-executable"), &[])?;
            let first = Conversation::open(directory.connect().await?);
            let second = Conversation::open(directory.connect().await?);
            let mut events = second.observe(None).await?;
            let inbox = first
                .send("shared-operation", "finish after I detach")
                .await?;
            let mut entering = std::pin::pin!(entering);
            loop {
                tokio::select! {
                    entered = &mut entering => { entered.map_err(storage)?; break; },
                    event = events.next() => match event? {
                        super::super::Event::Turn(entry) => return Err(Error::Storage(format!(
                            "turn ended before model dispatch: {:?}", entry.outcome,
                        ))),
                        super::super::Event::Status(status) if matches!(status.phase, crate::agent::Phase::Done { .. }) => {
                            return Err(Error::Storage(format!("owner ended before model dispatch: {:?}", status.phase)));
                        }
                        _ => {},
                    }
                }
            }
            drop(first);
            assert_eq!(
                second
                    .send("shared-operation", "finish after I detach")
                    .await?,
                inbox
            );
            release
                .send(())
                .map_err(|()| Error::Storage("model gate ended".into()))?;
            let Settlement::Reply(crate::agent::Reply::Answered { entry, .. }) = second
                .settle("shared-operation", inbox, &mut events)
                .await?
            else {
                return Err(Error::Invalid("shared prompt did not settle".into()));
            };
            assert_eq!(second.output_text(&entry).await?, "settled after detach");
            assert_eq!(provider.request_count(), 1);
            drop((second, events));
            let reattached = Conversation::open(directory.connect().await?);
            assert_eq!(reattached.settled().await?, 1);
            Ok(())
        }
        .await;
        let _ = stop.send(());
        server.await.map_err(storage)??;
        result?;
        drop(directory.own()?);
        Ok(())
    }
}
