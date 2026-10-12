//! Catalog for the service.
use super::*;

pub(crate) struct OpeningSession {
    pub(crate) key: SessionKey,
    pub(crate) cancelled: bool,
    pub(crate) complete: watch::Sender<bool>,
}

#[derive(Default)]
pub(crate) struct SessionCatalog {
    pub(crate) next_epoch: u64,
    pub(crate) opening: BTreeMap<String, OpeningSession>,
    pub(crate) active: BTreeMap<String, Arc<SessionHandle>>,
    pub(crate) draining: BTreeMap<SessionKey, Arc<SessionHandle>>,
    pub(crate) completed: BTreeSet<String>,
}

pub(crate) struct ConcurrentServiceControl {
    pub(crate) resources: ServiceResources,
    pub(crate) lifecycle: std::sync::atomic::AtomicU8,
    pub(crate) shutdown_deactivates: std::sync::atomic::AtomicBool,
    pub(crate) catalog: AsyncMutex<SessionCatalog>,
}

impl std::ops::Deref for ConcurrentServiceControl {
    type Target = ServiceResources;

    fn deref(&self) -> &Self::Target {
        &self.resources
    }
}

impl ConcurrentServiceControl {
    pub(crate) async fn open(
        data: PathBuf,
        configured: ServiceConfiguredInfrastructure,
        private_state_root: acyclic_fs::materializer::NativeHostPrivateStateRoot,
        original_bootstrap_work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<Self> {
        let (resources, sessions) = ServiceResources::open(
            data,
            configured,
            private_state_root,
            original_bootstrap_work,
        )
        .await?;
        Self::from_parts(resources, sessions, original_bootstrap_work)
    }

    pub(crate) fn from_parts(
        resources: ServiceResources,
        sessions: BTreeMap<String, SessionController>,
        original_bootstrap_work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<Self> {
        let mut catalog = SessionCatalog::default();
        for (session_id, control) in sessions {
            catalog.next_epoch += 1;
            let key = SessionKey {
                session_id: session_id.clone(),
                epoch: catalog.next_epoch,
            };
            let actor_resources = pi_root_provider::service_resources_with_native_work(
                &resources,
                original_bootstrap_work,
            )?;
            catalog.active.insert(
                session_id,
                Arc::new(SessionHandle::new(
                    key,
                    SessionController::HostManaged(Box::new(control)),
                    (actor_resources.0, Some(actor_resources.1)),
                )),
            );
        }
        Ok(Self {
            resources,
            lifecycle: std::sync::atomic::AtomicU8::new(SERVICE_RUNNING),
            shutdown_deactivates: std::sync::atomic::AtomicBool::new(false),
            catalog: AsyncMutex::new(catalog),
        })
    }

    pub(crate) fn ensure_running(&self) -> Result<(), String> {
        if self.lifecycle.load(std::sync::atomic::Ordering::Acquire) == SERVICE_RUNNING {
            Ok(())
        } else {
            Err("Acyclic service is draining".to_owned())
        }
    }

    pub(crate) async fn session(&self, session_id: &str) -> Result<Arc<SessionHandle>, String> {
        self.catalog
            .lock()
            .await
            .active
            .get(session_id)
            .cloned()
            .ok_or_else(|| "Acyclic native hook session is not registered".to_owned())
    }

    pub(crate) async fn start_session(
        &self,
        session_id: &str,
        host: &str,
        root: PathBuf,
        mut input: Value,
        original_native_work: &mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<(Arc<SessionHandle>, ControlDispatchResponse)> {
        let root = root
            .canonicalize()
            .map_err(|error| Error::Storage(display(error)))?;
        let arguments = input.as_object_mut().ok_or_else(|| {
            Error::Invalid("session bootstrap input must be an object".to_owned())
        })?;
        arguments.insert("session_id".to_owned(), json!(session_id));
        arguments.insert("cwd".to_owned(), json!(root));
        if let Some(owner) = self.route_session(&root).await.map_err(Error::Storage)?
            && owner.key.session_id != session_id
        {
            return Err(Error::Invalid(format!(
                "session {session_id} cannot attach a workspace owned by session {} as a physical root",
                owner.key.session_id
            )));
        }
        loop {
            self.ensure_running().map_err(Error::Invalid)?;
            let (key, wait) = {
                let mut catalog = self.catalog.lock().await;
                if let Some(handle) = catalog.active.get(session_id) {
                    let handle = Arc::clone(handle);
                    drop(catalog);
                    let output = handle
                        .dispatch(
                            ControlRequest {
                                version: 1,
                                command: ControlCommand::Hook,
                                cwd: root.clone(),
                                argv: Vec::new(),
                                name: format!("{host}:SessionStart"),
                                arguments: input.clone(),
                            },
                            original_native_work,
                        )
                        .await?;
                    return Ok((handle, output));
                }
                if let Some(opening) = catalog.opening.get(session_id) {
                    (opening.key.clone(), Some(opening.complete.subscribe()))
                } else {
                    catalog.completed.remove(session_id);
                    catalog.next_epoch += 1;
                    let key = SessionKey {
                        session_id: session_id.to_owned(),
                        epoch: catalog.next_epoch,
                    };
                    catalog.opening.insert(
                        session_id.to_owned(),
                        OpeningSession {
                            key: key.clone(),
                            cancelled: false,
                            complete: watch::channel(false).0,
                        },
                    );
                    (key, None)
                }
            };
            if let Some(mut wait) = wait {
                if !*wait.borrow() {
                    wait.changed()
                        .await
                        .map_err(|error| Error::Storage(display(error)))?;
                }
                continue;
            }

            let opening_result = async {
                if host == "pi" {
                    if original_native_work.is_none() {
                        let cancellation = CancellationToken::new();
                        *original_native_work = Some(
                            acyclic_fs::NativeMaterializationWork::new(
                                pi_root_provider::allocation_budget(),
                                &cancellation,
                            )
                            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?,
                        );
                    }
                    let work =
                        original_native_work
                            .as_ref()
                            .ok_or_else(|| Error::NativeBoundary {
                                code: acyclic_harness::wire::ErrorCode::Storage,
                                reason: "Pi session bootstrap lost its original query work",
                            })?;
                    let actor_resources = pi_root_provider::service_resources_with_native_work(
                        &self.resources,
                        work,
                    )?;
                    let native_runtime = input
                        .get("runtime")
                        .cloned()
                        .map(serde_json::from_value)
                        .transpose()
                        .map_err(|error| Error::Invalid(display(error)))?;
                    let mut control = pi_registration::register(
                        &self.resources,
                        session_id,
                        root.clone(),
                        native_runtime,
                        work,
                    )
                    .await?;
                    let opened = control
                        .dispatch(
                            ControlRequest {
                                version: 1,
                                command: ControlCommand::Hook,
                                cwd: root.clone(),
                                argv: Vec::new(),
                                name: "pi:SessionStart".to_owned(),
                                arguments: input.clone(),
                            },
                            original_native_work,
                        )
                        .await;
                    Ok::<_, Error>((
                        control,
                        opened,
                        (actor_resources.0, Some(actor_resources.1)),
                    ))
                } else {
                    let mut control = self
                        .resources
                        .open_session_directory(self.resources.session_directory(session_id))
                        .await
                        .map_err(Error::Storage)?;
                    let opened = control
                        .session_start(json!({"session_id": session_id, "cwd": root, "host": host}))
                        .await
                        .map(|value| (value, None))
                        .map_err(Error::Storage);
                    Ok((control.into(), opened, (self.resources.clone(), None)))
                }
            }
            .await;
            let (control, opened, actor_resources) = match opening_result {
                Ok(opened) => opened,
                Err(error) => {
                    let mut catalog = self.catalog.lock().await;
                    let opening = catalog.opening.remove(session_id).ok_or_else(|| {
                        Error::Storage("Acyclic session opening reservation disappeared".to_owned())
                    })?;
                    if opening.key != key {
                        return Err(Error::Storage(
                            "Acyclic session opening epoch changed".to_owned(),
                        ));
                    }
                    opening.complete.send_replace(true);
                    return Err(error);
                }
            };
            let handle = Some(Arc::new(SessionHandle::new(
                key.clone(),
                control,
                actor_resources,
            )));
            let (cancelled, complete) = {
                let mut catalog = self.catalog.lock().await;
                let opening = catalog.opening.remove(session_id).ok_or_else(|| {
                    Error::Storage("Acyclic session opening reservation disappeared".to_owned())
                })?;
                if opening.key != key {
                    return Err(Error::Storage(
                        "Acyclic session opening epoch changed".to_owned(),
                    ));
                }
                let cancelled = opened.is_err()
                    || opening.cancelled
                    || self.lifecycle.load(std::sync::atomic::Ordering::Acquire) != SERVICE_RUNNING;
                if let Some(handle) = &handle {
                    if cancelled {
                        catalog
                            .draining
                            .insert(handle.key.clone(), Arc::clone(handle));
                    } else {
                        catalog
                            .active
                            .insert(session_id.to_owned(), Arc::clone(handle));
                    }
                }
                (cancelled, opening.complete)
            };
            let output = match opened {
                Ok(output) => output,
                Err(error) => {
                    let cleanup = if let Some(handle) = &handle {
                        let cleanup = handle.shutdown(true, original_native_work).await;
                        if cleanup
                            .as_ref()
                            .is_ok_and(|status| *status == SessionCloseState::Closed)
                        {
                            let mut catalog = self.catalog.lock().await;
                            catalog.draining.remove(&handle.key);
                            catalog.completed.insert(session_id.to_owned());
                        }
                        cleanup
                    } else {
                        Ok(SessionCloseState::Closed)
                    };
                    let _ = complete.send(true);
                    return match cleanup {
                        Ok(_) => Err(error),
                        Err(_) => Err(error),
                    };
                }
            };
            let handle =
                handle.ok_or_else(|| Error::Storage("Acyclic session did not open".to_owned()))?;
            if cancelled {
                let cleanup = handle.shutdown(true, original_native_work).await;
                if cleanup
                    .as_ref()
                    .is_ok_and(|status| *status == SessionCloseState::Closed)
                {
                    let mut catalog = self.catalog.lock().await;
                    catalog.draining.remove(&handle.key);
                    catalog.completed.insert(session_id.to_owned());
                }
                let _ = complete.send(true);
                let _ = cleanup?;
                return Err(Error::Invalid(
                    "Acyclic session start was cancelled".to_owned(),
                ));
            }
            let _ = complete.send(true);
            return Ok((handle, output));
        }
    }

    pub(crate) async fn end_session(
        &self,
        session_id: &str,
        deactivate: bool,
        original_native_work: &mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<SessionCloseState> {
        let mut cancelled_opening = false;
        loop {
            let wait = {
                let mut catalog = self.catalog.lock().await;
                if let Some(opening) = catalog.opening.get_mut(session_id) {
                    opening.cancelled = true;
                    cancelled_opening = true;
                    Some(opening.complete.subscribe())
                } else {
                    None
                }
            };
            if let Some(mut wait) = wait {
                if !*wait.borrow() {
                    wait.changed()
                        .await
                        .map_err(|error| Error::Storage(display(error)))?;
                }
                continue;
            }
            break;
        }
        let handle = {
            let mut catalog = self.catalog.lock().await;
            if let Some(handle) = catalog.active.remove(session_id) {
                catalog
                    .draining
                    .insert(handle.key.clone(), Arc::clone(&handle));
                handle
            } else if let Some(handle) = catalog
                .draining
                .values()
                .find(|handle| handle.key.session_id == session_id)
                .cloned()
            {
                handle
            } else {
                if cancelled_opening {
                    return Ok(SessionCloseState::Closed);
                }
                if catalog.completed.contains(session_id) {
                    return Ok(SessionCloseState::Closed);
                }
                return Err(Error::Invalid(
                    "Acyclic native hook session is not registered".to_owned(),
                ));
            }
        };
        let result = handle.shutdown(deactivate, original_native_work).await;
        {
            let mut catalog = self.catalog.lock().await;
            if result
                .as_ref()
                .is_ok_and(|status| *status == SessionCloseState::Closed)
            {
                catalog.draining.remove(&handle.key);
                catalog.completed.insert(session_id.to_owned());
            }
        }
        if result
            .as_ref()
            .is_ok_and(|status| *status == SessionCloseState::Closed)
        {
            self.shared_roots.prune().await;
        }
        result
    }

    pub(crate) async fn route_session(
        &self,
        cwd: &Path,
    ) -> Result<Option<Arc<SessionHandle>>, String> {
        let cwd = cwd.canonicalize().map_err(display)?;
        let handles = self.catalog.lock().await;
        let handles = handles
            .active
            .values()
            .chain(handles.draining.values())
            .cloned()
            .collect::<Vec<_>>();
        let mut matches = Vec::new();
        for handle in handles {
            let score = handle.with_snapshot(|snapshot| {
                let Some(SessionSnapshot::HostManaged(snapshot)) = snapshot else {
                    return Ok::<_, String>(None);
                };
                snapshot.require_created_roots()?;
                let mut score = snapshot
                    .roots
                    .values()
                    .filter_map(|root| root.path.canonicalize().ok())
                    .filter(|root| cwd.starts_with(root))
                    .map(|root| root.components().count())
                    .max();
                for route in snapshot
                    .routes
                    .values()
                    .filter(|route| route.lifecycle.needs_mount())
                {
                    if let Ok(mount) = route.mount_path.canonicalize()
                        && cwd.starts_with(&mount)
                    {
                        score = Some(score.unwrap_or(0).max(mount.components().count()));
                    }
                }
                Ok(score)
            })??;
            if let Some(score) = score {
                matches.push((score, handle));
            }
        }
        matches.sort_by_key(|(score, _)| std::cmp::Reverse(*score));
        let Some((score, handle)) = matches.first() else {
            return Ok(None);
        };
        if matches
            .get(1)
            .is_some_and(|candidate| candidate.0 == *score)
        {
            return Err(
                "cwd belongs to multiple Acyclic sessions; run inside a managed child mount"
                    .to_owned(),
            );
        }
        Ok(Some(Arc::clone(handle)))
    }

    pub(crate) async fn drain_sessions(
        &self,
        deactivate: bool,
    ) -> acyclic_harness::Result<SessionCloseState> {
        self.lifecycle
            .store(SERVICE_DRAINING, std::sync::atomic::Ordering::Release);
        loop {
            let waits = {
                let mut catalog = self.catalog.lock().await;
                if catalog.opening.is_empty() {
                    break;
                }
                catalog
                    .opening
                    .values_mut()
                    .map(|opening| {
                        opening.cancelled = true;
                        opening.complete.subscribe()
                    })
                    .collect::<Vec<_>>()
            };
            for mut wait in waits {
                if !*wait.borrow() {
                    wait.changed()
                        .await
                        .map_err(|error| Error::Storage(display(error)))?;
                }
            }
        }
        let handles = {
            let mut catalog = self.catalog.lock().await;
            let active = std::mem::take(&mut catalog.active);
            for handle in active.values() {
                catalog
                    .draining
                    .insert(handle.key.clone(), Arc::clone(handle));
            }
            catalog.draining.values().cloned().collect::<Vec<_>>()
        };
        let mut status = SessionCloseState::Closed;
        let mut first_error = None;
        for handle in handles {
            match handle.shutdown(deactivate, &mut None).await {
                Ok(SessionCloseState::Closed) => {
                    self.catalog.lock().await.draining.remove(&handle.key);
                }
                Ok(SessionCloseState::Failed) => status = SessionCloseState::Failed,
                Err(error) => {
                    status = SessionCloseState::Failed;
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        if let Some(error) = first_error {
            Err(error)
        } else {
            if status == SessionCloseState::Closed {
                self.lifecycle
                    .store(SERVICE_CLOSED, std::sync::atomic::Ordering::Release);
            }
            Ok(status)
        }
    }

    /// Stops admitting requests ahead of a drain that ends every session, as a
    /// stop request asks; see [`ServiceMarker`].
    pub(crate) fn begin_drain(&self) {
        self.shutdown_deactivates
            .store(true, std::sync::atomic::Ordering::Release);
        self.lifecycle
            .store(SERVICE_DRAINING, std::sync::atomic::Ordering::Release);
    }

    pub(crate) async fn shutdown(self) -> acyclic_harness::Result<SessionCloseState> {
        let root_released = self.fs.local_root_release_barrier();
        let deactivate = self
            .shutdown_deactivates
            .load(std::sync::atomic::Ordering::Acquire);
        let result = self.drain_sessions(deactivate).await;
        self.shared_roots.collection.stop().await;
        drop(self);
        let released = wait_for_root_release(root_released).await;
        match result {
            Err(error) => Err(error),
            Ok(status) => {
                released.map_err(Error::Storage)?;
                Ok(status)
            }
        }
    }
}
