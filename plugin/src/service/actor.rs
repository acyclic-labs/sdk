//! Actor for the service.
use super::*;

pub(crate) const SESSION_ACTIVE: u8 = 0;
pub(crate) const SESSION_CLOSING: u8 = 1;
pub(crate) const SESSION_CLOSED: u8 = 2;
pub(crate) const SESSION_CLOSE_FAILED: u8 = 3;
pub(crate) const SESSION_DATA_CAPACITY: usize = 64;
pub(crate) const SESSION_QUEUE_CAPACITY: usize = SESSION_DATA_CAPACITY + 1;
pub(crate) const SERVICE_RUNNING: u8 = 0;
pub(crate) const SERVICE_DRAINING: u8 = 1;
pub(crate) const SERVICE_CLOSED: u8 = 2;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct SessionKey {
    pub(crate) session_id: String,
    pub(crate) epoch: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub(crate) enum SessionCloseState {
    Closed,
    Failed,
}

/// Readonly service metadata, never a native allocation or authority proof.
pub(crate) enum SessionSnapshot {
    HostManaged(AdapterState),
    Pi {
        original_task_id: acyclic_harness::TaskId,
        pending_root_registration: bool,
        roots_created: bool,
        scratch_created: bool,
        routes: usize,
        leases: usize,
        pending_recovery: bool,
    },
}

impl SessionSnapshot {
    fn observe(control: &SessionController) -> Self {
        let state = control.state();
        match &state.task_binding {
            PluginTaskBinding::Pi(binding) => Self::Pi {
                original_task_id: binding.task_id,
                pending_root_registration: state.pending_root_registration,
                roots_created: state
                    .roots
                    .values()
                    .all(|root| matches!(root.allocation, RootAllocation::Created(_))),
                scratch_created: matches!(
                    binding.scratch.allocation,
                    PiScratchAllocation::Created(_)
                ),
                routes: state.routes.len(),
                leases: state.leases.len(),
                pending_recovery: !state.pending_discards.is_empty() || !state.pending.is_empty(),
            },
            _ => Self::HostManaged(state.clone()),
        }
    }
}

pub(crate) struct SessionHandle {
    pub(crate) key: SessionKey,
    pub(crate) state: Arc<std::sync::atomic::AtomicU8>,
    pub(crate) admission: Mutex<()>,
    pub(crate) sender: tokio::sync::mpsc::Sender<SessionCommand>,
    pub(crate) data_slots: Arc<tokio::sync::Semaphore>,
    pub(crate) snapshot: Arc<std::sync::RwLock<Option<SessionSnapshot>>>,
    pub(crate) close_result: watch::Sender<Option<SessionCloseState>>,
}

pub(crate) enum SessionCommand {
    Dispatch {
        request: ControlRequest,
        original_native_work: Option<acyclic_fs::NativeMaterializationWork>,
        response: tokio::sync::oneshot::Sender<(
            acyclic_harness::Result<ControlDispatchResponse>,
            Option<acyclic_fs::NativeMaterializationWork>,
        )>,
        _permit: tokio::sync::OwnedSemaphorePermit,
    },
    Shutdown {
        deactivate: bool,
        original_native_work: Option<acyclic_fs::NativeMaterializationWork>,
        response: tokio::sync::oneshot::Sender<(
            acyclic_harness::Result<SessionCloseState>,
            Option<acyclic_fs::NativeMaterializationWork>,
        )>,
    },
    #[cfg(test)]
    Pause {
        entered: tokio::sync::oneshot::Sender<()>,
        resume: tokio::sync::oneshot::Receiver<()>,
    },
}

impl SessionHandle {
    pub(crate) fn new(
        key: SessionKey,
        control: SessionController,
        resources: (ServiceResources, Option<acyclic_fs::NativeAllocation>),
    ) -> Self {
        let snapshot = Arc::new(std::sync::RwLock::new(Some(SessionSnapshot::observe(
            &control,
        ))));
        let actor_snapshot = Arc::clone(&snapshot);
        let state = Arc::new(std::sync::atomic::AtomicU8::new(SESSION_ACTIVE));
        let actor_state = Arc::clone(&state);
        let close_result = watch::channel(None).0;
        let actor_close_result = close_result.clone();
        let data_slots = Arc::new(tokio::sync::Semaphore::new(SESSION_DATA_CAPACITY));
        let (sender, mut receiver) = tokio::sync::mpsc::channel(SESSION_QUEUE_CAPACITY);
        tokio::spawn(async move {
            let mut undelivered_close_failure: Option<Error> = None;
            let mut control = Some(control);
            while let Some(command) = receiver.recv().await {
                match command {
                    SessionCommand::Dispatch {
                        request,
                        mut original_native_work,
                        response,
                        _permit,
                    } => {
                        let result = match control.as_mut() {
                            Some(control) => {
                                control.dispatch(request, &mut original_native_work).await
                            }
                            None => Err(Error::Invalid(
                                "Acyclic session actor has no workspace".to_owned(),
                            )),
                        };
                        // Other sessions see only durable state: an unflushed
                        // save is published once flushed, after the reply.
                        let mut pending = Some((response, (result, original_native_work)));
                        if control.as_ref().is_some_and(SessionController::unflushed)
                            && let Some((response, result)) = pending.take()
                        {
                            let _ = response.send(result);
                        }
                        let durable = match control.as_mut() {
                            Some(control) => control.make_durable().await.is_ok(),
                            None => true,
                        };
                        if durable {
                            let next = control.as_ref().map(SessionSnapshot::observe);
                            if let Ok(mut snapshot) = actor_snapshot.write() {
                                *snapshot = next;
                            }
                        }
                        if let Some((response, result)) = pending {
                            let _ = response.send(result);
                        }
                    }
                    SessionCommand::Shutdown {
                        deactivate,
                        mut original_native_work,
                        response,
                    } => {
                        let result = async {
                            if original_native_work.is_none()
                                && control.as_ref().is_some_and(|current| {
                                    matches!(current, SessionController::Pi(_))
                                })
                            {
                                let cancellation = CancellationToken::new();
                                original_native_work = Some(
                                    acyclic_fs::NativeMaterializationWork::new(
                                        pi_root_provider::allocation_budget(),
                                        &cancellation,
                                    )
                                    .map_err(
                                        |(cause, work)| Error::NativeConstruction { cause, work },
                                    )?,
                                );
                            }
                            let current = control.as_ref().ok_or_else(|| {
                                Error::Invalid("Acyclic session actor has no workspace".to_owned())
                            })?;
                            let native = matches!(current, SessionController::Pi(_));
                            let mut terminal_state = if native {
                                None
                            } else {
                                Some(current.state().clone())
                            };
                            let mut directory = match (native, original_native_work.as_ref()) {
                                (true, Some(work)) => {
                                    let copied = work.path(current.directory(), None)?;
                                    (copied.0, Some(copied.1))
                                }
                                (true, None) => {
                                    return Err(Error::NativeBoundary {
                                        code: acyclic_harness::wire::ErrorCode::Storage,
                                        reason: "Pi close requires its original native query span",
                                    });
                                }
                                (false, _) => (current.directory().to_path_buf(), None),
                            };
                            let current = control.take().ok_or_else(|| {
                                Error::Invalid("Acyclic session actor has no workspace".to_owned())
                            })?;
                            match current
                                .close(deactivate, original_native_work.as_ref())
                                .await
                            {
                                Ok(mut slots) => {
                                    if deactivate
                                        && let Some(terminal_state) = terminal_state.as_mut()
                                    {
                                        terminal_state.active = false;
                                        slots
                                            .save(&directory.0, terminal_state, Survives::PowerLoss)
                                            .map_err(Error::Storage)?;
                                    }
                                    Ok(SessionCloseState::Closed)
                                }
                                Err(shutdown_error) => {
                                    let recovered = match original_native_work.as_ref() {
                                        Some(work) => {
                                            SessionController::open(directory.0, &resources.0, work)
                                                .await
                                        }
                                        None => resources
                                            .0
                                            .open_session_directory(directory.0)
                                            .await
                                            .map(Into::into)
                                            .map_err(Error::Storage),
                                    };
                                    if let Ok(recovered) = recovered {
                                        let recovered = match directory.1.take() {
                                            Some(custody) => {
                                                recovered.retain_original_input(custody)
                                            }
                                            None => Ok(recovered),
                                        };
                                        if let Ok(recovered) = recovered {
                                            control = Some(recovered);
                                        }
                                    }
                                    Err(shutdown_error)
                                }
                            }
                        }
                        .await;
                        let status = if result.is_ok() {
                            SessionCloseState::Closed
                        } else {
                            SessionCloseState::Failed
                        };
                        let _ = actor_close_result.send(Some(status));
                        if result.is_ok() {
                            actor_state.store(SESSION_CLOSED, std::sync::atomic::Ordering::Release);
                            if let Ok(mut snapshot) = actor_snapshot.write() {
                                *snapshot = None;
                            }
                            let _ = response.send((result, original_native_work));
                            break;
                        }
                        actor_state
                            .store(SESSION_CLOSE_FAILED, std::sync::atomic::Ordering::Release);
                        let next = control.as_ref().map(SessionSnapshot::observe);
                        if let Ok(mut snapshot) = actor_snapshot.write() {
                            *snapshot = next;
                        }
                        if let Err((Err(error), _)) = response.send((result, original_native_work))
                        {
                            if undelivered_close_failure.is_none() {
                                undelivered_close_failure = Some(error);
                            }
                        }
                    }
                    #[cfg(test)]
                    SessionCommand::Pause { entered, resume } => {
                        let _ = entered.send(());
                        let _ = resume.await;
                    }
                }
            }
            drop(control);
            // Capture the whole pair: resource bodies outlive controller
            // recovery and always drop before their original allocation.
            drop(resources);
        });
        Self {
            key,
            state,
            admission: Mutex::new(()),
            sender,
            data_slots,
            snapshot,
            close_result,
        }
    }

    pub(crate) fn with_snapshot<T>(
        &self,
        read: impl FnOnce(Option<&SessionSnapshot>) -> T,
    ) -> Result<T, String> {
        self.snapshot
            .read()
            .map(|snapshot| read(snapshot.as_ref()))
            .map_err(|_| "session routing snapshot lock is poisoned".to_owned())
    }

    pub(crate) fn is_active(&self) -> bool {
        self.state.load(std::sync::atomic::Ordering::Acquire) == SESSION_ACTIVE
    }

    pub(crate) async fn dispatch(
        &self,
        request: ControlRequest,
        original_native_work: &mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<ControlDispatchResponse> {
        let (response, result) = tokio::sync::oneshot::channel();
        {
            let _admission = self
                .admission
                .lock()
                .map_err(|_| Error::Storage("session admission lock is poisoned".to_owned()))?;
            if !self.is_active() {
                return Err(Error::Invalid("Acyclic session is closing".to_owned()));
            }
            let permit = Arc::clone(&self.data_slots)
                .try_acquire_owned()
                .map_err(|_| {
                    Error::Invalid("Acyclic session operation queue is full".to_owned())
                })?;
            self.sender
                .try_send(SessionCommand::Dispatch {
                    request,
                    original_native_work: original_native_work.clone(),
                    response,
                    _permit: permit,
                })
                .map_err(|error| {
                    Error::Invalid(match error {
                        tokio::sync::mpsc::error::TrySendError::Full(_) => {
                            "Acyclic session operation queue is full".to_owned()
                        }
                        tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                            "Acyclic session actor stopped".to_owned()
                        }
                    })
                })?;
        }
        let (result, returned_work) = result.await.map_err(|_| {
            Error::Storage("Acyclic session actor dropped an accepted operation".to_owned())
        })?;
        *original_native_work = returned_work;
        result
    }

    pub(crate) async fn shutdown(
        &self,
        deactivate: bool,
        original_native_work: &mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<SessionCloseState> {
        let (response, result) = tokio::sync::oneshot::channel();
        let mut existing = None;
        {
            let _admission = self
                .admission
                .lock()
                .map_err(|_| Error::Storage("session admission lock is poisoned".to_owned()))?;
            match self.state.load(std::sync::atomic::Ordering::Acquire) {
                SESSION_ACTIVE => self
                    .state
                    .store(SESSION_CLOSING, std::sync::atomic::Ordering::Release),
                SESSION_CLOSING => existing = Some(self.close_result.subscribe()),
                SESSION_CLOSE_FAILED => return Ok(SessionCloseState::Failed),
                SESSION_CLOSED => return Ok(SessionCloseState::Closed),
                _ => {
                    return Err(Error::Invalid(
                        "invalid Acyclic session lifecycle state".to_owned(),
                    ));
                }
            }
            if existing.is_none()
                && let Err(error) = self.sender.try_send(SessionCommand::Shutdown {
                    deactivate,
                    original_native_work: original_native_work.clone(),
                    response,
                })
            {
                let error = Error::Invalid(match error {
                    tokio::sync::mpsc::error::TrySendError::Full(_) => {
                        "Acyclic session control queue is unexpectedly full during shutdown"
                            .to_owned()
                    }
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                        "Acyclic session actor stopped before shutdown".to_owned()
                    }
                });
                self.state
                    .store(SESSION_CLOSE_FAILED, std::sync::atomic::Ordering::Release);
                let _ = self.close_result.send(Some(SessionCloseState::Failed));
                return Err(error);
            }
        }
        if let Some(mut existing) = existing {
            if existing.borrow().is_none() {
                existing
                    .changed()
                    .await
                    .map_err(|error| Error::Storage(display(error)))?;
            }
            return existing.borrow().as_ref().copied().ok_or_else(|| {
                Error::Storage("Acyclic session close completed without a status".to_owned())
            });
        }
        let (result, returned_work) = result
            .await
            .map_err(|_| Error::Storage("Acyclic session actor dropped shutdown".to_owned()))?;
        *original_native_work = returned_work;
        result
    }

    #[cfg(test)]
    #[allow(
        clippy::expect_used,
        reason = "this deterministic test gate must panic immediately if its actor fixture is broken"
    )]
    pub(crate) async fn pause(&self) -> tokio::sync::oneshot::Sender<()> {
        let (entered, accepted) = tokio::sync::oneshot::channel();
        let (resume, paused) = tokio::sync::oneshot::channel();
        self.sender
            .try_send(SessionCommand::Pause {
                entered,
                resume: paused,
            })
            .expect("session actor");
        accepted.await.expect("actor pause accepted");
        resume
    }
}
