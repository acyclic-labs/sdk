//! Single event-loop owner; no shared mutable client or lock crosses provider I/O.
use super::{
    file_client::{admission_error, error},
    query::WasmTaskVolume,
};
use crate::client::mailbox;
use crate::{
    client::{FileClient, FileClientError},
    task_volume::{Cursor, FileKey, Query},
};
use acyclic_stream::{
    BoxProviderFuture, BoxProviderStream, StreamError, StreamProvider,
    session::runtime::{Executor, PlatformExecutor},
    wire,
};
use futures::{FutureExt as _, StreamExt as _};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};
use wasm_bindgen::prelude::*;

type Inner = FileClient<dyn StreamProvider, dyn StreamProvider>;
enum Wake {
    Command(Option<Command>),
    Connected(Result<u64, acyclic_stream::StreamError>),
    Published(Option<Result<(), StreamError>>),
    Retry,
}
pub(super) enum Command {
    Enqueue {
        key: FileKey,
        bytes: js_sys::Uint8Array,
        reply: mailbox::Sender<Result<(), JsValue>>,
    },
    Flush(mailbox::Sender<Result<usize, JsValue>>),
    Idle(mailbox::Sender<Result<(), JsValue>>),
}
pub(super) struct State {
    pub(super) pending: Cell<usize>,
    pub(super) cursor: RefCell<Cursor>,
    pub(super) error: RefCell<JsValue>,
}
impl State {
    fn refresh(&self, inner: &Inner) {
        self.pending.set(inner.pending());
        *self.cursor.borrow_mut() = inner.cursor().clone();
    }
}
fn retry_after(
    error: Option<&FileClientError>,
    remote: bool,
    failures: &mut u32,
) -> Option<BoxProviderFuture<'static, ()>> {
    if !remote {
        return None;
    }
    let delay = crate::client::submission_retry(error, failures)?;
    Some(PlatformExecutor.sleep(delay))
}

pub(super) async fn start(
    mut inner: Inner,
    target: &WasmTaskVolume,
) -> Result<(mailbox::Sender<Command>, Rc<State>), JsValue> {
    let connections = target.connections.clone();
    // A target file follow owns the session while offline intent is retained.
    // It consumes credit without needing a renderer to be attached.
    let mut changes: BoxProviderStream<'static, Result<(), StreamError>> =
        if let Some(session) = &target.session {
            let cursor = target.volume.cut(&Query::Tasks).await.map_err(error)?;
            let frames = session
                .subscribe(wire::SessionSubscription {
                    channel: None,
                    resume: Some(wire::SessionCursor {
                        next: cursor.position,
                        opaque: Default::default(),
                    }),
                    kind: Some(wire::session_subscription::Kind::Follow(
                        wire::SessionFollow {
                            path: target.volume.path().to_string(),
                            delivery: wire::SessionDelivery::All as i32,
                            preceding_commit_id: if cursor.position == 0 {
                                Default::default()
                            } else {
                                cursor.commit.to_vec().into()
                            },
                        },
                    )),
                })
                .await
                .map_err(error)?;
            // Records AND standalone watermarks wake the durable outbox. The shared
            // pool retains cursor validation, fencing and receive credit.
            Box::pin(frames.map(|frame| frame.map(|_| ())))
        } else {
            Box::pin(futures::stream::pending())
        };
    let state = Rc::new(State {
        pending: Cell::new(inner.pending()),
        cursor: RefCell::new(inner.cursor().clone()),
        error: RefCell::new(JsValue::UNDEFINED),
    });
    let (commands, mut receiver) = mailbox::channel();
    let current = state.clone();
    wasm_bindgen_futures::spawn_local(async move {
        let mut generation = 0;
        let mut failures = 0_u32;
        let mut retry: Option<BoxProviderFuture<'static, ()>> = None;
        let mut idle: Option<mailbox::Sender<Result<(), JsValue>>> = None;
        loop {
            let event = {
                let observed = generation;
                let recovered = async {
                    match &connections {
                        Some(connections) => connections.changed(observed).await,
                        None => futures::future::pending().await,
                    }
                }
                .fuse();
                let retry_ready = async {
                    match &mut retry {
                        Some(deadline) => deadline.await,
                        None => futures::future::pending().await,
                    }
                }
                .fuse();
                futures::pin_mut!(recovered, retry_ready);
                futures::select! {
                    command = receiver.recv().fuse() => Wake::Command(command),
                    connection = recovered => Wake::Connected(connection),
                    change = changes.next().fuse() => Wake::Published(change),
                    () = retry_ready => Wake::Retry,
                }
            };
            let automatic = match event {
                Wake::Command(command) => {
                    let Some(command) = command else {
                        return;
                    };
                    match command {
                        Command::Enqueue { key, bytes, reply } => {
                            let result = inner
                                .enqueue_with(key, u64::from(bytes.length()), || {
                                    let bytes = bytes.to_vec();
                                    crate::client::validate_offline_request(&bytes)?;
                                    Ok(bytes.into())
                                })
                                .await;
                            if let Some(deadline) = retry_after(
                                result.as_ref().err(),
                                connections.is_some(),
                                &mut failures,
                            ) {
                                retry = Some(deadline);
                            }
                            current.refresh(&inner);
                            let admitted = result.is_ok();
                            let _ = reply.reply(result.map_err(admission_error));
                            admitted && connections.is_some()
                        }
                        Command::Flush(reply) => {
                            let result = inner.flush().await;
                            if let Some(deadline) = retry_after(
                                result.as_ref().err(),
                                connections.is_some(),
                                &mut failures,
                            ) {
                                retry = Some(deadline);
                            }
                            current.refresh(&inner);
                            let _ = reply.reply(result.map_err(admission_error));
                            false
                        }
                        Command::Idle(reply) => {
                            if inner.pending() == 0 {
                                let _ = reply.reply(Ok(()));
                            } else if idle.as_ref().is_some_and(|reply| !reply.is_closed()) {
                                let _ =
                                    reply.reply(Err(error("an idle wait is already registered")));
                            } else {
                                idle = Some(reply);
                            }
                            false
                        }
                    }
                }
                Wake::Connected(connection) => match connection {
                    Ok(next) => {
                        generation = next;
                        true
                    }
                    Err(failure) => {
                        *current.error.borrow_mut() = error(failure);
                        false
                    }
                },
                Wake::Retry => true,
                Wake::Published(change) => {
                    let ended = change.is_none();
                    if let Some(Err(failure)) = change {
                        *current.error.borrow_mut() = error(failure);
                    }
                    if ended {
                        changes = Box::pin(futures::stream::pending());
                    }
                    !ended && inner.pending() > 0
                }
            };
            if automatic {
                // Rejections retire in Rust, then later requests proceed.
                // Indeterminate HTTP publication retries the same immutable identity
                // with bounded error backoff. Durability waits remain push-driven.
                retry = None;
                loop {
                    match inner.flush().await {
                        Ok(_) => {
                            failures = 0;
                            break;
                        }
                        Err(failure @ FileClientError::Rejected { .. }) => {
                            *current.error.borrow_mut() = admission_error(failure);
                        }
                        Err(failure) => {
                            retry = retry_after(Some(&failure), true, &mut failures);
                            *current.error.borrow_mut() = admission_error(failure);
                            break;
                        }
                    }
                }
                current.refresh(&inner);
            }
            if inner.pending() == 0
                && let Some(idle) = idle.take()
            {
                let _ = idle.reply(Ok(()));
            }
        }
    });
    Ok((commands, state))
}
