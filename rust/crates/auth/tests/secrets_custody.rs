//! Original source invalidation cancels real HTTP headers and body waits.
#![cfg(feature = "client")]
use acyclic_auth::operation::Stopped;
use acyclic_auth::{
    contract::SecretListRequest,
    credential::{
        CredentialAction, CredentialCache, CredentialCacheError, CredentialError,
        CredentialProvider, CredentialRequest, CredentialResolution, CredentialSnapshot,
        CredentialSource, CredentialValidity, CredentialWatch, ResolvedCredential,
    },
    secrets::{Client, Error, HttpTransport},
};
use futures::FutureExt as _;
use futures::channel::oneshot;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Watch,
    Resolve,
    Check(usize),
}

#[derive(Clone, Copy)]
enum End {
    Source,
    Caller(Option<Stopped>),
    // Replacing the invocation leaves this real operation pending. The same
    // one-poll terminal oracle detects that regression without hanging.
    Detached,
}

fn expected(end: End) -> Error {
    match end {
        End::Source => Error::Credential,
        End::Caller(reason) => Error::Stopped(reason.unwrap_or(Stopped::Cancelled)),
        End::Detached => Error::Stopped(Stopped::Cancelled),
    }
}

struct PausedOwner {
    phase: Phase,
    checks: AtomicUsize,
    entered: Mutex<Option<oneshot::Sender<()>>>,
    resume: Mutex<Option<oneshot::Receiver<()>>>,
}

impl PausedOwner {
    fn at(&self, phase: Phase) -> CredentialAction<()> {
        if self.phase != phase {
            return Ok(()).into();
        }
        self.entered
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send(())
            .unwrap();
        let resume = self.resume.lock().unwrap().take().unwrap();
        CredentialAction::Pending(Box::pin(async move {
            resume.await.map_err(|_| CredentialError::Invalid)
        }))
    }
}

struct PausedRequest(Arc<PausedOwner>);

impl CredentialRequest for PausedRequest {
    fn token(&self) -> &str {
        "harmless-fixture"
    }
    fn binding(&self) -> Option<acyclic_auth::credential::CredentialBinding> {
        None
    }
    fn check(&self) -> CredentialAction<()> {
        self.0.at(Phase::Check(
            self.0.checks.fetch_add(1, Ordering::Relaxed) + 1,
        ))
    }
    fn reject(&self) -> CredentialAction<bool> {
        Ok(false).into()
    }
}

struct PausedSource {
    owner: Arc<PausedOwner>,
    changed: Mutex<Option<oneshot::Receiver<()>>>,
}

impl CredentialSource for PausedSource {
    fn resolve(&self) -> CredentialResolution<'_> {
        Box::pin(async move {
            self.owner.at(Phase::Resolve).await?;
            ResolvedCredential::from_custodian(PausedRequest(self.owner.clone())).await
        })
    }
    fn watch(&self) -> CredentialAction<Option<CredentialWatch>> {
        let paused = self.owner.at(Phase::Watch);
        let changed = self.changed.lock().unwrap().take().unwrap();
        CredentialAction::Pending(Box::pin(async move {
            let changed =
                Box::pin(
                    async move { changed.await.map_err(|_| CredentialCacheError::Unavailable) },
                );
            match futures::future::select(Box::pin(paused), changed).await {
                futures::future::Either::Left((result, changed)) => {
                    result?;
                    Ok(Some(CredentialWatch::from_custodian(
                        CredentialValidity {
                            now_unix_seconds: 1,
                            revision: None,
                        },
                        changed,
                    )))
                }
                futures::future::Either::Right(_) => Err(CredentialError::Invalid),
            }
        }))
    }
}

async fn source_loss_ends_owner_wait(phase: Phase, end: End) -> TestResult {
    let (context, stop) = crate::operation_context::invocation();
    let (detached, _detached_stop) = crate::operation_context::invocation();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let (entered, paused) = oneshot::channel();
    let (resume, waiting) = oneshot::channel();
    let (source, changed) = oneshot::channel();
    let client = Client::new(
        HttpTransport::new(&origin)?,
        CredentialProvider::from_custodian(PausedSource {
            owner: Arc::new(PausedOwner {
                phase,
                checks: AtomicUsize::new(0),
                entered: Mutex::new(Some(entered)),
                resume: Mutex::new(Some(waiting)),
            }),
            changed: Mutex::new(Some(changed)),
        }),
        Some(if matches!(end, End::Detached) {
            detached
        } else {
            context
        }),
    );
    let request = SecretListRequest {
        scope: "fixture".into(),
        cursor: None,
        page_size: 0,
    };
    let mut response = Box::pin(client.list(&request));
    let peer = async {
        let (mut socket, _) = listener.accept().await?;
        let mut request = Vec::new();
        let mut bytes = [0; 512];
        while !request.windows(4).any(|part| part == b"\r\n\r\n") {
            let count = socket.read(&mut bytes).await?;
            if count == 0 {
                return Err(std::io::Error::other("fixture request closed"));
            }
            request.extend_from_slice(&bytes[..count]);
        }
        let body = br#"{"generation":1,"variables":[],"nextCursor":null}"#;
        socket
            .write_all(
                format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).as_bytes(),
            )
            .await?;
        socket.write_all(body).await?;
        Ok::<_, std::io::Error>(socket)
    };
    tokio::pin!(paused);
    let socket = tokio::select! {
        paused = &mut paused => { paused?; None },
        peer = peer => Some(peer?),
        _ = &mut response => return Err("request completed before its original owner paused".into()),
    };
    if socket.is_some() {
        tokio::select! {
            paused = &mut paused => paused?,
            _ = &mut response => return Err("response admitted before its original owner paused".into()),
        }
    }
    // No change is emitted: dropping the original producer must itself wake
    // the call, while its resolution/check producer remains deliberately held.
    match end {
        End::Source => drop(source),
        End::Caller(Some(reason)) => stop.send(reason).unwrap(),
        End::Caller(None) => drop(stop),
        End::Detached => stop.send(Stopped::Cancelled).unwrap(),
    }
    let result = response.as_mut().now_or_never();
    if matches!(end, End::Detached) {
        assert!(
            result.is_none(),
            "replacement context escaped the negative control"
        );
    } else {
        assert_eq!(result, Some(Err(expected(end))));
    }
    drop(response);
    assert!(
        resume.send(()).is_err(),
        "cancelled owner action must release its receiver"
    );
    Ok(())
}

async fn source_change_ends_response_wait(body: bool, end: End) -> TestResult {
    let (context, stop) = crate::operation_context::invocation();
    let (detached, _detached_stop) = crate::operation_context::invocation();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let cache = CredentialCache::new(
        |_| async { CredentialSnapshot::new("harmless-fixture".into(), [1; 32].into(), 2) },
        || CredentialValidity {
            now_unix_seconds: 1,
            revision: Some([1; 32].into()),
        },
    );
    let client = Client::new(
        HttpTransport::new(&origin)?,
        cache.clone().into(),
        Some(if matches!(end, End::Detached) {
            detached
        } else {
            context
        }),
    );
    let request = SecretListRequest {
        scope: "fixture".into(),
        cursor: None,
        page_size: 0,
    };
    let mut response = Box::pin(client.list(&request));
    let peer = async {
        let (mut socket, _) = listener.accept().await?;
        let mut request = Vec::new();
        let mut bytes = [0; 512];
        while !request.windows(4).any(|part| part == b"\r\n\r\n") {
            let count = socket.read(&mut bytes).await?;
            if count == 0 {
                return Err(std::io::Error::other("fixture request closed"));
            }
            request.extend_from_slice(&bytes[..count]);
        }
        if body {
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4096\r\n\r\n")
                .await?;
            socket.write_all(b"{").await?;
        }
        Ok::<_, std::io::Error>(socket)
    };
    let socket = tokio::select! {
        peer = peer => peer?,
        _ = &mut response => return Err("request completed before source change".into()),
    };
    match end {
        End::Source => cache.invalidate()?,
        End::Caller(Some(reason)) => stop.send(reason).unwrap(),
        End::Caller(None) => drop(stop),
        End::Detached => stop.send(Stopped::Cancelled).unwrap(),
    }
    let result = response.as_mut().now_or_never();
    if matches!(end, End::Detached) {
        assert!(
            result.is_none(),
            "replacement context escaped the negative control"
        );
    } else {
        assert_eq!(result, Some(Err(expected(end))));
    }
    drop(socket);
    Ok(())
}

#[tokio::test]
async fn source_change_cancels_original_http_response_waits() -> TestResult {
    for end in [
        End::Source,
        End::Caller(Some(Stopped::Cancelled)),
        End::Caller(Some(Stopped::Deadline)),
        End::Caller(None),
        End::Detached,
    ] {
        source_change_ends_response_wait(false, end).await?;
        source_change_ends_response_wait(true, end).await?;
        for phase in [
            Phase::Watch,
            Phase::Resolve,
            Phase::Check(1),
            Phase::Check(2),
            Phase::Check(3),
        ] {
            source_loss_ends_owner_wait(phase, end).await?;
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    for missing in [true, false] {
        let (context, stop) = crate::operation_context::invocation();
        stop.send(Stopped::Cancelled).unwrap();
        let counter = calls.clone();
        let client = Client::new(
            HttpTransport::new("http://127.0.0.1:1")?,
            CredentialProvider::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                async { Ok("harmless-fixture".into()) }
            }),
            if missing { None } else { Some(context) },
        );
        assert_eq!(
            client
                .list(&SecretListRequest {
                    scope: "fixture".into(),
                    cursor: None,
                    page_size: 0
                })
                .await,
            Err(if missing {
                Error::MissingOperationContext
            } else {
                Error::Stopped(Stopped::Cancelled)
            })
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    Ok(())
}
