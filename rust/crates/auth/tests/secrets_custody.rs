//! Original source invalidation cancels real HTTP headers and body waits.
#![cfg(feature = "client")]
use acyclic_auth::{
    contract::SecretListRequest,
    credential::{
        CredentialAction, CredentialCache, CredentialCacheError, CredentialError,
        CredentialProvider, CredentialRequest, CredentialResolution, CredentialSnapshot,
        CredentialSource, CredentialValidity, CredentialWatch, ResolvedCredential,
    },
    secrets::{Client, Error, HttpTransport},
};
use futures::channel::oneshot;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Resolve,
    Check(usize),
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
        let changed = self.changed.lock().unwrap().take().unwrap();
        Ok(Some(CredentialWatch::from_custodian(
            CredentialValidity {
                now_unix_seconds: 1,
                revision: None,
            },
            async move { changed.await.map_err(|_| CredentialCacheError::Unavailable) },
        )))
        .into()
    }
}

async fn source_loss_ends_owner_wait(phase: Phase) -> TestResult {
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
    );
    let request = SecretListRequest {
        scope: "fixture".into(),
        cursor: None,
        page_size: 0,
    };
    let response = client.list(&request);
    tokio::pin!(response);
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
    drop(source);
    assert_eq!(response.await, Err(Error::Credential));
    assert!(
        resume.send(()).is_err(),
        "cancelled owner action must release its receiver"
    );
    Ok(())
}

async fn source_change_ends_response_wait(body: bool) -> TestResult {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let cache = CredentialCache::new(
        |_| async { CredentialSnapshot::new("harmless-fixture".into(), [1; 32].into(), 2) },
        || CredentialValidity {
            now_unix_seconds: 1,
            revision: Some([1; 32].into()),
        },
    );
    let client = Client::new(HttpTransport::new(&origin)?, cache.clone().into());
    let request = SecretListRequest {
        scope: "fixture".into(),
        cursor: None,
        page_size: 0,
    };
    let response = client.list(&request);
    tokio::pin!(response);
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
    cache.invalidate()?;
    assert_eq!(response.await, Err(Error::Credential));
    drop(socket);
    Ok(())
}

#[tokio::test]
async fn source_change_cancels_original_http_response_waits() -> TestResult {
    source_change_ends_response_wait(false).await?;
    source_change_ends_response_wait(true).await?;
    for phase in [
        Phase::Resolve,
        Phase::Check(1),
        Phase::Check(2),
        Phase::Check(3),
    ] {
        source_loss_ends_owner_wait(phase).await?;
    }
    Ok(())
}
