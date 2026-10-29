//! Real loopback gateway controls with event-held response bodies.
use super::*;
use acyclic_auth::operation::{Context, Stopped};
use futures::{FutureExt, channel::oneshot, future::pending};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test(start_paused = true)]
async fn original_context_owns_http_body_progress_and_clones() -> TestResult {
    for completion in ["deadline", "cancel", "producer-loss", "unchanged"] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}/", listener.local_addr()?);
        let (entered, started) = oneshot::channel();
        let (release, released) = oneshot::channel();
        // The real production JSON encoder supplies the service response.
        let body = json::encode("UsageResponse", &wire::UsageResponse::default())?;
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await?;
            let mut socket = BufReader::new(socket);
            loop {
                let mut line = String::new();
                if socket.read_line(&mut line).await? == 0 {
                    return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
                }
                if line == "\r\n" {
                    break;
                }
            }
            socket.get_mut().write_all(format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()
            ).as_bytes()).await?;
            entered.send(()).unwrap();
            let _ = released.await;
            // Cancellation may already have closed the receiving socket.
            let _ = socket.get_mut().write_all(&body).await;
            Ok::<_, std::io::Error>(())
        });
        let (expire, expired) = oneshot::channel();
        let (cancel, cancelled) = oneshot::channel();
        let objects =
            HttpObjects::new(&endpoint, "fixture", 4096, None)?.with_context(Context::new(
                u64::MAX,
                async {
                    let _ = expired.await;
                },
                async {
                    let _ = cancelled.await;
                },
            ));
        let mut request = Box::pin(objects.usage());
        futures::select! {
            result = request.as_mut().fuse() => panic!("response body still held: {result:?}"),
            _ = started.fuse() => {},
        }
        assert!(request.as_mut().now_or_never().is_none());
        let expected = match completion {
            "deadline" => {
                expire.send(()).unwrap();
                Some(Stopped::Deadline)
            }
            "cancel" => {
                cancel.send(()).unwrap();
                Some(Stopped::Cancelled)
            }
            "producer-loss" => {
                drop(cancel);
                Some(Stopped::Cancelled)
            }
            _ => None,
        };
        if let Some(expected) = expected {
            assert_eq!(
                request
                    .now_or_never()
                    .expect("the original stop must settle body reading")
                    .unwrap_err()
                    .stopped,
                Some(expected)
            );
            assert_eq!(
                objects.clone().usage().await.unwrap_err().stopped,
                Some(expected)
            );
            release.send(()).unwrap();
        } else {
            // A caller-owned budget cannot inherit the old 30-second timer.
            tokio::time::advance(std::time::Duration::from_secs(31)).await;
            assert!(request.as_mut().now_or_never().is_none());
            release.send(()).unwrap();
            request.await?;
        }
        server.await??;
    }
    Ok(())
}

#[tokio::test]
async fn a_credential_callback_cannot_cancel_then_dispatch_http() -> TestResult {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/", listener.local_addr()?);
    let (cancel, cancelled) = oneshot::channel();
    let cancel = std::sync::Mutex::new(Some(cancel));
    let provider = CredentialProvider::new(move || {
        cancel.lock().unwrap().take().unwrap().send(()).unwrap();
        std::future::ready(Ok("fixture".into()))
    });
    let objects = HttpObjects::with_credential_provider(&endpoint, provider, 4096, None)?
        .with_context(Context::new(u64::MAX, pending(), async {
            let _ = cancelled.await;
        }));
    assert_eq!(
        objects.usage().await.unwrap_err().stopped,
        Some(Stopped::Cancelled)
    );
    assert!(
        matches!(listener.into_std()?.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
    Ok(())
}
