//! The custodian's original admission socket, multiplexed by native readiness.

use std::io;
use std::net::TcpListener;
use std::task::Poll;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::oneshot;
use tokio::task::JoinSet;

pub(super) struct Request {
    pub pid: u32,
    pub tree: usize,
    pub secret: [u8; 16],
    pub reply: oneshot::Sender<bool>,
}

impl Request {
    /// A failed member lookup refuses only this connection. It cannot become
    /// a service failure or cause another tree's terminal state to be queried.
    pub fn complete(self, admitted: io::Result<bool>) -> Option<usize> {
        let accepted = admitted.unwrap_or(false);
        let _ = self.reply.send(accepted);
        accepted.then_some(self.tree)
    }
}

/// Each connection retains only its fixed request and original reply. A peer
/// that withholds bytes or stops reading cannot park another peer's producer.
/// Notification failure belongs to the original service, not that peer's I/O.
pub(super) async fn serve<F>(
    listener: TcpListener,
    notify: impl Fn(Request) -> F + Clone + Send + 'static,
) -> io::Result<()>
where
    F: std::future::Future<Output = io::Result<()>> + Send + 'static,
{
    listener.set_nonblocking(true)?;
    let listener = tokio::net::TcpListener::from_std(listener)?;
    let mut pending = JoinSet::new();
    loop {
        let accepted = std::future::poll_fn(|context| {
            if let Poll::Ready(Some(completed)) = pending.poll_join_next(context) {
                // Peer I/O stays local. The original dispatch producer's
                // failure and task panic terminate this same service owner.
                return Poll::Ready(match completed.map_err(io::Error::other)? {
                    Err(ConnectionFailure::Producer(error)) => Err(error),
                    Err(ConnectionFailure::Peer(error)) => {
                        drop(error);
                        Ok(None)
                    }
                    Ok(()) => Ok(None),
                });
            }
            listener
                .poll_accept(context)
                .map(|accepted| accepted.map(Some))
        })
        .await?;
        let Some((mut stream, _)) = accepted else {
            continue;
        };
        let notify = notify.clone();
        pending.spawn(async move {
            let mut bytes = [0; 28];
            stream.read_exact(&mut bytes).await?;
            let (pid, rest) = bytes.split_at(4);
            let (tree, secret) = rest.split_at(8);
            let pid = u32::from_le_bytes(pid.try_into().map_err(|_| io::ErrorKind::InvalidData)?);
            let tree = usize::try_from(u64::from_le_bytes(
                tree.try_into().map_err(|_| io::ErrorKind::InvalidData)?,
            ))
            .map_err(|_| io::ErrorKind::InvalidData)?;
            let secret = secret.try_into().map_err(|_| io::ErrorKind::InvalidData)?;
            let (reply, result) = oneshot::channel();
            notify(Request {
                pid,
                tree,
                secret,
                reply,
            })
            .await
            .map_err(ConnectionFailure::Producer)?;
            let admitted = result.await.unwrap_or(false);
            stream
                .write_all(&[u8::from(admitted)])
                .await
                .map_err(ConnectionFailure::Peer)
        });
    }
}

enum ConnectionFailure {
    Peer(io::Error),
    Producer(io::Error),
}

impl From<io::Error> for ConnectionFailure {
    fn from(error: io::Error) -> Self {
        Self::Peer(error)
    }
}

impl From<io::ErrorKind> for ConnectionFailure {
    fn from(kind: io::ErrorKind) -> Self {
        Self::Peer(kind.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future as _;
    use std::net::Ipv4Addr;

    #[test]
    fn launch_admission_progress_survives_partial_input_and_reply_producer_loss() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("native socket runtime");
        runtime.block_on(async {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("listener");
            let address = listener.local_addr().expect("address");
            let fault_file =
                tempfile::NamedTempFile::new().expect("native notification fault source");
            let mut write_only = std::fs::OpenOptions::new()
                .write(true)
                .open(fault_file.path())
                .expect("write-only native source");
            let original = std::io::Read::read(&mut write_only, &mut [0])
                .expect_err("real native read refusal");
            let expected_code = original.raw_os_error().expect("native error code");
            let expected_kind = original.kind();
            let original = std::sync::Arc::new(std::sync::Mutex::new(Some(original)));
            let (send, mut receive) = tokio::sync::mpsc::channel(1);
            let server = serve(listener, move |request| {
                let send = send.clone();
                let original = std::sync::Arc::clone(&original);
                async move {
                    send.send(request).await.map_err(|_| {
                        original
                            .lock()
                            .expect("original notification fault")
                            .take()
                            .expect("one original failed notification")
                    })
                }
            });
            let client = async move {
                let mut partial = tokio::net::TcpStream::connect(address)
                    .await
                    .expect("partial peer");
                partial.write_all(&[1]).await.expect("one request byte");
                let bytes = [
                    7_u32.to_le_bytes().as_slice(),
                    9_u64.to_le_bytes().as_slice(),
                    &[13; 16],
                ]
                .concat();
                let mut peer = tokio::net::TcpStream::connect(address)
                    .await
                    .expect("complete peer");
                peer.write_all(&bytes).await.expect("complete request");
                let request = receive.recv().await.expect("same admission producer");
                assert_eq!(
                    (request.pid, request.tree, request.secret),
                    (7, 9, [13; 16])
                );
                assert_eq!(request.complete(Ok(true)), Some(9));
                let mut reply = [0];
                peer.read_exact(&mut reply).await.expect("acknowledgement");
                assert_eq!(reply, [1]);
                // Losing the original reply producer returns a refusal; it never
                // holds later admissions behind an unrelated blocking receive.
                let mut lost = tokio::net::TcpStream::connect(address)
                    .await
                    .expect("lost peer");
                lost.write_all(&bytes)
                    .await
                    .expect("request before producer loss");
                drop(receive.recv().await.expect("original request"));
                lost.read_exact(&mut reply)
                    .await
                    .expect("producer-loss refusal");
                assert_eq!(reply, [0]);
                // An actual refused native read remains local to its connection;
                // another tree can still receive its original acknowledgement.
                let file = tempfile::NamedTempFile::new().expect("native file");
                let mut write_only = std::fs::OpenOptions::new()
                    .write(true)
                    .open(file.path())
                    .expect("write-only handle");
                let failed_read = std::io::Read::read(&mut write_only, &mut [0]);
                let native_error = failed_read.expect_err("a write-only handle cannot be read");
                let mut refused = tokio::net::TcpStream::connect(address)
                    .await
                    .expect("refused peer");
                refused.write_all(&bytes).await.expect("refused request");
                assert_eq!(
                    receive
                        .recv()
                        .await
                        .expect("refused request producer")
                        .complete(Err(native_error)),
                    None
                );
                refused
                    .read_exact(&mut reply)
                    .await
                    .expect("native refusal");
                assert_eq!(reply, [0]);
                let mut other_bytes = bytes.clone();
                other_bytes[4..12].copy_from_slice(&10_u64.to_le_bytes());
                let mut other = tokio::net::TcpStream::connect(address)
                    .await
                    .expect("unrelated tree peer");
                other
                    .write_all(&other_bytes)
                    .await
                    .expect("unrelated request");
                assert_eq!(
                    receive
                        .recv()
                        .await
                        .expect("unrelated request producer")
                        .complete(Ok(true)),
                    Some(10)
                );
                other
                    .read_exact(&mut reply)
                    .await
                    .expect("unrelated acknowledgement");
                assert_eq!(reply, [1]);
                // Releasing the incomplete peer loses no authenticated request.
                drop(partial);
                drop(receive);
                let mut failed = tokio::net::TcpStream::connect(address)
                    .await
                    .expect("lost dispatch receiver peer");
                failed
                    .write_all(&bytes)
                    .await
                    .expect("request after receiver loss");
                assert_eq!(
                    failed
                        .read(&mut reply)
                        .await
                        .expect("failed notification closes its original peer"),
                    0
                );
            };
            let mut server = std::pin::pin!(server);
            let mut client = std::pin::pin!(client);
            let result = tokio::select! {
                result = server.as_mut() => { client.as_mut().await; result },
                () = client.as_mut() => {
                    // On this current-thread runtime, peer EOF follows the
                    // notifying task's terminal. Poll the SAME service once:
                    // dropping its cause must fail immediately, not hang.
                    std::future::poll_fn(|context| Poll::Ready(
                        match server.as_mut().poll(context) {
                            Poll::Ready(result) => result,
                            Poll::Pending => panic!("original notification failure was lost"),
                        }
                    )).await
                },
            };
            let failure = result.expect_err("original notification failure terminates service");
            assert_eq!(failure.raw_os_error(), Some(expected_code));
            assert_eq!(failure.kind(), expected_kind);
        });
    }
}
