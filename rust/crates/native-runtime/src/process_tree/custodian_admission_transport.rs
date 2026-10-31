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

/// Each connection retains only its fixed request and original reply. A peer
/// that withholds bytes or stops reading cannot park another peer's producer.
pub(super) async fn serve(
    listener: TcpListener,
    notify: impl Fn(Request) -> io::Result<()> + Clone + Send + 'static,
) -> io::Result<()> {
    listener.set_nonblocking(true)?;
    let listener = tokio::net::TcpListener::from_std(listener)?;
    let mut pending = JoinSet::new();
    loop {
        let accepted = std::future::poll_fn(|context| {
            if let Poll::Ready(Some(completed)) = pending.poll_join_next(context) {
                // Retire completed connections without retaining history. An
                // individual IO failure closes its peer; a task panic fails
                // this service and drops all still-owned connection tasks.
                return Poll::Ready(completed.map(|_peer_result| None).map_err(io::Error::other));
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
            })?;
            let admitted = result.await.unwrap_or(false);
            stream.write_all(&[u8::from(admitted)]).await
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
            let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
            let server = tokio::spawn(serve(listener, move |request| {
                send.send(request)
                    .map_err(|_| io::ErrorKind::BrokenPipe.into())
            }));
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
            request.reply.send(true).expect("original reply consumer");
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
            // Releasing the incomplete peer loses no authenticated request.
            drop(partial);
            server.abort();
            assert!(
                server
                    .await
                    .expect_err("retired socket service")
                    .is_cancelled()
            );
        });
    }
}
