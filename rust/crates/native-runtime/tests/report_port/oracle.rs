//! Replay the public report decoder over its original native stream transport.
use acyclic_native_runtime::obs::ReportPort;
use std::io;
use tokio::io::{AsyncBufReadExt as _, AsyncRead, AsyncWrite, AsyncWriteExt as _, BufReader};

const GENERATION: &str = "0123456789abcdef0123456789abcdef";
type Decoded = Option<(String, String, u64)>;
type Observation = Result<Decoded, (io::ErrorKind, String)>;

async fn channels() -> (
    impl AsyncRead + AsyncWrite + Unpin + Send,
    impl AsyncRead + AsyncWrite + Unpin + Send,
) {
    #[cfg(unix)]
    {
        let (owner, child) = std::os::unix::net::UnixStream::pair().unwrap();
        owner.set_nonblocking(true).unwrap();
        child.set_nonblocking(true).unwrap();
        (
            tokio::net::UnixStream::from_std(owner).unwrap(),
            tokio::net::UnixStream::from_std(child).unwrap(),
        )
    }
    #[cfg(windows)]
    {
        use std::sync::atomic::{AtomicU64, Ordering};
        use tokio::net::windows::named_pipe::{ClientOptions, ServerOptions};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let name = format!(
            r"\\.\pipe\acyclic-report-codec-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let child = ServerOptions::new()
            .first_pipe_instance(true)
            .create(&name)
            .unwrap();
        let owner = ClientOptions::new().open(&name).unwrap();
        child.connect().await.unwrap();
        (owner, child)
    }
}

async fn observe(input: &[u8], chunk: usize, ready: bool) -> Observation {
    let (mut owner, child) = channels().await;
    let mut port = ReportPort::new(GENERATION.into(), child).unwrap();
    let write = async move {
        if ready {
            let mut line = String::new();
            BufReader::new(&mut owner)
                .read_line(&mut line)
                .await
                .unwrap();
            let announcement: serde_json::Value = serde_json::from_str(&line).unwrap();
            assert_eq!(announcement["ready"], true);
            assert_eq!(announcement["generation"], GENERATION);
        }
        for bytes in input.chunks(chunk) {
            owner.write_all(bytes).await.unwrap();
        }
        drop(owner);
    };
    let read = async {
        if ready {
            port.ready().await.unwrap();
            assert_eq!(
                port.ready().await.unwrap_err().kind(),
                io::ErrorKind::AlreadyExists,
                "report port repeated its original readiness announcement"
            );
        }
        let observed = port
            .request()
            .await
            .map(|request| {
                request.map(|request| {
                    (
                        request.request_id,
                        request.generation,
                        request.deadline_unix_ms,
                    )
                })
            })
            .map_err(|error| (error.kind(), error.to_string()));
        assert!(
            matches!(port.request().await, Err(error) if error.kind() == io::ErrorKind::AlreadyExists),
            "report decoder readmitted its consumed request"
        );
        observed
    };
    tokio::pin!(write, read);
    tokio::select! {
        // A decoded line consumes the port even when input remains unread.
        // Retire that input producer with the original request observer.
        observed = &mut read => observed,
        () = &mut write => read.await,
    }
}

/// The original writer closes its endpoint, including after a partial frame.
/// Every read has this same-path EOF producer, without a timer. Native streams
/// may coalesce writes: this varies write boundaries, without assuming reads.
pub fn check(input: &[u8]) {
    if input.len() > 4096 {
        return;
    }
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    let runtime = RUNTIME.get_or_init(|| acyclic_native_runtime::coordination_runtime().unwrap());
    runtime.block_on(async {
        let whole = observe(input, input.len().max(1), false).await;
        assert_eq!(
            whole,
            observe(input, 17, true).await,
            "ready report port changed its original request outcome"
        );
        if let Ok(Some((request, generation, _))) = &whole {
            for identity in [request, generation] {
                assert_eq!(
                    identity,
                    &format!("{:032x}", u128::from_str_radix(identity, 16).unwrap()),
                    "report decoder admitted a noncanonical identity"
                );
            }
        }
        if input == include_bytes!("corpus/request") {
            assert!(
                matches!(&whole, Ok(Some((request, generation, deadline)))
                    if request == "fedcba9876543210fedcba9876543210"
                        && generation == GENERATION && *deadline == u64::MAX),
                "original native report writer was rejected: {whole:?}"
            );
            // Derive noncanonical spellings from the captured writer, without
            // inventing another external frame or copying the decoder.
            for identity in ["fedcba9876543210fedcba9876543210", GENERATION] {
                let start = input
                    .windows(identity.len())
                    .position(|bytes| bytes == identity.as_bytes())
                    .unwrap();
                let letter = identity
                    .bytes()
                    .position(|byte| byte.is_ascii_lowercase())
                    .unwrap();
                let mut noncanonical = input.to_vec();
                noncanonical[start + letter].make_ascii_uppercase();
                assert!(
                    observe(&noncanonical, 1, false).await.is_err(),
                    "report decoder admitted a noncanonical identity"
                );
            }
        }
        for chunk in [1, 17] {
            assert_eq!(
                whole,
                observe(input, chunk, false).await,
                "report decoder changed its original result across native write boundaries"
            );
        }
    });
}
