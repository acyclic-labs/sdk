//! Filesystem mailbox control exchange.

use super::*;

#[cfg(target_os = "linux")]
pub(crate) async fn send_linux_mailbox_request(
    data: &Path,
    encoded: &[u8],
    request_id: &control_protocol::RequestId,
    maximum_wait: std::time::Duration,
) -> Result<Value, ControlRequestError> {
    use std::time::{SystemTime, UNIX_EPOCH};

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let mailbox = linux_control_mailbox_path(data);
    let nonce = format!(
        "{}:{}:{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let name = short_hash(nonce.as_bytes());
    let unpublished = format!("{name}{LINUX_EXCHANGE_UNPUBLISHED}");
    let mailbox_directory = Arc::new(open_linux_directory(rustix::fs::CWD, &mailbox).map_err(
        |error| {
            ControlRequestError::Unavailable(format!("Acyclic service is not running: {error}"))
        },
    )?);
    let metadata = rustix::fs::fstat(&*mailbox_directory)
        .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
    // Only this user's own service may answer: the mailbox must be private
    // to this user and owned by them.
    if metadata.st_mode & 0o777 != 0o700 || metadata.st_uid != rustix::process::getuid().as_raw() {
        return Err(ControlRequestError::Unavailable(
            "Acyclic service mailbox is not this user's private directory".to_owned(),
        ));
    }
    rustix::fs::mkdirat(&*mailbox_directory, &unpublished, rustix::fs::Mode::RWXU).map_err(
        |error| {
            ControlRequestError::Unavailable(format!(
                "cannot create Acyclic control exchange: {error}"
            ))
        },
    )?;
    let mut exchange = LinuxMailboxExchange {
        mailbox: Arc::clone(&mailbox_directory),
        exchange: None,
        name: unpublished.clone().into(),
    };
    let result = tokio::time::timeout(maximum_wait, async {
        let directory = open_linux_directory(&*mailbox_directory, &unpublished)
            .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
        let directory = &*exchange.exchange.insert(directory);
        rustix::fs::mkfifoat(
            directory,
            LINUX_EXCHANGE_RESPONSE,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
        // Holding the write end as well means the response ends at its
        // newline, never at an end of file before the service opens it.
        let mut response_pipe = rustix::fs::openat(
            directory,
            LINUX_EXCHANGE_RESPONSE,
            rustix::fs::OFlags::RDWR
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(errno_to_io)
        .and_then(tokio::net::unix::pipe::Receiver::from_owned_fd)
        .map(|pipe| BufReader::new(pipe).take((MAXIMUM_CONTROL_MESSAGE_BYTES + 1) as u64))
        .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
        let request_file = rustix::fs::openat(
            directory,
            LINUX_EXCHANGE_REQUEST,
            rustix::fs::OFlags::WRONLY
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::EXCL
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
        std::fs::File::from(request_file)
            .write_all(encoded)
            .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
        rustix::fs::renameat(
            &*mailbox_directory,
            &unpublished,
            &*mailbox_directory,
            &name,
        )
        .map_err(|error| ControlRequestError::Indeterminate(error.to_string()))?;
        exchange.name = name.into();
        let mut response = Vec::new();
        response_pipe
            .read_until(b'\n', &mut response)
            .await
            .map_err(|error| ControlRequestError::Indeterminate(error.to_string()))?;
        if response.len() > MAXIMUM_CONTROL_MESSAGE_BYTES || response.pop() != Some(b'\n') {
            return Err(ControlRequestError::Indeterminate(
                "invalid response from Acyclic service".to_owned(),
            ));
        }
        decode_control_response(&response, request_id)
    })
    .await
    .unwrap_or_else(|_| {
        Err(ControlRequestError::Indeterminate(
            "Acyclic service did not answer before the filesystem control deadline".to_owned(),
        ))
    });
    // The service removes every exchange that it answers.
    if result.is_err() {
        exchange.remove();
    }
    result
}
