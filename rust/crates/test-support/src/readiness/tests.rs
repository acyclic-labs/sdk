use super::*;
use std::io::{Cursor, Read};

const MARKER: &str = "MOUNT_READY";
const PREFIX: &[u8] = b"running 1 test\ntest fixture::mount_owner ... ";

#[test]
fn readiness_survives_an_unterminated_libtest_prefix() -> io::Result<()> {
    // Former producers appended the marker to libtest's unterminated line.
    let mut old = PREFIX.to_vec();
    writeln!(old, "{MARKER}")?;
    assert_eq!(
        wait(&mut Cursor::new(old), MARKER).unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof,
    );

    let mut output = PREFIX.to_vec();
    signal(&mut output, MARKER)?;
    output.extend_from_slice(b"after readiness\n");
    let mut reader = io::BufReader::new(Cursor::new(output));
    wait(&mut reader, MARKER)?;
    let mut remaining = String::new();
    reader.read_to_string(&mut remaining)?;
    assert_eq!(remaining, "after readiness\n");
    Ok(())
}

#[test]
fn readiness_requires_a_complete_exact_record() -> io::Result<()> {
    assert_eq!(
        wait(&mut Cursor::new(MARKER), MARKER).unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof,
    );
    let mut reader = Cursor::new(b"not MOUNT_READY\nMOUNT_READY suffix\nMOUNT_READY\r\n");
    wait(&mut reader, MARKER)?;
    assert_eq!(reader.position(), reader.get_ref().len() as u64);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn readiness_deadline_covers_silence_partial_lines_and_preserves_framing() -> io::Result<()> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, BufReader};
    use tokio::time::{Duration, Instant};

    for prefix in [b"".as_slice(), PREFIX, b"\nMOUNT_READY"] {
        let (mut producer, output) = tokio::io::duplex(128);
        producer.write_all(prefix).await?;
        let mut reader = BufReader::new(output);
        let deadline = Instant::now() + Duration::from_secs(10);
        assert_eq!(
            wait_until(&mut reader, MARKER, deadline)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut,
        );
        assert_eq!(Instant::now(), deadline);
        drop(producer);
    }
    let mut output = PREFIX.to_vec();
    signal(&mut output, MARKER)?;
    output.extend_from_slice(b"retained\n");
    let mut reader = BufReader::new(output.as_slice());
    wait_until(
        &mut reader,
        MARKER,
        Instant::now() + Duration::from_secs(10),
    )
    .await?;
    let mut retained = String::new();
    reader.read_to_string(&mut retained).await?;
    assert_eq!(retained, "retained\n");
    assert_eq!(
        wait_until(
            &mut reader,
            MARKER,
            Instant::now() + Duration::from_secs(10)
        )
        .await
        .unwrap_err()
        .kind(),
        io::ErrorKind::UnexpectedEof,
    );
    Ok(())
}
