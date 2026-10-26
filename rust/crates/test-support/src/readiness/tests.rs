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
