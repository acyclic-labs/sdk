use super::*;
use std::fs::OpenOptions;
use std::io::{Seek as _, SeekFrom};

const FRAME: LogFrame = LogFrame {
    domain: b"journal-v1\0",
    wide: false,
    minimum_body: 1,
    maximum_body: 32,
};

fn journal(bytes: &[u8]) -> io::Result<(tempfile::TempDir, File)> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal");
    std::fs::write(&path, bytes)?;
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    Ok((directory, file))
}

fn maximum() -> usize {
    usize::try_from(FRAME.maximum_frame_bytes().unwrap()).unwrap()
}

#[test]
fn torn_operation_retires_intact_fragments_with_its_incomplete_tail() -> io::Result<()> {
    for missing in [false, true] {
        let mut bytes = Vec::new();
        FRAME.encode(&[b"committed"], &mut bytes)?;
        let retained_end = bytes.len() as u64;
        FRAME.encode(&[b"uncommitted fragment"], &mut bytes)?;
        let invalid_start = bytes.len() as u64;
        if !missing {
            let mut partial = Vec::new();
            FRAME.encode(&[b"incomplete"], &mut partial)?;
            bytes.extend_from_slice(&partial[..partial.len() - 3]);
        }
        let (directory, mut file) = journal(&bytes)?;
        assert_eq!(
            recover_log_tail(
                &file,
                retained_end,
                invalid_start,
                maximum(),
                Durability::Full,
                |bytes| FRAME.is_valid(bytes)
            )?,
            LogTail::Torn,
        );
        assert_eq!(file.stream_position()?, retained_end);
        assert_eq!(
            std::fs::read(directory.path().join("journal"))?,
            bytes[..retained_end as usize]
        );
    }
    Ok(())
}

#[test]
fn later_valid_frame_prevents_rollback_of_a_damaged_operation() -> io::Result<()> {
    let mut bytes = Vec::new();
    FRAME.encode(&[b"committed"], &mut bytes)?;
    let retained_end = bytes.len() as u64;
    FRAME.encode(&[b"uncommitted fragment"], &mut bytes)?;
    let invalid_start = bytes.len() as u64;
    bytes.resize(bytes.len() + maximum() * 3 + 7, 0);
    FRAME.encode(&[b"later valid frame"], &mut bytes)?;
    let (directory, file) = journal(&bytes)?;
    assert_eq!(
        recover_log_tail(
            &file,
            retained_end,
            invalid_start,
            maximum(),
            Durability::Full,
            |bytes| FRAME.is_valid(bytes)
        )?,
        LogTail::Corrupt,
    );
    assert_eq!(std::fs::read(directory.path().join("journal"))?, bytes);
    Ok(())
}

#[test]
fn invalid_recovery_extents_never_truncate_extend_or_seek_the_file() -> io::Result<()> {
    let bytes = b"retained file bytes";
    let length = bytes.len() as u64;
    for (retained_end, invalid_start, bound) in
        [(5, 4, maximum()), (0, length + 1, maximum()), (0, 0, 0)]
    {
        let (directory, mut file) = journal(bytes)?;
        file.seek(SeekFrom::Start(2))?;
        let error = recover_log_tail(
            &file,
            retained_end,
            invalid_start,
            bound,
            Durability::Full,
            |bytes| FRAME.is_valid(bytes),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(file.stream_position()?, 2);
        assert_eq!(std::fs::read(directory.path().join("journal"))?, bytes);
    }
    Ok(())
}
