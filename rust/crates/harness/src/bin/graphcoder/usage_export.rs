use std::{
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
};

/// A file export replaces its destination only after the complete snapshot
/// succeeds. Dropping an unfinished export removes only its temporary sibling.
pub(super) enum Output {
    Stdout(io::StdoutLock<'static>),
    File {
        writer: BufWriter<tempfile::NamedTempFile>,
        destination: PathBuf,
    },
}

impl Output {
    pub(super) fn open(path: &Path) -> io::Result<Self> {
        if path.as_os_str() == "-" {
            return Ok(Self::Stdout(io::stdout().lock()));
        }
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        Ok(Self::File {
            writer: BufWriter::new(tempfile::NamedTempFile::new_in(parent)?),
            destination: path.to_owned(),
        })
    }

    pub(super) fn writer(&mut self) -> &mut dyn Write {
        match self {
            Self::Stdout(writer) => writer,
            Self::File { writer, .. } => writer,
        }
    }

    pub(super) fn finish(self) -> io::Result<()> {
        match self {
            Self::Stdout(mut writer) => writer.flush(),
            Self::File {
                mut writer,
                destination,
            } => {
                writer.flush()?;
                let file = writer.into_inner().map_err(|error| error.into_error())?;
                file.persist(destination).map_err(|error| error.error)?;
                Ok(())
            }
        }
    }
}

#[test]
fn an_incomplete_snapshot_preserves_the_previous_report() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("usage.jsonl");
    std::fs::write(&path, b"previous complete report\n")?;
    {
        let mut export = Output::open(&path)?;
        export.writer().write_all(b"partial new report")?;
        assert_eq!(std::fs::read(&path)?, b"previous complete report\n");
        // A query or serialization error drops this uncommitted snapshot.
    }
    assert_eq!(std::fs::read(&path)?, b"previous complete report\n");
    let mut export = Output::open(&path)?;
    export.writer().write_all(b"complete new report\n")?;
    export.finish()?;
    assert_eq!(std::fs::read(&path)?, b"complete new report\n");
    assert_eq!(std::fs::read_dir(directory.path())?.count(), 1);
    Ok(())
}
