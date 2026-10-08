//! Bounded, best-effort diagnostics. Never use this writer for durable data.
//!
//! All writers lock the active file itself. Copy-and-truncate rollover keeps
//! its identity, so even aliases share the same lock across restarts. Each
//! file is at most `limit` bytes; an oversized write is rejected.
//! Copy into one bounded staging file before atomically publishing the archive.
//! Logs are diagnostic output, so copying does not imply a durability guarantee.
//! Destinations are trusted host configuration. Writers must reserve the active
//! archive and staging names; replacing/unlinking an active file is unsupported.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek as _, Write};
use std::path::{Path, PathBuf};

pub(crate) const LIMIT: u64 = 4 * 1024 * 1024;

pub(crate) struct ServiceLog {
    archive: PathBuf,
    staged: PathBuf,
    file: File,
    limit: u64,
}

impl ServiceLog {
    pub(crate) fn open(path: &Path, limit: u64) -> io::Result<Self> {
        if limit == 0 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            let mut directory = fs::DirBuilder::new();
            directory.recursive(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt as _;
                directory.mode(0o700);
            }
            directory.create(parent)?;
        }
        let mut log = Self {
            archive: suffix(path, ".1"),
            staged: suffix(path, ".1.next"),
            file: file_options().create(true).open(path)?,
            limit,
        };
        log.locked(|log| {
            match file_options().open(&log.archive) {
                Ok(file) => {
                    size(&file, limit)?;
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            size(&log.file, limit)?;
            remove_staged(&log.staged)
        })?;
        Ok(log)
    }

    fn locked<T>(&mut self, action: impl FnOnce(&mut Self) -> io::Result<T>) -> io::Result<T> {
        fs2::FileExt::lock_exclusive(&self.file)?;
        let result = action(self);
        let unlocked = fs2::FileExt::unlock(&self.file);
        // The formatter can report errors to stderr; strip user paths.
        result
            .and_then(|value| unlocked.map(|()| value))
            .map_err(|error| error.kind().into())
    }

    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        let length = u64::try_from(bytes.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
        if length > self.limit {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let active_size = size(&self.file, self.limit)?;
        if active_size > self.limit - length {
            // No instrumented filesystem calls here: tracing from its own
            // writer would recursively acquire this lock.
            remove_staged(&self.staged)?;
            let mut archive = file_options().create_new(true).open(&self.staged)?;
            self.file.rewind()?;
            io::copy(&mut self.file, &mut archive)?;
            drop(archive);
            // Replace the directory entry, never truncate through an alias.
            fs::rename(&self.staged, &self.archive)?;
            self.file.set_len(0)?;
        }
        self.file.seek(io::SeekFrom::End(0))?;
        self.file.write_all(bytes)
    }
}

impl Write for ServiceLog {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.locked(|log| log.append(bytes)).map(|()| bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn size(file: &File, limit: u64) -> io::Result<u64> {
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    if metadata.len() > limit {
        return Err(io::ErrorKind::InvalidData.into());
    }
    Ok(metadata.len())
}

fn file_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).write(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        // A diagnostic destination must not block waiting for a FIFO reader.
        options.custom_flags(libc::O_NONBLOCK);
        options.mode(0o600);
    }
    options
}

fn suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

fn remove_staged(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::indexing_slicing,
        reason = "test assertions and fixed-size records"
    )]
    use super::*;

    fn writer(path: &Path, limit: u64) -> ServiceLog {
        ServiceLog::open(path, limit).expect("open log")
    }

    #[test]
    fn restart_rollover_and_boundary_writes_have_fixed_retention() {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("service.log");
        writer(&path, 8)
            .write_all(b"12345678")
            .expect("exact boundary");
        assert!(!suffix(&path, ".1").exists());
        let mut restarted = writer(&path, 8);
        restarted.write_all(b"abcdefgh").expect("rollover");
        assert_eq!(fs::read(suffix(&path, ".1")).expect("archive"), b"12345678");
        assert_eq!(fs::read(&path).expect("active"), b"abcdefgh");
        assert_eq!(
            restarted.write(b"too-large").expect_err("oversize").kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(fs::read(&path).expect("unchanged"), b"abcdefgh");
        restarted.write_all(b"latest").expect("second rollover");
        assert_eq!(fs::read(suffix(&path, ".1")).expect("archive"), b"abcdefgh");
        assert_eq!(fs::read(&path).expect("active"), b"latest");
        assert_eq!(fs::read_dir(directory.path()).expect("entries").count(), 2);
    }

    #[test]
    fn rollover_failure_does_not_allow_unbounded_append_and_can_recover() {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("service.log");
        let mut log = writer(&path, 4);
        log.write_all(b"full").expect("fill");
        fs::create_dir(suffix(&path, ".1")).expect("block archive with directory");
        for _ in 0..20 {
            let error = log.write_all(b"x").expect_err("rollover must fail");
            assert!(
                !error
                    .to_string()
                    .contains(&directory.path().display().to_string())
            );
            assert_eq!(fs::read(&path).expect("active"), b"full");
        }
        fs::remove_dir(suffix(&path, ".1")).expect("repair archive");
        log.write_all(b"ok").expect("recover without restart");
        assert_eq!(fs::read(&path).expect("active"), b"ok");
        assert_eq!(fs::read(suffix(&path, ".1")).expect("archive"), b"full");
    }

    #[test]
    fn rejects_oversized_logs_and_invalid_destinations() {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("service.log");
        fs::write(&path, b"oversized content").expect("oversized active");
        assert!(ServiceLog::open(&path, 4).is_err());
        assert_eq!(
            fs::read(&path).expect("unchanged active"),
            b"oversized content"
        );
        fs::write(&path, b"full").expect("bounded active");
        fs::write(suffix(&path, ".1"), b"oversized old archive").expect("old archive");
        assert!(ServiceLog::open(&path, 4).is_err());
        assert_eq!(
            fs::read(suffix(&path, ".1")).expect("unchanged archive"),
            b"oversized old archive"
        );
        fs::write(suffix(&path, ".1"), b"past").expect("bounded archive");
        let log = writer(&path, 4);
        drop(log);
        fs::remove_file(&path).expect("remove active");
        fs::create_dir(&path).expect("invalid active");
        assert!(ServiceLog::open(&path, 4).is_err());
        assert!(ServiceLog::open(&path, 0).is_err());
        assert!(ServiceLog::open(&directory.path().join("service.log.1/child"), 4).is_err());
    }

    #[test]
    fn subscriber_delivers_whole_records_and_rejects_oversized_events() {
        use tracing_subscriber::layer::SubscriberExt as _;
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("service.log");
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .without_time()
                .with_level(false)
                .with_target(false)
                .with_ansi(false)
                .with_writer(std::sync::Mutex::new(writer(&path, 8))),
        );
        let _second = tracing::Dispatch::new(tracing_subscriber::registry());
        let _guard = tracing::subscriber::set_default(subscriber);
        tracing::info!("{}{}", "12345678", "x");
        assert_eq!(fs::metadata(&path).expect("active").len(), 0);
        tracing::info!("{}{}", "1234", "567");
        assert_eq!(fs::read(&path).expect("active"), b"1234567\n");
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "isolated filesystem fault helper"]
    fn filesystem_fault_child() {
        let path = PathBuf::from(std::env::var_os("ACYCLIC_TEST_LOG_PATH").expect("log path"));
        let mut log = writer(&path, 32);
        limit_file_size(8);
        let error = log
            .write_all(b"0123456789abcdef")
            .expect_err("partial filesystem write");
        assert!(!error.to_string().contains(&path.display().to_string()));
        assert_eq!(fs::metadata(&path).expect("partial log").len(), 8);
    }

    #[cfg(unix)]
    #[allow(unsafe_code, reason = "called only in dedicated fault subprocesses")]
    fn limit_file_size(bytes: libc::rlim_t) {
        let limit = libc::rlimit {
            rlim_cur: bytes,
            rlim_max: bytes,
        };
        // SAFETY: valid fixed-size limit pointer, with no retained references;
        // this helper runs only in a dedicated process that exits afterwards.
        assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_FSIZE, &limit) }, 0);
        assert_ne!(
            // SAFETY: SIG_IGN is the OS-defined handler, and this process is isolated.
            unsafe { libc::signal(libc::SIGXFSZ, libc::SIG_IGN) },
            libc::SIG_ERR
        );
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "isolated archive-copy failure helper"]
    fn archive_fault_child() {
        let path = PathBuf::from(std::env::var_os("ACYCLIC_TEST_LOG_PATH").expect("log path"));
        let mut log = writer(&path, 16);
        log.write_all(b"0123456789abcdef").expect("fill active");
        fs::write(suffix(&path, ".1"), b"past").expect("published archive");
        limit_file_size(8);
        assert!(log.write_all(b"new").is_err());
        assert_eq!(
            fs::read(&path).expect("active preserved"),
            b"0123456789abcdef"
        );
        assert_eq!(
            fs::read(suffix(&path, ".1")).expect("archive preserved"),
            b"past"
        );
        assert!(
            fs::metadata(suffix(&path, ".1.next"))
                .expect("partial stage")
                .len()
                <= 16
        );
    }

    #[cfg(unix)]
    #[test]
    fn archive_copy_failure_preserves_published_logs_and_restart_recovers() {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("service.log");
        let output = run_child(&path, "service_log::tests::archive_fault_child");
        assert!(output.status.success(), "{output:?}");
        let mut restarted = writer(&path, 16);
        assert!(!suffix(&path, ".1.next").exists());
        restarted.write_all(b"restart").expect("recovered rollover");
        assert_eq!(
            fs::read(suffix(&path, ".1")).expect("archive"),
            b"0123456789abcdef"
        );
        assert_eq!(fs::read(&path).expect("active"), b"restart");
    }

    #[test]
    fn restart_discards_only_the_reserved_unpublished_stage() {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("service.log");
        fs::write(&path, b"active").expect("active");
        fs::write(suffix(&path, ".1"), b"archive").expect("archive");
        fs::write(suffix(&path, ".1.next"), b"partial").expect("interrupted stage");
        drop(writer(&path, 8));
        assert!(!suffix(&path, ".1.next").exists());
        assert_eq!(fs::read(&path).expect("active preserved"), b"active");
        assert_eq!(
            fs::read(suffix(&path, ".1")).expect("archive preserved"),
            b"archive"
        );
    }

    #[cfg(unix)]
    #[test]
    fn partial_filesystem_write_is_bounded_and_restart_recovers() {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("service.log");
        let output = run_child(&path, "service_log::tests::filesystem_fault_child");
        assert!(output.status.success(), "{output:?}");
        assert_eq!(fs::read(&path).expect("partial record"), b"01234567");
        writer(&path, 32).write_all(b"recovered").expect("restart");
        assert_eq!(
            fs::read(&path).expect("recovered record"),
            b"01234567recovered"
        );
    }

    #[test]
    #[ignore = "isolated process termination helper"]
    fn terminated_writer_child() {
        let path = PathBuf::from(std::env::var_os("ACYCLIC_TEST_LOG_PATH").expect("log path"));
        let mut log = writer(&path, 8);
        log.write_all(b"complete").expect("fill");
        fs2::FileExt::lock_exclusive(&log.file).expect("hold lock");
        // Deliberately bypass Rust destructors while holding the file lock.
        std::process::exit(17);
    }

    #[test]
    fn terminated_writer_releases_its_lock_and_restart_rolls_over() {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("service.log");
        let output = run_child(&path, "service_log::tests::terminated_writer_child");
        assert_eq!(output.status.code(), Some(17));
        writer(&path, 8)
            .write_all(b"restart")
            .expect("released lock");
        assert_eq!(fs::read(suffix(&path, ".1")).expect("archive"), b"complete");
        assert_eq!(fs::read(&path).expect("active"), b"restart");
    }

    fn run_child(path: &Path, name: &str) -> std::process::Output {
        acyclic_native_runtime::process_output(
            std::process::Command::new(std::env::current_exe().expect("test binary"))
                .args(["--exact", name, "--ignored"])
                .env("ACYCLIC_TEST_LOG_PATH", path),
            std::time::Duration::from_secs(10),
            4096,
        )
        .expect("bounded child")
    }

    #[cfg(unix)]
    #[test]
    fn fifo_destination_is_rejected_without_waiting_for_a_reader() {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("service.log");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&path)
                .status()
                .expect("mkfifo")
                .success()
        );
        assert!(ServiceLog::open(&path, 32).is_err());
    }

    #[test]
    fn independently_opened_concurrent_writers_keep_whole_records_and_bounds() {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("service.log");
        let threads: Vec<_> = (0..8)
            .map(|writer_id| {
                let path = path.clone();
                std::thread::spawn(move || {
                    let mut log = writer(&path, 64);
                    for record in 0..100 {
                        log.write_all(format!("{writer_id}:{record:03}\n").as_bytes())
                            .expect("write record");
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("writer thread");
        }
        for file in [&path, &suffix(&path, ".1")] {
            let bytes = fs::read(file).expect("retained file");
            assert!(bytes.len() <= 64);
            assert_eq!(bytes.len() % 6, 0, "torn record");
            for record in bytes.as_chunks::<6>().0 {
                assert!((b'0'..=b'7').contains(&record[0]));
                assert_eq!(record[1], b':');
                assert!(record[2..5].iter().all(u8::is_ascii_digit));
                assert_eq!(record[5], b'\n');
            }
        }
    }

    #[test]
    fn rollover_replaces_archive_hard_links_without_truncating_the_target() {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("service.log");
        let external = directory.path().join("external");
        fs::write(&external, b"safe").expect("external content");
        fs::hard_link(&external, suffix(&path, ".1")).expect("archive alias");
        let mut log = writer(&path, 4);
        log.write_all(b"full").expect("active");
        log.write_all(b"next").expect("rollover");
        assert_eq!(fs::read(&external).expect("external preserved"), b"safe");
        assert_eq!(fs::read(suffix(&path, ".1")).expect("archive"), b"full");

        fs::remove_file(suffix(&path, ".1")).expect("remove archive");
        fs::hard_link(&path, suffix(&path, ".1")).expect("archive aliases active");
        log.write_all(b"last").expect("self-alias rollover");
        assert_eq!(fs::read(suffix(&path, ".1")).expect("archive"), b"next");
        assert_eq!(fs::read(&path).expect("active preserved"), b"last");
        drop(log);
        fs::remove_file(suffix(&path, ".1")).expect("remove archive");
        fs::hard_link(&path, suffix(&path, ".1")).expect("restart archive alias");
        let mut restarted = writer(&path, 4);
        assert_eq!(fs::read(&path).expect("startup preserves active"), b"last");
        restarted
            .write_all(b"more")
            .expect("rollover after alias restart");
        assert_eq!(fs::read(suffix(&path, ".1")).expect("archive"), b"last");
        assert_eq!(fs::read(&path).expect("active"), b"more");
    }

    #[test]
    fn active_aliases_keep_one_identity_across_rollover() {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("service.log");
        let alias = directory.path().join("alias.log");
        let mut first = writer(&path, 4);
        fs::hard_link(&path, &alias).expect("active alias");
        let mut second = writer(&alias, 4);
        first.write_all(b"full").expect("fill");
        second.write_all(b"next").expect("alias rollover");
        first.write_all(b"last").expect("original rollover");
        assert_eq!(fs::read(&path).expect("active"), b"last");
        assert_eq!(fs::read(&alias).expect("alias"), b"last");
        assert_eq!(
            fs::read(suffix(&alias, ".1")).expect("alias archive"),
            b"full"
        );
        assert_eq!(fs::read(suffix(&path, ".1")).expect("archive"), b"next");
    }

    #[cfg(unix)]
    #[test]
    fn created_log_files_and_directories_are_private() {
        use std::os::unix::fs::PermissionsExt as _;
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("logs/service.log");
        let mut log = writer(&path, 4);
        log.write_all(b"full").expect("fill");
        log.write_all(b"next").expect("rollover");
        for file in [&path, &suffix(&path, ".1")] {
            assert_eq!(
                fs::metadata(file).expect("metadata").permissions().mode() & 0o077,
                0
            );
        }
        assert_eq!(
            fs::metadata(path.parent().expect("parent"))
                .expect("directory metadata")
                .permissions()
                .mode()
                & 0o077,
            0
        );
    }

    #[cfg(unix)]
    #[test]
    fn rollover_replaces_archive_symlinks_without_following_them() {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("service.log");
        let target = directory.path().join("missing-target");
        std::os::unix::fs::symlink(&target, suffix(&path, ".1")).expect("dangling alias");
        let mut log = writer(&path, 4);
        log.write_all(b"full").expect("fill");
        log.write_all(b"next").expect("replace dangling alias");
        assert!(!target.exists());
        assert_eq!(fs::read(suffix(&path, ".1")).expect("archive"), b"full");
        fs::remove_file(suffix(&path, ".1")).expect("remove archive");
        fs::write(&target, b"safe").expect("target");
        std::os::unix::fs::symlink(&target, suffix(&path, ".1")).expect("existing alias");
        log.write_all(b"last").expect("replace existing alias");
        assert_eq!(fs::read(&target).expect("target preserved"), b"safe");
    }

    #[test]
    #[ignore = "subprocess helper for process_writers_serialize_independent_handles"]
    fn process_writer() {
        let path = PathBuf::from(std::env::var_os("ACYCLIC_TEST_LOG_PATH").expect("log path"));
        let id = std::env::var("ACYCLIC_TEST_LOG_WRITER").expect("writer id");
        let start = PathBuf::from(std::env::var_os("ACYCLIC_TEST_LOG_START").expect("start path"));
        let mut log = writer(&path, 50_000);
        fs::write(start.with_extension(&id), b"ready").expect("ready marker");
        wait_for(&start);
        for record in 0..1000 {
            log.write_all(format!("{id}:{record:04}\n").as_bytes())
                .expect("record");
        }
    }

    fn wait_for(path: &Path) {
        let started = std::time::Instant::now();
        while !path.exists() {
            assert!(
                started.elapsed() < std::time::Duration::from_secs(10),
                "child barrier timed out"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    #[test]
    fn process_writers_serialize_independent_handles() {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("service.log");
        drop(writer(&path, 50_000));
        let alias = directory.path().join("alias.log");
        fs::hard_link(&path, &alias).expect("active alias");
        let start = directory.path().join("start");
        let children: Vec<_> = (0..4)
            .map(|id| {
                std::process::Command::new(std::env::current_exe().expect("test binary"))
                    .args(["--exact", "service_log::tests::process_writer", "--ignored"])
                    .env(
                        "ACYCLIC_TEST_LOG_PATH",
                        if id % 2 == 0 { &path } else { &alias },
                    )
                    .env("ACYCLIC_TEST_LOG_WRITER", id.to_string())
                    .env("ACYCLIC_TEST_LOG_START", &start)
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .spawn()
                    .expect("child writer")
            })
            .collect();
        for id in 0..4 {
            wait_for(&start.with_extension(id.to_string()));
        }
        fs::write(&start, b"start").expect("release children");
        for mut child in children {
            assert!(child.wait().expect("child completion").success());
        }
        let records = fs::read_to_string(&path).expect("log");
        let observed: std::collections::BTreeSet<_> = records.lines().collect();
        assert_eq!(records.lines().count(), 4000);
        for id in 0..4 {
            for record in 0..1000 {
                assert!(observed.contains(format!("{id}:{record:04}").as_str()));
            }
        }
    }
}
