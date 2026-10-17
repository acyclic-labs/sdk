use std::io::{self, BufRead, Read, Write};
use std::os::windows::fs::OpenOptionsExt as _;
use std::path::Path;
use std::time::{Duration, SystemTime};
use windows::Win32::Storage::FileSystem::{FILE_SHARE_READ, FILE_SHARE_WRITE};

pub(super) const MODE: &str = "ACYCLIC_PROJFS_METADATA_CLIENT";
const PATH: &str = "ACYCLIC_PROJFS_METADATA_PATH";
const READY: &str = "ACYCLIC_METADATA_READY";

pub(super) fn run(
    mode: &str,
    path: &Path,
    before_action: impl FnOnce() -> Result<(), Box<dyn std::error::Error>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let module = module_path!()
        .split_once("::")
        .expect("client belongs to a test crate")
        .1
        .strip_suffix("::client")
        .expect("client is nested beneath its metadata tests");
    let mut child = acyclic_native_runtime::ProcessTree::spawn(
        std::process::Command::new(std::env::current_exe()?)
            .arg(format!(
                "{module}::external_metadata_only_change_survives_remount"
            ))
            .args(["--exact", "--ignored", "--nocapture"])
            .env(MODE, mode)
            .env(PATH, path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped()),
    )?;
    let stdout = child.take_stdout().ok_or("client stdout missing")?;
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
    let reader = std::thread::spawn(move || -> io::Result<()> {
        let mut stdout = io::BufReader::new(stdout);
        let mut line = String::new();
        let mut ready = Some(ready_tx);
        while stdout.read_line(&mut line)? != 0 {
            if line.trim_end() == READY {
                if let Some(sender) = ready.take() {
                    let _ = sender.send(());
                }
            } else {
                eprint!("{line}");
            }
            line.clear();
        }
        Ok(())
    });
    let action: Result<(), Box<dyn std::error::Error>> = (|| {
        ready_rx.recv_timeout(Duration::from_secs(5))?;
        before_action()?;
        let mut stdin = child.take_stdin().ok_or("client stdin missing")?;
        stdin.write_all(b"g")?;
        drop(stdin);
        let status = child.wait(Duration::from_secs(20))?;
        if !status.success() {
            return Err(format!("metadata client failed: {status}").into());
        }
        Ok(())
    })();
    let cleanup = child.terminate(None);
    let output = reader
        .join()
        .map_err(|_| io::Error::other("client output reader panicked"))
        .and_then(std::convert::identity);
    action
        .and(cleanup.map_err(Into::into))
        .and(output.map_err(Into::into))
}

fn start_action() -> io::Result<()> {
    println!("\n{READY}");
    io::stdout().flush()?;
    let mut signal = [0];
    io::stdin().read_exact(&mut signal)?;
    if signal != *b"g" {
        return Err(io::Error::other("invalid client signal"));
    }
    Ok(())
}

fn writable(path: &Path) -> io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0)
        .open(path)
}

pub(super) fn execute(mode: &std::ffi::OsStr) -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::var_os(PATH).ok_or("client path missing")?;
    let path = Path::new(&path);
    match mode.to_str() {
        Some("late") => {
            std::fs::write(path, b"late")?;
            let file = writable(path)?;
            start_action()?;
            file.set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(978_307_200))?;
        }
        Some("stress") => {
            start_action()?;
            stress(path)?;
        }
        Some("readonly") => {
            start_action()?;
            let mut permissions = std::fs::metadata(path)?.permissions();
            permissions.set_readonly(true);
            std::fs::set_permissions(path, permissions)?;
        }
        _ => return Err("unknown metadata client mode".into()),
    }
    Ok(())
}

fn read_contents(file: &mut std::fs::File, index: usize, data: &mut Vec<u8>) -> io::Result<()> {
    data.clear();
    file.read_to_end(data)?;
    if data.len() != 4096 * (index % 2) || data.iter().any(|byte| *byte != b'x') {
        return Err(io::Error::other(format!("unexpected contents in f{index}")));
    }
    Ok(())
}

fn receive_index(receiver: &std::sync::mpsc::Receiver<usize>, expected: usize) -> io::Result<()> {
    if receiver.recv().map_err(io::Error::other)? != expected {
        return Err(io::Error::other("unexpected metadata client phase"));
    }
    Ok(())
}

fn read_path(directory: &Path, index: usize, data: &mut Vec<u8>) -> io::Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ.0)
        .open(directory.join(format!("f{index}.txt")))?;
    read_contents(&mut file, index, data)
}

fn reader_read_path(directory: &Path, index: usize, data: &mut Vec<u8>) -> io::Result<()> {
    match read_path(directory, index, data) {
        // ReadAllBytes also denies sharing writes. The original reader allowed
        // these expected collisions with the editor and projection worker.
        Err(error) if error.raw_os_error() == Some(32) => Ok(()),
        result => result,
    }
}

fn stress(directory: &Path) -> io::Result<()> {
    let editor = std::sync::Mutex::new(());
    std::thread::scope(|scope| {
        let editor = &editor;
        let lanes: Vec<_> = (0..4)
            .map(|lane| scope.spawn(move || stress_lane(directory, lane, editor)))
            .collect();
        for lane in lanes {
            lane.join()
                .map_err(|_| io::Error::other("metadata client lane panicked"))??;
        }
        Ok(())
    })
}

fn stress_lane(directory: &Path, lane: usize, editor: &std::sync::Mutex<()>) -> io::Result<()> {
    let started = std::time::Instant::now();
    let (phase_tx, phase_rx) = std::sync::mpsc::sync_channel(0);
    let (read_tx, read_rx) = std::sync::mpsc::sync_channel(0);
    std::thread::scope(|scope| {
        let reader = scope.spawn(move || -> io::Result<usize> {
            let mut reads = 0;
            let mut data = Vec::with_capacity(4097);
            for _ in 0..4 {
                for index in (lane..120).step_by(4) {
                    reader_read_path(directory, index, &mut data)?;
                    reads += 1;
                }
            }
            // The fifth complete pass verifies the original deny-write sharing
            // conflict. The editor retries only after the reader closes.
            for index in (lane..120).step_by(4) {
                let mut file = std::fs::OpenOptions::new()
                    .read(true)
                    .share_mode(FILE_SHARE_READ.0)
                    .open(directory.join(format!("f{index}.txt")))?;
                read_contents(&mut file, index, &mut data)?;
                reads += 1;
                read_tx.send(index).map_err(io::Error::other)?;
                receive_index(&phase_rx, index)?;
                drop(file);
                read_tx.send(index).map_err(io::Error::other)?;
            }
            // Like ReadAllBytes, readers deny sharing writes. Read each edit
            // after its writable handle closes, without retry polling.
            for index in (lane..120).step_by(4) {
                receive_index(&phase_rx, index)?;
                reader_read_path(directory, index, &mut data)?;
                reads += 1;
                read_tx.send(index).map_err(io::Error::other)?;
            }
            receive_index(&phase_rx, 120)?;
            for _ in 0..4 {
                for index in (lane..120).step_by(4) {
                    reader_read_path(directory, index, &mut data)?;
                    reads += 1;
                }
            }
            Ok(reads)
        });
        let editing = (|| -> io::Result<usize> {
            // The original workload has one editor and concurrent readers.
            let _editor = editor
                .lock()
                .map_err(|_| io::Error::other("editor panicked"))?;
            for index in (lane..120).step_by(4) {
                receive_index(&read_rx, index)?;
                let path = directory.join(format!("f{index}.txt"));
                match writable(&path) {
                    Err(error) if error.raw_os_error() == Some(32) => {}
                    Err(error) => return Err(error),
                    Ok(_) => return Err(io::Error::other("reader did not deny sharing writes")),
                }
                phase_tx.send(index).map_err(io::Error::other)?;
                receive_index(&read_rx, index)?;
            }
            let mut prefetches = 0;
            let mut data = Vec::with_capacity(4097);
            for index in (lane..120).step_by(4) {
                if index % 3 == 0 {
                    read_path(directory, index, &mut data)?;
                    prefetches += 1;
                }
                let file = writable(&directory.join(format!("f{index}.txt")))?;
                file.set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000))?;
                drop(file);
                phase_tx.send(index).map_err(io::Error::other)?;
                receive_index(&read_rx, index)?;
            }
            phase_tx.send(120).map_err(io::Error::other)?;
            Ok(prefetches)
        })();
        drop(phase_tx);
        drop(read_rx);
        let reading = reader
            .join()
            .map_err(|_| io::Error::other("metadata reader panicked"));
        eprintln!("lane {lane} elapsed {:?}", started.elapsed());
        let (reads, prefetches) = match (reading?, editing) {
            (Ok(reads), Ok(prefetches)) => (reads, prefetches),
            (Err(reader), Err(editor)) => {
                return Err(io::Error::other(format!(
                    "reader: {reader}; editor: {editor}"
                )));
            }
            (Err(error), _) | (_, Err(error)) => return Err(error),
        };
        if reads + prefetches != 310 {
            return Err(io::Error::other("metadata read coverage changed"));
        }
        Ok(())
    })
}
