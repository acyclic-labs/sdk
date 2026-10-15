use super::*;
use std::io::{BufRead as _, Write as _};
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
use windows_sys::Win32::Foundation::{FILETIME, WAIT_OBJECT_0};
use windows_sys::Win32::System::Threading::{
    CreateEventW, GetCurrentThread, GetThreadTimes, SetEvent, WaitForSingleObject,
};

const EVENT: &str = "ACYCLIC_NATIVE_WAIT_EVENT";
const READY: &str = "ACYCLIC_NATIVE_WAIT_READY";

fn event(name: &str) -> io::Result<OwnedHandle> {
    let wide: Vec<_> = name.encode_utf16().chain(Some(0)).collect();
    // SAFETY: the terminated name is live for this synchronous creation/open.
    let handle = unsafe { CreateEventW(std::ptr::null(), 1, 0, wide.as_ptr()) };
    if handle.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: CreateEventW transfers one fresh owned handle on success.
    Ok(unsafe { OwnedHandle::from_raw_handle(handle) })
}

fn thread_cpu() -> io::Result<u64> {
    let empty = FILETIME {
        dwLowDateTime: 0,
        dwHighDateTime: 0,
    };
    let (mut created, mut exited, mut kernel, mut user) = (empty, empty, empty, empty);
    // SAFETY: the current-thread pseudo handle and writable outputs are valid.
    if unsafe {
        GetThreadTimes(
            GetCurrentThread(),
            &raw mut created,
            &raw mut exited,
            &raw mut kernel,
            &raw mut user,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let ticks =
        |time: FILETIME| u64::from(time.dwLowDateTime) | (u64::from(time.dwHighDateTime) << 32);
    ticks(kernel)
        .checked_add(ticks(user))
        .ok_or(io::ErrorKind::InvalidData.into())
}

fn thread_cycles() -> io::Result<u64> {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn QueryThreadCycleTime(
            thread: windows_sys::Win32::Foundation::HANDLE,
            cycles: *mut u64,
        ) -> i32;
    }
    let mut cycles = 0;
    // SAFETY: the current-thread pseudo handle and writable cycle count are valid.
    if unsafe { QueryThreadCycleTime(GetCurrentThread(), &raw mut cycles) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(cycles)
}

#[test]
fn wait_helper() -> io::Result<()> {
    let Ok(name) = std::env::var(EVENT) else {
        return Ok(());
    };
    let event = event(&name)?;
    println!("{READY}");
    std::io::stdout().flush()?;
    // SAFETY: this helper retains its event while the parent controls release.
    assert_eq!(
        unsafe { WaitForSingleObject(event.as_raw_handle(), 5000) },
        WAIT_OBJECT_0
    );
    Ok(())
}

#[test]
fn wait_preserves_timeout_and_child_ownership() -> io::Result<()> {
    wait_preserves_ownership(false)
}

#[test]
fn coarse_wait_preserves_timeout_and_child_ownership() -> io::Result<()> {
    wait_preserves_ownership(true)
}

fn wait_preserves_ownership(coarse: bool) -> io::Result<()> {
    if std::env::var_os("ACYCLIC_NATIVE_WAIT_START_GATE").is_some() {
        let mut signal = [0];
        std::io::stdin().read_exact(&mut signal)?;
        assert_eq!(signal, *b"g");
    }
    let _subscriber = std::env::var_os("ACYCLIC_NATIVE_WAIT_TRACING").map(|_| {
        tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::TRACE)
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
                .with_ansi(false)
                .finish(),
        )
    });
    let root = tempfile::tempdir()?;
    let suffix = root
        .path()
        .file_name()
        .ok_or(io::ErrorKind::InvalidInput)?
        .to_string_lossy();
    let name = format!("Local\\acyclic-native-wait-{}-{suffix}", std::process::id());
    let gate = event(&name)?;
    let accounting = std::env::var_os("ACYCLIC_NATIVE_WAIT_ACCOUNTING")
        .map(|_| crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX));
    let meter = accounting
        .as_ref()
        .map(|meter| meter as &dyn crate::NativeReadMeter);
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args([
            "--exact",
            "process_tree::wait_tests::wait_helper",
            "--nocapture",
        ])
        .env_clear()
        .env(EVENT, name)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped());
    let mut custody = None;
    if let Err(error) = ProcessTree::spawn_controlled(&mut command, meter, &mut custody) {
        if let Some(tree) = custody.as_mut() {
            tree.terminate(meter)?;
        }
        return Err(error);
    }
    let mut tree = custody.ok_or(io::ErrorKind::NotFound)?;
    let wait = |tree: &mut ProcessTree, timeout| {
        process_control_attempt(meter, crate::NativeOperationKind::Read, || {
            tree.wait(timeout)
        })
    };
    let pending_wait = |tree: &mut ProcessTree, timeout: Duration| {
        if !coarse {
            return wait(tree, timeout);
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(io::ErrorKind::InvalidInput)?;
        process_control_attempt(meter, crate::NativeOperationKind::Read, || {
            platform::wait_for_exit_coarse(
                tree.child.as_mut().ok_or(io::ErrorKind::NotFound)?,
                deadline,
            )
        })
    };
    let stdout = tree.take_stdout().ok_or(io::ErrorKind::NotFound)?;
    let (sent, received) = std::sync::mpsc::sync_channel(1);
    let reader = std::thread::spawn(move || -> io::Result<()> {
        for line in std::io::BufReader::new(stdout).lines() {
            if line?.ends_with(READY) {
                sent.send(()).map_err(io::Error::other)?;
            }
        }
        Ok(())
    });
    let result = (|| {
        received
            .recv_timeout(Duration::from_secs(5))
            .map_err(io::Error::other)?;
        assert_eq!(
            wait(&mut tree, Duration::MAX)
                .err()
                .ok_or(io::ErrorKind::Other)?
                .kind(),
            io::ErrorKind::InvalidInput,
        );
        assert_eq!(
            platform::wait_for_exit_coarse(
                tree.child.as_mut().ok_or(io::ErrorKind::NotFound)?,
                Instant::now(),
            )
            .err()
            .ok_or(io::ErrorKind::Other)?
            .kind(),
            io::ErrorKind::TimedOut,
        );
        assert_eq!(
            wait(&mut tree, Duration::ZERO)
                .err()
                .ok_or(io::ErrorKind::Other)?
                .kind(),
            io::ErrorKind::TimedOut
        );
        let cycles = thread_cycles()?;
        let cpu = thread_cpu()?;
        let operations = || {
            accounting.as_ref().map_or(0, |meter| {
                meter
                    .used_operations
                    .load(std::sync::atomic::Ordering::Acquire)
            })
        };
        let before = operations();
        let started = Instant::now();
        let result = pending_wait(&mut tree, Duration::from_millis(50));
        let elapsed = started.elapsed();
        let cpu = thread_cpu()? - cpu;
        let cycles = thread_cycles()? - cycles;
        let recorded = operations() - before;
        assert_eq!(recorded, u64::from(meter.is_some()));
        assert_eq!(
            result.err().ok_or(io::ErrorKind::Other)?.kind(),
            io::ErrorKind::TimedOut
        );
        assert!(elapsed >= Duration::from_millis(50));
        assert!(!tree.is_reaped());
        assert!(tree.try_wait(meter)?.is_none());
        eprintln!(
            "NATIVE_WAIT_TIMEOUT elapsed_us={} cpu_100ns={cpu} cycles={cycles} recorded_ops={recorded} coarse={coarse}",
            elapsed.as_micros()
        );
        // SAFETY: this parent owns the event and the ready helper waits on it.
        if unsafe { SetEvent(gate.as_raw_handle()) } == 0 {
            return Err(io::Error::last_os_error());
        }
        assert!(pending_wait(&mut tree, Duration::from_secs(5))?.success());
        assert!(!tree.is_reaped());
        assert!(wait(&mut tree, Duration::ZERO)?.success());
        assert!(tree.try_wait(meter)?.is_some());
        assert!(
            platform::wait_for_exit_coarse(
                tree.child.as_mut().ok_or(io::ErrorKind::NotFound)?,
                Instant::now(),
            )?
            .success()
        );
        Ok(())
    })();
    let cleanup = tree.terminate(meter);
    let drained = reader
        .join()
        .map_err(|_| io::Error::other("readiness reader panicked"))?;
    result.and(cleanup).and(drained)
}
