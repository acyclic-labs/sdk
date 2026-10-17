use super::console_owner::Owner;
use super::input_fence::InputFence;
use super::{
    Capabilities, Decoder, Environment, PROBE_TIMEOUT, ProbeIo, RestoreGuard, ScreenMode, detect,
};
use acyclic_native_runtime::terminal as ffi;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::windows::io::AsRawHandle;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

struct ConsoleIo {
    input: File,
    output: File,
    original: ffi::ConsoleState,
}
impl ConsoleIo {
    fn apply(&mut self, state: ffi::ConsoleState) -> io::Result<()> {
        ffi::console_apply(
            self.input.as_raw_handle() as usize,
            self.output.as_raw_handle() as usize,
            state,
        )
    }
    fn restore(&mut self) -> io::Result<()> {
        self.apply(self.original)
    }
    fn reapply(&mut self) -> io::Result<()> {
        let mut raw = self.original;
        raw.input_mode = (raw.input_mode & !(1 | 2 | 4 | 64)) | 8 | 128 | 512;
        raw.output_mode |= 1 | 4;
        raw.input_page = 65001;
        raw.output_page = 65001;
        self.apply(raw)
    }
}
impl Write for ConsoleIo {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        super::counters::wrote();
        self.output.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

/// The console owner, the input fence every console read enters, and the
/// wake event unbounded input waits include. Every close goes through
/// `Console::close`, so a closed console never leaves a reader blocked.
pub(super) struct Console {
    owner: Arc<Owner<ConsoleIo>>,
    pub(super) input: InputFence,
    pub(super) closed: ffi::ConsoleWake,
}
impl Console {
    pub(super) fn close(&self) -> io::Result<()> {
        // Release a waiting reader, refuse further reads and wait for the
        // admitted one, then restore: no read runs under restored modes.
        let wake = self.closed.signal();
        let fenced = self.input.close();
        Owner::close(&self.owner, ConsoleIo::restore)
            .and(wake)
            .and(fenced)
    }
}

static CONSOLE: OnceLock<Arc<Console>> = OnceLock::new();
fn console_control(event: u32) -> bool {
    if matches!(event, 0 | 1 | 2 | 5 | 6)
        && let Some(console) = CONSOLE.get()
    {
        let _ = console.close();
    }
    #[cfg(test)]
    tests::observe_restored_console(event);
    false
}

/// Windows console owner. Driver operations run outside the owner lock;
/// restoration waits for cancellation acknowledgement before undoing modes.
pub struct Tty {
    pub(super) console: Arc<Console>,
    pub(super) reader: File,
    pub(super) decoder: Decoder,
    capabilities: Option<Capabilities>,
}
/// Output half using the console owner's existing I/O/restoration boundary.
pub struct Output {
    console: Arc<Console>,
}
/// Bytes per console call. Each call runs under its own write deadline
/// (PROBE_TIMEOUT, 100 ms). Measured on the local console: one 4 MiB call
/// times out (110 ms); 256 KiB calls take 10-17 ms at worst and lose no
/// throughput (4 MiB in 256 KiB calls 99 ms, 1 MiB calls 98 ms, 64 KiB calls
/// 114 ms, 4 KiB calls 289 ms). Frames up to 256 KiB are one call.
pub(crate) const CONSOLE_CHUNK: usize = 256 * 1024;

impl super::Present for Output {
    fn present(&mut self, frame: &mut Vec<u8>, cursor_hidden: bool) -> io::Result<()> {
        let owner = &self.console.owner;
        let presented = super::frame::present_parts(frame, CONSOLE_CHUNK, |part| {
            Owner::run(owner, Instant::now() + PROBE_TIMEOUT, move |guard| {
                part.apply(guard, cursor_hidden)
            })
        });
        if presented.is_err() {
            let _ = Owner::run(owner, Instant::now() + PROBE_TIMEOUT, |guard| {
                guard.abort_frame()
            });
        }
        presented
    }
}

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = super::frame::char_prefix(bytes, CONSOLE_CHUNK);
        let bytes = bytes
            .get(..end)
            .ok_or(io::ErrorKind::InvalidInput)?
            .to_vec();
        Owner::run(
            &self.console.owner,
            Instant::now() + PROBE_TIMEOUT,
            move |guard| guard.writer().write(&bytes),
        )
    }
    fn flush(&mut self) -> io::Result<()> {
        Owner::run(
            &self.console.owner,
            Instant::now() + PROBE_TIMEOUT,
            |guard| guard.writer().flush(),
        )
    }
}
impl Drop for Output {
    fn drop(&mut self) {
        let _ = self.console.close();
    }
}

impl Tty {
    /// Separate output while retaining this decoder's negotiation-buffered keys.
    /// Dropping either half restores the console and cancels its outstanding I/O.
    pub fn split(self) -> (Self, Output) {
        let output = Output {
            console: Arc::clone(&self.console),
        };
        (self, output)
    }
    /// Capture modes and code pages before enabling VT and UTF-8.
    pub fn open() -> io::Result<Self> {
        let input = OpenOptions::new().read(true).write(true).open("CONIN$")?;
        let output = OpenOptions::new().read(true).write(true).open("CONOUT$")?;
        let original = ffi::console_state(
            input.as_raw_handle() as usize,
            output.as_raw_handle() as usize,
        )?;
        let reader = input.try_clone()?;
        let guard = RestoreGuard::new(
            ConsoleIo {
                input,
                output,
                original,
            },
            &Capabilities::default(),
        );
        let console = Arc::new(Console {
            owner: Owner::new(guard, ffi::cancel_thread),
            input: InputFence::default(),
            closed: ffi::ConsoleWake::new()?,
        });
        CONSOLE
            .set(Arc::clone(&console))
            .map_err(|_| io::ErrorKind::AlreadyExists)?;
        ffi::console_register(console_control)?;
        if let Err(error) = Owner::run(&console.owner, Instant::now() + PROBE_TIMEOUT, |guard| {
            guard.writer().reapply()
        }) {
            let _ = console.close();
            return Err(error);
        }
        Ok(Self {
            console,
            reader,
            decoder: Decoder::default(),
            capabilities: None,
        })
    }
    /// Cache query-based capabilities and retain the whole-stream decoder.
    pub fn negotiate(&mut self, env: &Environment) -> io::Result<&Capabilities> {
        if self.capabilities.is_none() {
            let (caps, decoder) = detect(self, env)?;
            self.capabilities = Some(caps);
            self.decoder = decoder;
        }
        self.capabilities
            .as_ref()
            .ok_or(io::ErrorKind::NotConnected.into())
    }
    /// Enable known mutable modes, with all I/O waits outside owner state.
    pub fn setup(&mut self, screen: ScreenMode) -> io::Result<()> {
        Owner::run(
            &self.console.owner,
            Instant::now() + PROBE_TIMEOUT,
            move |guard| guard.setup(screen),
        )
    }
    /// Enable one known mutable mode under the same journal.
    pub fn enable_mode(&mut self, mode: u16) -> io::Result<()> {
        Owner::run(
            &self.console.owner,
            Instant::now() + PROBE_TIMEOUT,
            move |guard| guard.enable_mode(mode),
        )
    }
    /// Fence the worker, then restore DEC/console modes and code pages.
    pub fn restore(&mut self) -> io::Result<()> {
        self.console.close()
    }
    fn write_until(&mut self, bytes: &[u8], deadline: Instant) -> io::Result<usize> {
        let bytes = bytes.to_vec();
        Owner::run(&self.console.owner, deadline, move |guard| {
            guard.writer().write(&bytes)
        })
    }
}
impl ProbeIo for Tty {
    fn send(&mut self, mut bytes: &[u8], deadline: Instant) -> io::Result<()> {
        if Instant::now() >= deadline {
            return Err(io::ErrorKind::TimedOut.into());
        }
        while !bytes.is_empty() {
            let count = self.write_until(bytes, deadline)?;
            if count == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            bytes = bytes.get(count..).ok_or(io::ErrorKind::InvalidData)?;
        }
        Ok(())
    }
    fn receive(&mut self, bytes: &mut [u8], deadline: Instant) -> io::Result<usize> {
        // Input stays on the caller: a timed-out output worker must never
        // consume bytes whose result could be abandoned at the deadline.
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Ok(0);
            }
            let count = self.console.input.read(|| {
                ffi::console_read(
                    self.reader.as_raw_handle() as usize,
                    bytes,
                    Some(deadline.min(now + std::time::Duration::from_millis(1))),
                    None,
                )
            })?;
            let ffi::ConsoleRead::Bytes(count) = count else {
                continue;
            };
            if count != 0 || bytes.is_empty() {
                return Ok(count);
            }
        }
    }
    fn detected(&mut self, caps: &Capabilities) -> io::Result<()> {
        let caps = caps.clone();
        Owner::run(
            &self.console.owner,
            Instant::now() + PROBE_TIMEOUT,
            move |guard| guard.detected(&caps),
        )
    }
}
impl Write for Tty {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.write_until(bytes, Instant::now() + PROBE_TIMEOUT)
    }
    fn flush(&mut self) -> io::Result<()> {
        Owner::run(
            &self.console.owner,
            Instant::now() + PROBE_TIMEOUT,
            |guard| guard.writer().flush(),
        )
    }
}
impl Drop for Tty {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::Duration;

    static REPORT: OnceLock<PathBuf> = OnceLock::new();
    static BASELINE: OnceLock<ffi::ConsoleState> = OnceLock::new();

    pub(super) fn observe_restored_console(event: u32) {
        if let (Some(path), Some(old)) = (REPORT.get(), BASELINE.get()) {
            let input = OpenOptions::new().read(true).write(true).open("CONIN$");
            let output = OpenOptions::new().read(true).write(true).open("CONOUT$");
            let (Ok(input), Ok(output)) = (input, output) else {
                return;
            };
            if let Ok(actual) = ffi::console_state(
                input.as_raw_handle() as usize,
                output.as_raw_handle() as usize,
            ) {
                let contents = format!(
                    "{event} {} {} {} {} {} {} {} {}",
                    old.input_mode,
                    old.output_mode,
                    old.input_page,
                    old.output_page,
                    actual.input_mode,
                    actual.output_mode,
                    actual.input_page,
                    actual.output_page
                );
                let _ = std::fs::write(path, contents);
            }
        }
    }

    #[test]
    fn windows_lifecycle_child() -> Result<(), Box<dyn std::error::Error>> {
        let Ok(report) = std::env::var("ACYCLIC_TERMINAL_WINDOWS_REPORT") else {
            return Ok(());
        };
        let path = PathBuf::from(&report);
        assert!(REPORT.set(path.clone()).is_ok());
        let input = OpenOptions::new().read(true).write(true).open("CONIN$")?;
        let output = OpenOptions::new().read(true).write(true).open("CONOUT$")?;
        let mut baseline = ffi::console_state(
            input.as_raw_handle() as usize,
            output.as_raw_handle() as usize,
        )?;
        baseline.input_mode |= 1 | 2 | 4;
        baseline.output_mode &= !4;
        baseline.input_page = 437;
        baseline.output_page = 437;
        ffi::console_apply(
            input.as_raw_handle() as usize,
            output.as_raw_handle() as usize,
            baseline,
        )?;
        assert!(BASELINE.set(baseline).is_ok());
        let mut tty = Tty::open()?;
        assert!(
            matches!(tty.send(b"EXPIRED", Instant::now()), Err(error) if error.kind() == io::ErrorKind::TimedOut)
        );
        let raw = ffi::console_state(
            input.as_raw_handle() as usize,
            output.as_raw_handle() as usize,
        )?;
        assert_eq!(raw.input_mode & (1 | 2 | 4), 0);
        assert_ne!(raw.input_mode & 512, 0);
        assert_ne!(raw.output_mode & 4, 0);
        assert_eq!((raw.input_page, raw.output_page), (65001, 65001));
        // The ConPTY host sends one 100-byte input packet. A zero-capacity
        // receive must consume nothing; the next receive may consume only 24.
        std::fs::write(path.with_extension("input-ready"), b"input")?;
        assert_eq!(
            tty.receive(&mut [], Instant::now() + Duration::from_millis(20))?,
            0
        );
        let mut first = [0; 24];
        let first_count = tty.receive(&mut first, Instant::now() + Duration::from_millis(500))?;
        assert!(first_count > 0 && first_count <= first.len());
        let mut received = first[..first_count].to_vec();
        let deadline = Instant::now() + Duration::from_millis(500);
        while received.len() < 100 && Instant::now() < deadline {
            let mut remaining = [0; 24];
            let count = tty.receive(&mut remaining, deadline)?;
            received.extend_from_slice(&remaining[..count]);
        }
        assert_eq!(received, b"0123456789".repeat(10));
        std::fs::write(path.with_extension("probe-ready"), b"probe")?;
        let caps = tty.negotiate(&Environment::default())?;
        assert_eq!(caps.mode(2004), super::super::ModeState::Reset);
        assert_eq!(caps.mode(1049), super::super::ModeState::Reset);
        assert_eq!(caps.mode(1003), super::super::ModeState::Reset);
        std::fs::write(path.with_extension("negotiated"), b"negotiated")?;
        tty.setup(ScreenMode::Alternate)?;
        std::fs::write(path.with_extension("setup"), b"setup")?;
        tty.enable_mode(1004)?;
        let blocked = std::env::var("ACYCLIC_TERMINAL_WINDOWS_EVENT").as_deref() == Ok("blocked");
        let payload = vec![b'B'; 1024 * 1024];
        let mut setup_wait = None;
        let mut close_wait = None;
        if blocked {
            // The host leaves ConPTY output unread through the whole send.
            // The production-worker loom model checks that these waits do
            // not retain the owner lock used to admit another operation.
            for index in 0..2 {
                let start = Instant::now();
                assert!(
                    matches!(tty.send(&payload, start + Duration::from_millis(50)), Err(error) if error.kind() == io::ErrorKind::TimedOut)
                );
                assert!(start.elapsed() < Duration::from_millis(500));
                std::fs::write(path.with_extension(format!("send{index}")), b"timed out")?;
            }
            let setting_up = Arc::clone(&tty.console.owner);
            let setup = std::thread::spawn(move || {
                let _ = Owner::run(&setting_up, Instant::now() + PROBE_TIMEOUT, |guard| {
                    guard.enable_mode(1003)
                });
            });
            let start = Instant::now();
            let result = tty.send(b"\x1b[c", start + Duration::from_millis(2));
            assert!(
                result.is_ok()
                    || matches!(result, Err(error) if error.kind() == io::ErrorKind::TimedOut)
            );
            assert!(start.elapsed() < Duration::from_millis(50));
            std::fs::write(path.with_extension("short-send"), b"bounded")?;
            setup_wait = Some(setup);
            let closing = Arc::clone(&tty.console);
            close_wait = Some(std::thread::spawn(move || closing.close()));
            let start = Instant::now();
            let result = tty.send(b"\x1b[c", start + Duration::from_millis(2));
            assert!(
                result.is_ok()
                    || matches!(result, Err(error) if matches!(error.kind(), io::ErrorKind::TimedOut | io::ErrorKind::NotConnected))
            );
            assert!(start.elapsed() < Duration::from_millis(50));
        }
        if std::env::var("ACYCLIC_TERMINAL_WINDOWS_EVENT").as_deref() == Ok("resize") {
            use crate::key_event::InputEvent;
            use futures_core::Stream;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()?;
            let mut events = std::pin::pin!(tty.events()?);
            std::fs::write(path.with_extension("ready"), b"ready")?;
            let dimensions = runtime.block_on(std::future::poll_fn(|cx| {
                loop {
                    match events.as_mut().poll_next(cx) {
                        std::task::Poll::Ready(Some(Ok(InputEvent::Resize(cols, rows)))) => {
                            return std::task::Poll::Ready(Ok((cols, rows)));
                        }
                        std::task::Poll::Ready(Some(Ok(_))) => {}
                        std::task::Poll::Ready(Some(Err(error))) => {
                            return std::task::Poll::Ready(Err(error));
                        }
                        std::task::Poll::Ready(None) => {
                            return std::task::Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
                        }
                        std::task::Poll::Pending => return std::task::Poll::Pending,
                    }
                }
            }))?;
            std::fs::write(
                path.with_extension("resize"),
                format!("{} {}", dimensions.0, dimensions.1),
            )?;
            return Ok(());
        }
        if std::env::var("ACYCLIC_TERMINAL_WINDOWS_EVENT").as_deref() == Ok("latency") {
            return measure_latency(tty, &path);
        }
        std::fs::write(path.with_extension("ready"), b"ready")?;
        if blocked {
            // Windows routes GenerateConsoleCtrlEvent through the same server
            // whose output is blocked. Drain only after the timeout assertion;
            // the real-lock loom model covers close racing the pending wait.
            while !path.with_extension("draining").exists() {
                std::thread::sleep(Duration::from_millis(1));
            }
            if let Some(setup) = setup_wait {
                setup
                    .join()
                    .map_err(|_| io::Error::other("setup worker panicked"))?;
            }
            if let Some(closing) = close_wait {
                let _ = closing
                    .join()
                    .map_err(|_| io::Error::other("restoration worker panicked"))?;
            }
            ffi::console_break()?;
        }
        if std::env::var("ACYCLIC_TERMINAL_WINDOWS_EVENT").as_deref() == Ok("break") {
            ffi::console_break()?;
        }
        loop {
            std::thread::park();
        }
    }

    /// Records when each key the ConPTY host typed reached the stream
    /// consumer, in system-precise-time ticks the host compares with its own
    /// send times. The host owns the statistics; nothing here is asserted
    /// about time.
    fn measure_latency(tty: Tty, path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
        use crate::key_event::{InputEvent, KeyEventKind};
        use futures_core::Stream;
        use std::time::SystemTime;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()?;
        let mut events = std::pin::pin!(tty.events()?);
        let count: usize = std::fs::read_to_string(path.with_extension("latency-count"))?
            .trim()
            .parse()?;
        let mut received = Vec::with_capacity(count);
        std::fs::write(path.with_extension("ready"), b"ready")?;
        runtime.block_on(std::future::poll_fn(|cx| {
            while received.len() < count {
                match events.as_mut().poll_next(cx) {
                    std::task::Poll::Ready(Some(Ok(InputEvent::Key(key))))
                        if key.kind == KeyEventKind::Press =>
                    {
                        let ticks = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .map_or(0, |since| since.as_nanos() / 100);
                        received.push(ticks);
                    }
                    std::task::Poll::Ready(Some(Ok(_))) => {}
                    std::task::Poll::Ready(Some(Err(error))) => {
                        return std::task::Poll::Ready(Err(error));
                    }
                    std::task::Poll::Ready(None) => {
                        return std::task::Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
                    }
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                }
            }
            std::task::Poll::Ready(Ok(()))
        }))?;
        let lines: Vec<String> = received.iter().map(u128::to_string).collect();
        std::fs::write(path.with_extension("latency-child"), lines.join("\n"))?;
        Ok(())
    }
}
