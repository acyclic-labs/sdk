//! Interpose the actual positional libc boundary, retaining real file contents.
#![allow(
    unsafe_code,
    reason = "interposes positional libc calls and forwards their live buffers to the kernel"
)]

use super::*;
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::FileExt as _;

const PAYLOAD: &[u8] = b"native-byte-progress";
const OFFSET: u64 = 7;
const CHUNK: usize = 3;

#[derive(Clone, Copy, PartialEq)]
enum Direction {
    Read,
    Write,
}

#[derive(Clone, Copy)]
enum Fault {
    Interrupted,
    Zero,
}

#[derive(Clone, Copy)]
struct Probe {
    descriptor: libc::c_int,
    direction: Direction,
    fault: Option<(usize, Fault)>,
    calls: usize,
}

thread_local! {
    static PROBE: Cell<Option<Probe>> = const { Cell::new(None) };
}

fn offered(descriptor: libc::c_int, direction: Direction, length: usize) -> Result<usize, Fault> {
    PROBE
        .try_with(|slot| {
            let Some(mut probe) = slot
                .get()
                .filter(|probe| probe.descriptor == descriptor && probe.direction == direction)
            else {
                return Ok(length);
            };
            probe.calls += 1;
            slot.set(Some(probe));
            if let Some((call, fault)) = probe.fault
                && call == probe.calls
            {
                return Err(fault);
            }
            Ok(length.min(CHUNK))
        })
        .unwrap_or(Ok(length))
}

fn transfer(
    direction: Direction,
    descriptor: libc::c_int,
    pointer: *const libc::c_void,
    length: usize,
    offset: libc::off_t,
) -> libc::ssize_t {
    let length = match offered(descriptor, direction, length) {
        Ok(length) => length,
        Err(Fault::Zero) => return 0,
        Err(Fault::Interrupted) => {
            // SAFETY: errno belongs to this calling thread.
            unsafe { *libc::__errno_location() = libc::EINTR };
            return -1;
        }
    };
    let operation = match direction {
        Direction::Read => libc::SYS_pread64,
        Direction::Write => libc::SYS_pwrite64,
    };
    // SAFETY: the caller supplied this live descriptor and buffer. Only the
    // offered length is reduced; pointer, offset and syscall direction persist.
    unsafe { libc::syscall(operation, descriptor, pointer, length, offset) as libc::ssize_t }
}

#[unsafe(no_mangle)]
extern "C" fn pread(
    descriptor: libc::c_int,
    pointer: *mut libc::c_void,
    length: usize,
    offset: libc::off_t,
) -> libc::ssize_t {
    transfer(Direction::Read, descriptor, pointer, length, offset)
}

#[unsafe(no_mangle)]
extern "C" fn pread64(
    descriptor: libc::c_int,
    pointer: *mut libc::c_void,
    length: usize,
    offset: libc::off64_t,
) -> libc::ssize_t {
    transfer(Direction::Read, descriptor, pointer, length, offset)
}

#[unsafe(no_mangle)]
extern "C" fn pwrite(
    descriptor: libc::c_int,
    pointer: *const libc::c_void,
    length: usize,
    offset: libc::off_t,
) -> libc::ssize_t {
    transfer(Direction::Write, descriptor, pointer, length, offset)
}

#[unsafe(no_mangle)]
extern "C" fn pwrite64(
    descriptor: libc::c_int,
    pointer: *const libc::c_void,
    length: usize,
    offset: libc::off64_t,
) -> libc::ssize_t {
    transfer(Direction::Write, descriptor, pointer, length, offset)
}

fn probe<T>(
    file: &File,
    direction: Direction,
    fault: Option<(usize, Fault)>,
    operation: impl FnOnce() -> T,
) -> (T, usize) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            PROBE.set(None);
        }
    }
    assert!(PROBE.get().is_none());
    PROBE.set(Some(Probe {
        descriptor: file.as_raw_fd(),
        direction,
        fault,
        calls: 0,
    }));
    let _reset = Reset;
    let result = operation();
    (result, PROBE.get().unwrap().calls)
}

fn physical_bytes(file: &File) -> io::Result<Vec<u8>> {
    let mut bytes = vec![0; file.metadata()?.len() as usize];
    file.read_exact_at(&mut bytes, 0)?;
    Ok(bytes)
}

fn positional_writes_return_first_failure_and_keep_only_the_physical_prefix() -> io::Result<()> {
    for metered in [false, true] {
        for fault in [Fault::Interrupted, Fault::Zero] {
            for call in 1..=3 {
                let mut file = tempfile::tempfile()?;
                file.seek(SeekFrom::Start(91))?;
                let meter = ReadBudgetMeter::new(u64::MAX, u64::MAX);
                let selected = metered.then_some((&meter as &dyn NativeReadMeter, true));
                let (result, calls) = probe(&file, Direction::Write, Some((call, fault)), || {
                    write_all_at(&file, OFFSET, PAYLOAD, selected)
                });
                assert_eq!(calls, call);
                assert_eq!(
                    result.unwrap_err().kind(),
                    match fault {
                        Fault::Interrupted => io::ErrorKind::Interrupted,
                        Fault::Zero => io::ErrorKind::WriteZero,
                    }
                );
                let prefix = (call - 1) * CHUNK;
                let mut expected = if prefix == 0 {
                    Vec::new()
                } else {
                    vec![0; OFFSET as usize]
                };
                expected.extend_from_slice(&PAYLOAD[..prefix]);
                assert_eq!(physical_bytes(&file)?, expected);
                assert_eq!(file.stream_position()?, 91);
                assert_eq!(
                    meter.used_operations.load(Ordering::Acquire),
                    u64::from(metered) * call as u64
                );
                assert_eq!(
                    meter.used_bytes.load(Ordering::Acquire),
                    u64::from(metered) * prefix as u64
                );
                // A later explicit invocation writes only the unacknowledged
                // suffix; it is not an automatic reissue of the failed step.
                write_all_at(&file, OFFSET + prefix as u64, &PAYLOAD[prefix..], selected)?;
                let mut expected = vec![0; OFFSET as usize];
                expected.extend_from_slice(PAYLOAD);
                assert_eq!(physical_bytes(&file)?, expected);
                assert_eq!(file.stream_position()?, 91);
            }
        }
        let file = tempfile::tempfile()?;
        let meter = ReadBudgetMeter::new(u64::MAX, u64::MAX);
        let (result, calls) = probe(&file, Direction::Write, None, || {
            write_all_at(
                &file,
                0,
                PAYLOAD,
                metered.then_some((&meter as &dyn NativeReadMeter, true)),
            )
        });
        result?;
        assert_eq!(calls, PAYLOAD.len().div_ceil(CHUNK));
        assert_eq!(physical_bytes(&file)?, PAYLOAD);
    }
    Ok(())
}

fn bounded_reads_end_on_interruption_without_losing_initialized_prefixes() -> io::Result<()> {
    let mut file = tempfile::tempfile()?;
    write_all_at(&file, OFFSET, PAYLOAD, None)?;
    file.seek(SeekFrom::Start(91))?;
    for spare in [false, true] {
        for require_full in [false, true] {
            for call in 1..=3 {
                let meter = ReadBudgetMeter::new(u64::MAX, u64::MAX);
                let mut bytes = if spare {
                    Vec::with_capacity(PAYLOAD.len())
                } else {
                    vec![0xa5; PAYLOAD.len()]
                };
                let (result, calls) = probe(
                    &file,
                    Direction::Read,
                    Some((call, Fault::Interrupted)),
                    || {
                        if spare {
                            crate::file::read_bounded_spare(
                                &file,
                                OFFSET,
                                &mut bytes,
                                PAYLOAD.len(),
                                require_full,
                                &meter,
                            )
                        } else {
                            crate::file::read_bounded_slice(
                                &file,
                                OFFSET,
                                &mut bytes,
                                require_full,
                                &meter,
                            )
                        }
                    },
                );
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
                assert_eq!(calls, call);
                let prefix = (call - 1) * CHUNK;
                assert_eq!(&bytes[..prefix], &PAYLOAD[..prefix]);
                if spare {
                    assert_eq!(bytes.len(), prefix);
                } else {
                    assert!(bytes[prefix..].iter().all(|byte| *byte == 0xa5));
                }
                assert_eq!(meter.used_operations.load(Ordering::Acquire), call as u64);
                assert_eq!(meter.used_bytes.load(Ordering::Acquire), prefix as u64);
                assert_eq!(file.stream_position()?, 91);
            }
        }
    }
    // Real EOF is terminal even for a requested full range. Short successful
    // reads are byte progress, and the final zero result is observed once.
    for require_full in [false, true] {
        let meter = ReadBudgetMeter::new(u64::MAX, u64::MAX);
        let mut bytes = vec![0xa5; PAYLOAD.len() + 1];
        let (result, calls) = probe(&file, Direction::Read, None, || {
            crate::file::read_bounded_slice(&file, OFFSET, &mut bytes, require_full, &meter)
        });
        if require_full {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
        } else {
            assert_eq!(result?, PAYLOAD.len());
        }
        assert_eq!(&bytes[..PAYLOAD.len()], PAYLOAD);
        assert_eq!(bytes[PAYLOAD.len()], 0xa5);
        assert_eq!(calls, PAYLOAD.len().div_ceil(CHUNK) + 1);
    }
    Ok(())
}

fn positional_negative_control_std_reissues_the_interrupted_native_call() -> io::Result<()> {
    let file = tempfile::tempfile()?;
    let (result, calls) = probe(
        &file,
        Direction::Write,
        Some((2, Fault::Interrupted)),
        || file.write_all_at(PAYLOAD, OFFSET),
    );
    result?;
    assert_eq!(calls, PAYLOAD.len().div_ceil(CHUNK) + 1);
    let mut bytes = vec![0; PAYLOAD.len()];
    let (result, calls) = probe(
        &file,
        Direction::Read,
        Some((2, Fault::Interrupted)),
        || file.read_exact_at(&mut bytes, OFFSET),
    );
    result?;
    assert_eq!(calls, PAYLOAD.len().div_ceil(CHUNK) + 1);
    assert_eq!(bytes, PAYLOAD);
    Ok(())
}

fn positional_refusal_precedes_physical_dispatch_and_preserves_the_original_file() -> io::Result<()>
{
    let file = tempfile::tempfile()?;
    write_all_at(&file, 0, PAYLOAD, None)?;
    let meter = ReadBudgetMeter::new(u64::MAX, u64::MAX);
    for metered in [false, true] {
        let (result, calls) = probe(
            &file,
            Direction::Write,
            Some((1, Fault::Interrupted)),
            || {
                write_all_at(
                    &file,
                    u64::MAX,
                    PAYLOAD,
                    metered.then_some((&meter as &dyn NativeReadMeter, true)),
                )
            },
        );
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert_eq!(calls, 0);
    }
    meter.cancelled.store(true, Ordering::Release);
    let (result, calls) = probe(
        &file,
        Direction::Write,
        Some((1, Fault::Interrupted)),
        || write_all_at(&file, 0, PAYLOAD, Some((&meter, true))),
    );
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
    assert_eq!(calls, 0);
    let mut bytes = vec![0xa5; PAYLOAD.len()];
    let (result, calls) = probe(
        &file,
        Direction::Read,
        Some((1, Fault::Interrupted)),
        || crate::file::read_bounded_slice(&file, 0, &mut bytes, true, &meter),
    );
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
    assert_eq!(calls, 0);
    assert!(bytes.iter().all(|byte| *byte == 0xa5));
    assert_eq!(meter.used_operations.load(Ordering::Acquire), 0);
    assert_eq!(meter.used_bytes.load(Ordering::Acquire), 0);
    assert_eq!(physical_bytes(&file)?, PAYLOAD);
    Ok(())
}

#[test]
fn positional_first_result_preserves_physical_prefixes_and_refuses_before_dispatch()
-> io::Result<()> {
    positional_writes_return_first_failure_and_keep_only_the_physical_prefix()?;
    bounded_reads_end_on_interruption_without_losing_initialized_prefixes()?;
    positional_negative_control_std_reissues_the_interrupted_native_call()?;
    positional_refusal_precedes_physical_dispatch_and_preserves_the_original_file()
}
