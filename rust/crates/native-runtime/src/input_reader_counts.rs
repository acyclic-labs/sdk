use super::*;
use std::io::{Seek, Write};

#[repr(C)]
#[derive(Debug)]
struct Counts {
    reads: u64,
    polls: u64,
    acquires: u64,
    publishes: u64,
    finishes: u64,
    last_count: isize,
    error: i32,
    bytes: [u8; 64],
    length: usize,
}
impl Default for Counts {
    fn default() -> Self {
        Self {
            reads: 0,
            polls: 0,
            acquires: 0,
            publishes: 0,
            finishes: 0,
            last_count: 0,
            error: 0,
            bytes: [0; 64],
            length: 0,
        }
    }
}
unsafe extern "C" {
    fn acyclic_input_counts(fd: c_int, redundant: c_int, blocked: c_int, out: *mut Counts);
}
fn run(length: usize, redundant: bool, blocked: bool) -> Counts {
    // Each implementation has an independently initialized descriptor and data.
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&(0..length as u8).collect::<Vec<_>>())
        .unwrap();
    file.rewind().unwrap();
    let mut result = Counts::default();
    // SAFETY: synchronous C call borrows this live file and initialized output.
    unsafe {
        acyclic_input_counts(
            file.as_raw_fd(),
            redundant.into(),
            blocked.into(),
            &mut result,
        );
    }
    assert_eq!(
        &result.bytes[..length],
        (0..length as u8).collect::<Vec<_>>()
    );
    assert_eq!(result.length, length);
    assert_eq!((result.last_count, result.error), (0, 0));
    assert_eq!(
        (result.acquires, result.publishes, result.finishes),
        (length as u64 + 1, length as u64 + 1, 1)
    );
    result
}
fn exact_work(result: &Counts, length: usize, blocked: bool) {
    assert_eq!(result.reads, length as u64 + 1 + u64::from(blocked));
    assert_eq!(result.polls, u64::from(blocked));
}
#[test]
fn production_worker_polls_only_after_would_block() {
    for length in [32, 64] {
        for blocked in [false, true] {
            exact_work(&run(length, false, blocked), length, blocked);
        }
    }
}
#[test]
fn restored_redundant_poll_fails_the_same_exact_work_assertion() {
    for length in [32, 64] {
        for blocked in [false, true] {
            let baseline = run(length, true, blocked);
            assert_eq!(baseline.polls, length as u64 + 1 + 2 * u64::from(blocked));
            assert!(std::panic::catch_unwind(|| exact_work(&baseline, length, blocked)).is_err());
        }
    }
}
