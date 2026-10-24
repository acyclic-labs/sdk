//! Process-wide syscall and storage counters. These include all worker threads
//! and are distinct from logical journal or payload-load counts.
use super::{Result, invalid};

const NAMES: [&str; 6] = [
    "rchar",
    "wchar",
    "syscr",
    "syscw",
    "read_bytes",
    "write_bytes",
];

#[derive(Clone, Copy, Default)]
pub(super) struct Counters([u64; 6]);

impl Counters {
    pub(super) fn parse(input: &str) -> Result<Self> {
        let mut values = [None; 6];
        for line in input.lines() {
            let Some((name, value)) = line.split_once(':') else {
                return Err(invalid());
            };
            let Some(index) = NAMES.iter().position(|expected| *expected == name) else {
                continue;
            };
            let previous = values
                .get_mut(index)
                .ok_or_else(invalid)?
                .replace(value.trim().parse::<u64>().map_err(|_| invalid())?);
            if previous.is_some() {
                return Err(invalid());
            }
        }
        let mut result = Self::default();
        for (destination, value) in result.0.iter_mut().zip(values) {
            *destination = value.ok_or_else(invalid)?;
        }
        Ok(result)
    }

    fn since(self, previous: Self) -> Result<Self> {
        let mut result = Self::default();
        for ((destination, current), previous) in result.0.iter_mut().zip(self.0).zip(previous.0) {
            *destination = current.checked_sub(previous).ok_or_else(invalid)?;
        }
        Ok(result)
    }

    #[cfg(target_os = "linux")]
    pub(super) fn report(self, previous: Self, step: usize, turns: u64) -> Result<()> {
        let [
            read_characters,
            written_characters,
            read_calls,
            write_calls,
            read_bytes,
            written_bytes,
        ] = self.since(previous)?.0;
        println!(
            "process I/O ending at turn {step}: {read_characters} read characters; {written_characters} written characters; {read_calls} read syscalls; {write_calls} write syscalls; {read_bytes} storage read bytes; {written_bytes} storage write bytes/{turns} turns"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_windows_are_deltas_and_missing_counters_cannot_become_zero() -> Result<()> {
        let earlier = "rchar: 100\nwchar: 200\nsyscr: 10\nsyscw: 20\nread_bytes: 4096\nwrite_bytes: 8192\ncancelled_write_bytes: 0\n";
        let later = "write_bytes: 16384\nread_bytes: 12288\nsyscw: 25\nsyscr: 13\nwchar: 1200\nrchar: 600\n";
        let earlier = Counters::parse(earlier)?;
        let later = Counters::parse(later)?;
        assert_eq!(
            later.since(earlier)?.0,
            [500, 1000, 3, 5, 8192, 8192],
            "process I/O reported lifetime totals instead of the measured turn window"
        );
        assert!(earlier.since(later).is_err());
        for invalid in [
            "wchar: 1\nsyscr: 1\nsyscw: 1\nread_bytes: 1\nwrite_bytes: 1\n",
            "rchar: 1\nrchar: 2\nwchar: 1\nsyscr: 1\nsyscw: 1\nread_bytes: 1\nwrite_bytes: 1\n",
            "rchar: invalid\nwchar: 1\nsyscr: 1\nsyscw: 1\nread_bytes: 1\nwrite_bytes: 1\n",
        ] {
            assert!(
                Counters::parse(invalid).is_err(),
                "missing I/O evidence was accepted"
            );
        }
        Ok(())
    }
}
