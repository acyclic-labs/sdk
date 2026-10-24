//! Optional process counters for the bb2 benchmark, sampled only at checkpoints.
//! CPU is reported in OS ticks, including worker threads; this is not span time.
use acyclic_harness::{Error, Result};

#[cfg(any(target_os = "linux", test))]
#[path = "resources/io.rs"]
mod io;

#[derive(Clone, Copy)]
struct Sample {
    cpu_ticks: u64,
    rss_bytes: u64,
    peak_rss_bytes: u64,
    #[cfg(target_os = "linux")]
    io: io::Counters,
}

#[derive(Default)]
pub(super) struct Samples(Option<(Sample, usize)>);

fn invalid() -> Error {
    Error::Invalid("invalid process resource counters".into())
}

#[cfg(any(target_os = "linux", test))]
fn parse(stat: &str, status: &str) -> Result<Sample> {
    // comm may contain spaces and parentheses; field 3 follows its final ')'.
    let (_, fields) = stat.rsplit_once(')').ok_or_else(invalid)?;
    let mut fields = fields.split_whitespace().skip(11);
    let user = fields
        .next()
        .ok_or_else(invalid)?
        .parse::<u64>()
        .map_err(|_| invalid())?;
    let system = fields
        .next()
        .ok_or_else(invalid)?
        .parse::<u64>()
        .map_err(|_| invalid())?;
    let bytes = |name| -> Result<u64> {
        let line = status
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .ok_or_else(invalid)?;
        let mut parts = line.split_whitespace();
        let kb = parts
            .next()
            .ok_or_else(invalid)?
            .parse::<u64>()
            .map_err(|_| invalid())?;
        if parts.next() != Some("kB") || parts.next().is_some() {
            return Err(invalid());
        }
        kb.checked_mul(1024).ok_or_else(invalid)
    };
    Ok(Sample {
        cpu_ticks: user.checked_add(system).ok_or_else(invalid)?,
        rss_bytes: bytes("VmRSS:")?,
        peak_rss_bytes: bytes("VmHWM:")?,
        #[cfg(target_os = "linux")]
        io: io::Counters::default(),
    })
}

#[cfg(target_os = "linux")]
fn sample() -> Result<Sample> {
    let stat = std::fs::read_to_string("/proc/self/stat")
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let status = std::fs::read_to_string("/proc/self/status")
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let mut sample = parse(&stat, &status)?;
    sample.io = io::Counters::parse(
        &std::fs::read_to_string("/proc/self/io")
            .map_err(|error| Error::Invalid(error.to_string()))?,
    )?;
    Ok(sample)
}

#[cfg(not(target_os = "linux"))]
fn sample() -> Result<Sample> {
    Err(Error::Invalid(
        "--resources currently requires Linux procfs".into(),
    ))
}

impl Samples {
    pub(super) fn from_arguments() -> Result<Self> {
        if std::env::args().any(|arg| arg == "--resources") {
            Ok(Self(Some((sample()?, 0))))
        } else {
            Ok(Self::default())
        }
    }

    // Exclude task startup, then sample the last ten turns at larger cuts.
    pub(super) fn start(&mut self, step: usize) -> Result<()> {
        if self.0.is_some() {
            self.0 = Some((sample()?, step));
        }
        Ok(())
    }

    pub(super) fn report(&mut self, step: usize) -> Result<()> {
        let Some((previous, before)) = self.0 else {
            return Ok(());
        };
        let current = sample()?;
        let turns =
            u64::try_from(step.checked_sub(before).ok_or_else(invalid)?).map_err(|_| invalid())?;
        let ticks = current
            .cpu_ticks
            .checked_sub(previous.cpu_ticks)
            .ok_or_else(invalid)?;
        let per_turn = ticks
            .checked_mul(1000)
            .and_then(|ticks| ticks.checked_div(turns))
            .ok_or_else(invalid)?;
        println!(
            "resources ending at turn {step}: {ticks} process CPU ticks/{turns} turns ({}.{:03} ticks/turn); RSS {} bytes; peak RSS {} bytes",
            per_turn / 1000,
            per_turn % 1000,
            current.rss_bytes,
            current.peak_rss_bytes
        );
        #[cfg(target_os = "linux")]
        current.io.report(previous.io, step, turns)?;
        self.0 = Some((current, step));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn procfs_names_and_counter_failures_are_not_reported_as_zero() -> Result<()> {
        let stat = "1 (bench name) with parens) R 0 0 0 0 0 0 0 0 0 0 17 3 0";
        let status = "VmRSS:\t2 kB\nVmHWM:\t3 kB\n";
        let sample = parse(stat, status)?;
        assert_eq!(sample.cpu_ticks, 20);
        assert_eq!(sample.rss_bytes, 2048);
        assert_eq!(sample.peak_rss_bytes, 3072);
        // The whitespace-only old shape misreads comm as multiple fields.
        assert_ne!(stat.split_whitespace().nth(13), Some("17"));
        for malformed in [
            "",
            "1 (bench) R 0",
            "1 (bench) R 0 0 0 0 0 0 0 0 0 0 invalid 3",
        ] {
            assert!(parse(malformed, status).is_err());
        }
        for malformed in [
            "",
            "VmRSS: 2 bytes\nVmHWM: 3 kB",
            "VmRSS: 18446744073709551615 kB\nVmHWM: 3 kB",
        ] {
            assert!(parse(stat, malformed).is_err());
        }
        Ok(())
    }
}
