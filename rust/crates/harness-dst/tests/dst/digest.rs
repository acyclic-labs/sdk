//! Cross-process oracle over every held trace byte, saved for first-divergence reports.
use super::*;
use std::sync::OnceLock;

// Deliberately outside seeded scope, as if production cached host entropy at
// startup. Both in-process replays see the same value and miss this mutant.
pub(super) fn startup_value() -> &'static uuid::Uuid {
    static VALUE: OnceLock<uuid::Uuid> = OnceLock::new();
    VALUE.get_or_init(uuid::Uuid::new_v4)
}

/// Writes every trace field as canonical lines: one poll, boundary or write per
/// line, Debug-escaped strings, and byte fields as offset-prefixed 32-byte hex
/// chunks. The digest hashes exactly these bytes, so a saved copy locates the
/// first differing line of a cross-process mismatch.
fn canonical(trace: &Trace, out: &mut impl std::io::Write) -> std::io::Result<()> {
    // Exhaustive destructuring makes an added trace field a compile error until
    // this oracle includes it. Counts and lengths delimit every sequence.
    let Trace {
        polls,
        boundaries,
        writes,
        requests,
        dispatches,
        records,
        durable,
    } = trace;
    writeln!(out, "dst-trace-v3")?;
    writeln!(out, "polls {}", polls.len())?;
    for (id, kind) in polls {
        writeln!(out, "poll {id} {kind:?}")?;
    }
    writeln!(out, "boundaries {}", boundaries.len())?;
    for (name, detail) in boundaries {
        writeln!(out, "boundary {name:?} {detail:?}")?;
    }
    writeln!(out, "writes {}", writes.len())?;
    for DurableWrite {
        ordinal,
        operation,
        flushes,
        requested,
        transferred,
        failure,
    } in writes
    {
        writeln!(
            out,
            "write {ordinal} {operation:?} {requested} {transferred:?} {failure:?} flushes={flushes}"
        )?;
    }
    for (label, value) in [
        ("requests", requests),
        ("dispatches", dispatches),
        ("records", records),
    ] {
        chunks(out, label, value)?;
    }
    writeln!(out, "durable {}", durable.len())?;
    for (name, (symlink, value)) in durable {
        writeln!(out, "file {name:?} symlink={symlink}")?;
        chunks(out, "bytes", value)?;
    }
    Ok(())
}

fn chunks(out: &mut impl std::io::Write, label: &str, bytes: &[u8]) -> std::io::Result<()> {
    fn hex(nibble: u8) -> u8 {
        if nibble < 10 {
            b'0' + nibble
        } else {
            b'a' + (nibble - 10)
        }
    }

    writeln!(out, "{label} {}", bytes.len())?;
    let mut line = [[0; 2]; 32];
    for (index, chunk) in bytes.chunks(line.len()).enumerate() {
        write!(out, "{:08x} ", index * line.len())?;
        for (pair, byte) in line.iter_mut().zip(chunk) {
            *pair = [hex(byte >> 4), hex(byte & 15)];
        }
        let encoded = line
            .as_flattened()
            .get(..2 * chunk.len())
            .ok_or_else(|| std::io::Error::other("digest chunk exceeds its hex buffer"))?;
        out.write_all(encoded)?;
        writeln!(out)?;
    }
    Ok(())
}

/// Feeds the canonical bytes to the digest and, unchanged, to `copy`.
struct Tee<W> {
    hash: blake3::Hasher,
    copy: W,
}

impl<W: std::io::Write> std::io::Write for Tee<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.copy.write_all(bytes)?;
        self.hash.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.copy.flush()
    }
}

fn pair_into(trace: &Trace, copy: impl std::io::Write) -> std::io::Result<[blake3::Hash; 2]> {
    let mut tee = Tee {
        hash: blake3::Hasher::new(),
        copy,
    };
    let mut out = std::io::BufWriter::with_capacity(1 << 16, &mut tee);
    canonical(trace, &mut out)?;
    std::io::Write::flush(&mut out)?;
    drop(out);
    let seeded = tee.hash.finalize();
    // Commit to the complete trace once; the mutant adds process-global input
    // to that commitment instead of hashing all held durable bytes again.
    let mut mutant = blake3::Hasher::new_derive_key("acyclic.dst.startup-mutant.v1");
    mutant.update(seeded.as_bytes());
    mutant.update(startup_value().as_bytes());
    Ok([seeded, mutant.finalize()])
}

pub(super) fn pair(trace: &Trace) -> std::io::Result<[blake3::Hash; 2]> {
    pair_into(trace, std::io::sink())
}

// Save every original replay world, including seeds and timestamp inputs that
// the two cross-process digest witnesses do not print. Process identity names
// the evidence file only; it never enters the canonical bytes.
pub(super) fn record_replay(
    trace: &Trace,
    seed: u64,
    erase_source: bool,
    directory_time: i64,
) -> std::io::Result<()> {
    let Some(directory) = std::env::var_os("DST_TRACE_DIR") else {
        return Ok(());
    };
    let path = Path::new(&directory).join(format!(
        "replay-{}-seed-{seed}-entropy-{erase_source}-time-{directory_time}.trace",
        std::process::id(),
    ));
    let [digest, _] = pair_into(trace, std::fs::File::create(&path)?)?;
    println!(
        "\nDST_REPLAY_INPUT_TRACE={} digest={digest}",
        path.display()
    );
    Ok(())
}

/// Prints both digests. Under qualification (DST_TRACE_DIR), the same pass also
/// saves the canonical trace so a cross-process mismatch reports its first
/// differing poll, boundary, write or byte chunk.
pub(super) fn emit(trace: &Trace) -> std::io::Result<[blake3::Hash; 2]> {
    let digests = match std::env::var_os("DST_TRACE_DIR") {
        Some(directory) => {
            let path = Path::new(&directory).join(format!("replay-{}.trace", std::process::id()));
            let digests = pair_into(trace, std::fs::File::create(&path)?)?;
            println!("\nDST_REPLAY_TRACE={}", path.display());
            digests
        }
        None => pair(trace)?,
    };
    let [seeded, mutant] = digests;
    println!("\nDST_REPLAY_DIGEST={seeded}");
    println!("DST_STARTUP_MUTANT_DIGEST={mutant}");
    Ok(digests)
}

#[test]
fn canonical_trace_locates_divergence_and_is_the_digest_input() -> Result<()> {
    let mut trace = Trace {
        polls: vec![(1, "task"), (2, "native")],
        boundaries: vec![("tool".into(), "multi\nline \"detail\"".into())],
        writes: vec![DurableWrite {
            ordinal: 0,
            operation: "write",
            flushes: false,
            requested: 3,
            transferred: Some(3),
            failure: None,
        }],
        requests: vec![7; 40],
        dispatches: Vec::new(),
        records: b"{}\n".to_vec(),
        durable: BTreeMap::from([("a/b".into(), (false, vec![0; 100]))]),
    };
    let lines = |trace: &Trace| -> Result<String> {
        let mut bytes = Vec::new();
        let digests = pair_into(trace, &mut bytes).map_err(storage)?;
        // The saved copy is exactly the digest input: no second encoding.
        assert_eq!(digests[0], blake3::hash(&bytes));
        assert_eq!(digests, pair(trace).map_err(storage)?);
        String::from_utf8(bytes).map_err(storage)
    };
    let left = lines(&trace)?;
    // Embedded newlines stay escaped: one line per element.
    assert_eq!(
        left.lines()
            .filter(|line| line.starts_with("boundary "))
            .count(),
        1
    );
    let differing = |right: &str| -> Vec<(String, String)> {
        left.lines()
            .zip(right.lines())
            .filter(|(left, right)| left != right)
            .map(|(left, right)| (left.into(), right.into()))
            .collect()
    };
    // One durable bit names the chunk holding it.
    trace.durable.get_mut("a/b").unwrap().1[70] ^= 1;
    let right = lines(&trace)?;
    let changed = differing(&right);
    assert_eq!(changed.len(), 1);
    assert!(changed[0].0.starts_with("00000040 ") && changed[0].1.starts_with("00000040 "));
    trace.durable.get_mut("a/b").unwrap().1[70] ^= 1;
    // One poll names that poll.
    trace.polls[1].1 = "task";
    let changed = differing(&lines(&trace)?);
    let poll = |kind: &str| format!("poll 2 {kind:?}");
    assert_eq!(changed, [(poll("native"), poll("task"))]);
    Ok(())
}

#[test]
fn cross_process_seeded_replay_digest() -> Result<()> {
    startup_value();
    let runtime = acyclic_native_runtime::testing::seeded_runtime(71).map_err(storage)?;
    runtime.block_on(async {
        tokio::time::pause();
        let first = replay(71, false).await?;
        emit(&first).map_err(storage)?;
        // Keep the original erasure oracle beside its independent baseline;
        // moving it here balances the two processes without removing a world.
        let broken = replay(71, true).await?;
        assert!(
            same_trace(&first, &broken).is_err(),
            "unseeded trace escaped the replay oracle"
        );
        assert_ne!(
            first.dispatches, broken.dispatches,
            "unseeded production negative control escaped"
        );
        Ok(())
    })
}
