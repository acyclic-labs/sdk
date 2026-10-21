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
        files,
        jsonl,
        durable,
    } = trace;
    writeln!(out, "dst-trace-v2")?;
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
        requested,
        transferred,
        failure,
    } in writes
    {
        writeln!(
            out,
            "write {ordinal} {operation:?} {requested} {transferred:?} {failure:?}"
        )?;
    }
    for (label, value) in [
        ("requests", requests),
        ("dispatches", dispatches),
        ("files", files),
        ("jsonl", jsonl),
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
    const HEX: &[u8; 16] = b"0123456789abcdef";
    writeln!(out, "{label} {}", bytes.len())?;
    let mut line = [0; 64 + 1];
    for (index, chunk) in bytes.chunks(32).enumerate() {
        write!(out, "{:08x} ", index * 32)?;
        for (at, byte) in chunk.iter().enumerate() {
            line[2 * at] = HEX[usize::from(byte >> 4)];
            line[2 * at + 1] = HEX[usize::from(byte & 15)];
        }
        let end = 2 * chunk.len();
        line[end] = b'\n';
        out.write_all(&line[..=end])?;
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

fn pair_into(trace: &Trace, copy: impl std::io::Write) -> [blake3::Hash; 2] {
    let mut tee = Tee {
        hash: blake3::Hasher::new(),
        copy,
    };
    let mut out = std::io::BufWriter::with_capacity(1 << 16, &mut tee);
    canonical(trace, &mut out)
        .and_then(|()| std::io::Write::flush(&mut out))
        .expect("canonical trace");
    drop(out);
    let seeded = tee.hash.finalize();
    // Commit to the complete trace once; the mutant adds process-global input
    // to that commitment instead of hashing all held durable bytes again.
    let mut mutant = blake3::Hasher::new_derive_key("acyclic.dst.startup-mutant.v1");
    mutant.update(seeded.as_bytes());
    mutant.update(startup_value().as_bytes());
    [seeded, mutant.finalize()]
}

pub(super) fn pair(trace: &Trace) -> [blake3::Hash; 2] {
    pair_into(trace, std::io::sink())
}

/// Prints both digests. Under qualification (DST_TRACE_DIR), the same pass also
/// saves the canonical trace so a cross-process mismatch reports its first
/// differing poll, boundary, write or byte chunk.
pub(super) fn emit(trace: &Trace) -> [blake3::Hash; 2] {
    let digests = match std::env::var_os("DST_TRACE_DIR") {
        Some(directory) => {
            let path = Path::new(&directory).join(format!("replay-{}.trace", std::process::id()));
            let digests = pair_into(trace, std::fs::File::create(&path).expect("trace file"));
            println!("\nDST_REPLAY_TRACE={}", path.display());
            digests
        }
        None => pair(trace),
    };
    let [seeded, mutant] = digests;
    println!("\nDST_REPLAY_DIGEST={seeded}");
    println!("DST_STARTUP_MUTANT_DIGEST={mutant}");
    digests
}

#[test]
fn canonical_trace_locates_divergence_and_is_the_digest_input() {
    let mut trace = Trace {
        polls: vec![(1, "task"), (2, "native")],
        boundaries: vec![("tool".into(), "multi\nline \"detail\"".into())],
        writes: vec![DurableWrite {
            ordinal: 0,
            operation: "write",
            requested: 3,
            transferred: Some(3),
            failure: None,
        }],
        requests: vec![7; 40],
        dispatches: Vec::new(),
        files: Vec::new(),
        jsonl: b"{}\n".to_vec(),
        durable: BTreeMap::from([("a/b".into(), (false, vec![0; 100]))]),
    };
    let lines = |trace: &Trace| {
        let mut bytes = Vec::new();
        let digests = pair_into(trace, &mut bytes);
        // The saved copy is exactly the digest input: no second encoding.
        assert_eq!(digests[0], blake3::hash(&bytes));
        assert_eq!(digests, pair(trace));
        String::from_utf8(bytes).unwrap()
    };
    let left = lines(&trace);
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
    let right = lines(&trace);
    let changed = differing(&right);
    assert_eq!(changed.len(), 1);
    assert!(changed[0].0.starts_with("00000040 ") && changed[0].1.starts_with("00000040 "));
    trace.durable.get_mut("a/b").unwrap().1[70] ^= 1;
    // One poll names that poll.
    trace.polls[1].1 = "task";
    let changed = differing(&lines(&trace));
    let poll = |kind: &str| format!("poll 2 {kind:?}");
    assert_eq!(changed, [(poll("native"), poll("task"))]);
}

#[test]
fn cross_process_seeded_replay_digest() -> Result<()> {
    startup_value();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    runtime.block_on(async {
        tokio::time::pause();
        let first = replay(71, false).await?;
        emit(&first);
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
