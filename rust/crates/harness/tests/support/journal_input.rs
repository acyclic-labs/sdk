//! Client journal decoding and repair, shared by the `journal_decode` fuzz
//! target and the decoder properties: the fixture records [`Seen`] events and
//! only [`INVARIANTS`] judge them. Tests inject defects through `tamper`.
use acyclic_harness::{
    client::{ClientStateStore, FileClientState, SessionRecord},
    wire,
};
use prost::Message;
use std::path::Path;

/// A journal load, with the error rendered.
pub type Loaded = Result<Vec<SessionRecord>, String>;

/// Where a load happened; `tamper` receives it with the file path.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Stage {
    /// The second load of arbitrary bytes.
    Reload,
    /// The load of arbitrary bytes behind valid outer checksums.
    Framed,
    /// The load of an oversized-length header.
    Oversized,
    /// The load after two committed appends.
    Committed,
    /// The first load after cutting the journal.
    Cut,
    /// The load of a journal with one flipped bit.
    Corrupt,
}

pub type Tamper = fn(Stage, &Path, &mut Loaded);

#[derive(Debug)]
pub enum Seen {
    /// Repairing arbitrary bytes twice loads the same records.
    Reloaded {
        first: Loaded,
        second: Loaded,
    },
    /// `expected` is the direct protobuf decode, taken before the load.
    Framed {
        expected: Option<SessionRecord>,
        loaded: Loaded,
    },
    Oversized {
        loaded: Loaded,
        length: u64,
    },
    Committed {
        records: Vec<SessionRecord>,
        recovered: Loaded,
    },
    /// The journal cut at `cut`, loaded twice.
    Cut {
        cut: usize,
        expected: Vec<SessionRecord>,
        loads: [(Loaded, u64); 2],
        repaired_end: u64,
    },
    Corrupt {
        loaded: Loaded,
        untouched: bool,
    },
}

pub type Check = fn(&Seen) -> Result<(), String>;

pub const INVARIANTS: [(&str, Check); 6] = [
    ("idempotent", |seen| match seen {
        Seen::Reloaded {
            first: Ok(first),
            second,
        } if second.as_ref() != Ok(first) => Err(format!(
            "repair is not idempotent: {first:?} then {second:?}"
        )),
        _ => Ok(()),
    }),
    ("framed", |seen| match seen {
        Seen::Framed {
            expected: Some(record),
            loaded,
        } if loaded.as_deref() != Ok(std::slice::from_ref(record)) => Err(format!(
            "a framed record decoded as {loaded:?}, not {record:?}"
        )),
        Seen::Framed {
            expected: None,
            loaded: Ok(records),
        } => Err(format!(
            "an undecodable framed record loaded as {records:?}"
        )),
        _ => Ok(()),
    }),
    ("oversized", |seen| match seen {
        Seen::Oversized { loaded, length }
            if loaded.as_ref().map(Vec::len) != Ok(0) || *length != 0 =>
        {
            Err(format!(
                "an oversized header loaded {loaded:?} and kept {length} bytes"
            ))
        }
        _ => Ok(()),
    }),
    ("committed", |seen| match seen {
        Seen::Committed { records, recovered } if recovered.as_ref() != Ok(records) => Err(
            format!("journal lost a complete committed record: {recovered:?}"),
        ),
        Seen::Cut {
            cut,
            expected,
            loads,
            ..
        } if loads
            .iter()
            .any(|(loaded, _)| loaded.as_ref() != Ok(expected)) =>
        {
            Err(format!("a cut at {cut} lost committed records: {loads:?}"))
        }
        _ => Ok(()),
    }),
    ("repaired", |seen| match seen {
        Seen::Cut {
            cut,
            loads,
            repaired_end,
            ..
        } if loads.iter().any(|(_, length)| length != repaired_end) => Err(format!(
            "journal retained an incomplete trailing record after a cut at {cut}: {loads:?}"
        )),
        _ => Ok(()),
    }),
    ("corrupt", |seen| match seen {
        Seen::Corrupt { loaded, untouched } if loaded.is_ok() || !untouched => Err(format!(
            "a complete corrupt record was accepted ({loaded:?}) or rewritten ({untouched})"
        )),
        _ => Ok(()),
    }),
];

/// Fuzz entry: every invariant, as panics.
pub fn check(data: &[u8]) {
    let judged = observe(data, |_, _, _| {}, &mut |seen| {
        for (name, check) in INVARIANTS {
            if let Err(detail) = check(&seen) {
                panic!("{name}: {detail}");
            }
        }
        Ok::<(), std::convert::Infallible>(())
    });
    if let Err(never) = judged {
        match never {}
    }
}

fn load(store: &mut FileClientState, stage: Stage, path: &Path, tamper: Tamper) -> Loaded {
    let mut loaded = store.load().map_err(|error| error.to_string());
    tamper(stage, path, &mut loaded);
    loaded
}

fn length(path: &Path) -> u64 {
    std::fs::metadata(path).unwrap().len()
}

/// Records how the journal loads and repairs `data` and journals built from it.
pub fn observe<S>(
    data: &[u8],
    tamper: Tamper,
    see: &mut impl FnMut(Seen) -> Result<(), S>,
) -> Result<(), S> {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state");
    std::fs::write(&path, data).unwrap();
    {
        let mut store = FileClientState::open(&path).unwrap();
        let first = store.load().map_err(|error| error.to_string());
        let second = load(&mut store, Stage::Reload, &path, tamper);
        see(Seen::Reloaded { first, second })?;
    }
    // Valid outer checksums force arbitrary bytes through the nested protobuf
    // decoder. A random fuzzer cannot ordinarily guess both BLAKE3 checksums.
    let mut framed = (data.len() as u32).to_le_bytes().to_vec();
    framed.extend_from_slice(blake3::hash(data).as_bytes());
    let checksum = blake3::hash(&framed);
    framed.extend_from_slice(checksum.as_bytes());
    framed.extend_from_slice(data);
    std::fs::write(&path, &framed).unwrap();
    {
        let expected = SessionRecord::decode(data).ok();
        let mut store = FileClientState::open(&path).unwrap();
        let loaded = load(&mut store, Stage::Framed, &path, tamper);
        see(Seen::Framed { expected, loaded })?;
    }
    framed[..4].copy_from_slice(&u32::MAX.to_le_bytes());
    let checksum = blake3::hash(&framed[..36]);
    framed[36..68].copy_from_slice(checksum.as_bytes());
    std::fs::write(&path, &framed).unwrap();
    {
        let mut store = FileClientState::open(&path).unwrap();
        let loaded = load(&mut store, Stage::Oversized, &path, tamper);
        see(Seen::Oversized {
            loaded,
            length: length(&path),
        })?;
    }
    std::fs::write(&path, []).unwrap();
    let records = vec![
        SessionRecord::decode(data).unwrap_or_default(),
        SessionRecord {
            request: Some(wire::ClientFrame {
                frame: Some(wire::client_frame::Frame::Acknowledge(wire::Acknowledge {
                    authority: None,
                    generation: String::from_utf8_lossy(data).into_owned(),
                    through_revision: u64::MAX,
                })),
            }),
            ..Default::default()
        },
    ];
    let mut ends = Vec::new();
    {
        let mut store = FileClientState::open(&path).unwrap();
        for record in &records {
            store.append(record).unwrap();
            ends.push(length(&path) as usize);
        }
        let recovered = load(&mut store, Stage::Committed, &path, tamper);
        see(Seen::Committed {
            records: records.clone(),
            recovered,
        })?;
    }
    let bytes = std::fs::read(&path).unwrap();
    let choice = data.iter().fold(0usize, |choice, &byte| {
        choice.wrapping_mul(257).wrapping_add(usize::from(byte))
    });
    // Boundaries are observed from the production writer, without duplicating
    // the framing, checksums or decoder's partial-record logic.
    for cut in [
        choice % (bytes.len() + 1),
        ends[0] - 1,
        ends[0],
        bytes.len() - 1,
        bytes.len(),
    ] {
        std::fs::write(&path, &bytes[..cut]).unwrap();
        let count = ends.iter().take_while(|&&end| end <= cut).count();
        let repaired_end = count.checked_sub(1).map_or(0, |index| ends[index]) as u64;
        let mut store = FileClientState::open(&path).unwrap();
        let first = load(&mut store, Stage::Cut, &path, tamper);
        let first = (first, length(&path));
        let second = store.load().map_err(|error| error.to_string());
        see(Seen::Cut {
            cut,
            expected: records[..count].to_vec(),
            loads: [first, (second, length(&path))],
            repaired_end,
        })?;
    }
    let mut corrupt = bytes;
    let offset = choice % corrupt.len();
    corrupt[offset] ^= 1;
    std::fs::write(&path, &corrupt).unwrap();
    let mut store = FileClientState::open(&path).unwrap();
    let loaded = load(&mut store, Stage::Corrupt, &path, tamper);
    see(Seen::Corrupt {
        loaded,
        untouched: std::fs::read(&path).unwrap() == corrupt,
    })
}
