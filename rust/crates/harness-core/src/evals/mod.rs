//! Evaluation files are pure, lazy projections of the original settled turns.
//! No evaluation database or credential input exists at this boundary.
use crate::agent::{ConversationEntry, TurnInput, TurnOutcome};
use crate::conversation::FileRef;
use acyclic_fs::{demand::*, kernel::*};
use acyclic_stream::{Record, StreamPath};
use serde::Serialize;
use std::{io::Write, marker::PhantomData};
#[cfg(not(target_arch = "wasm32"))]
mod stream;
#[cfg(not(target_arch = "wasm32"))]
pub use stream::{Snapshot, StreamProvider};

/// Exact original turn inputs resident for one filesystem demand. Implementors
/// retain their authenticated record owners and load only the demanded page.
pub trait Inputs: Send + Sync + 'static {
    /// The original conversation stream, including inherited records.
    fn stream(&self) -> &StreamPath;
    /// Exclusive conversation tail at the selected immutable cut.
    fn tail(&self) -> u64;
    /// One already-resident original record and its decoded turn.
    fn entry(&self, sequence: u64) -> Result<(&Record, &ConversationEntry), DemandError>;
}

/// A case's selected context is the prefix before its decision was admitted.
/// Source addresses convey no authority; export must use the current reader.
#[derive(Serialize)]
struct Cut<'a> {
    stream: &'a str,
    through: u64,
}

/// The version-one export schema borrows only committed product data. Host
/// credential/configuration objects cannot be passed into this serializer.
#[derive(Serialize)]
struct Case<'a> {
    version: u32,
    cut: Cut<'a>,
    source: Source<'a>,
    decision: Decision<'a>,
    outcome: Outcome<'a>,
}
#[derive(Serialize)]
struct Source<'a> {
    origin: &'a str,
    sequence: u64,
    commit: &'a [u8],
}
#[derive(Serialize)]
struct Decision<'a> {
    inbox: u64,
    input: &'a TurnInput,
    steering: &'a Option<crate::agent::SteeringTranscript>,
}
#[derive(Serialize)]
struct Outcome<'a> {
    status: &'a TurnOutcome,
    output: &'a FileRef,
    tool_results: Option<&'a str>,
}

/// The same case JSON is read as a file or copied by an authorized scope export.
/// The complete selected record is bounded by the stream envelope; file bodies
/// remain original references and are read through existing content authority.
pub fn write_case(
    inputs: &impl Inputs,
    sequence: u64,
    sink: &mut impl Write,
) -> Result<(), DemandError> {
    if sequence >= inputs.tail() {
        return Err(DemandError::Absent);
    }
    let (record, entry) = inputs.entry(sequence)?;
    if record.sequence != sequence {
        return Err(DemandError::StaleSource);
    }
    serde_json::to_writer(
        sink,
        &Case {
            version: 1,
            cut: Cut {
                stream: inputs.stream().as_str(),
                through: sequence,
            },
            source: Source {
                origin: record.origin.as_str(),
                sequence,
                commit: record.commit_id.as_bytes(),
            },
            decision: Decision {
                inbox: entry.inbox,
                input: &entry.input,
                steering: &entry.steering,
            },
            outcome: Outcome {
                status: &entry.outcome,
                output: &entry.output,
                tool_results: entry.tool_results.as_deref(),
            },
        },
    )
    .map_err(|error| DemandError::Io(error.into()))
}

/// Filesystem representation composed with the existing `Computed` adapter.
/// Construction does no IO and allocates no per-agent index or background task.
pub struct View<C>(PhantomData<fn() -> C>);
impl<C: Inputs> Default for View<C> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

fn sequence(
    path: &NamespacePath,
    ctx: &mut KernelAccounting<'_>,
) -> Result<Option<u64>, DemandError> {
    let text = path.unicode_text(ctx)?;
    if text == "/" {
        return Ok(None);
    }
    let name = text
        .strip_prefix('/')
        .and_then(|name| name.strip_suffix(".json"))
        .filter(|name| name.len() == 20 && name.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or(DemandError::Absent)?;
    Ok(Some(name.parse().map_err(|_| DemandError::Absent)?))
}

fn node(length: Option<u64>, sequence: Option<u64>) -> SourceNode {
    let mut identity = [0; 32];
    if let Some(sequence) = sequence {
        identity[..8].copy_from_slice(&sequence.to_le_bytes());
        identity[8] = 1;
    }
    SourceNode {
        kind: if sequence.is_some() {
            SourceNodeKind::RegularFile
        } else {
            SourceNodeKind::Directory
        },
        logical_bytes: length,
        file_identity: identity,
        link_count: Some(1),
        device: None,
        version: SourceVersion([0; 32]),
        metadata: SourceMetadata::default(),
    }
}

struct Range<'a, W> {
    sink: &'a mut W,
    position: u64,
    offset: u64,
    length: u64,
}
impl<W: Write> Write for Range<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let start = self
            .offset
            .saturating_sub(self.position)
            .min(bytes.len() as u64) as usize;
        let end = self
            .offset
            .saturating_add(self.length)
            .saturating_sub(self.position)
            .min(bytes.len() as u64) as usize;
        self.sink.write_all(&bytes[start..end.max(start)])?;
        self.position = self
            .position
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| std::io::Error::other("case length overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.sink.flush()
    }
}

impl<C: Inputs> acyclic_fs::demand::View for View<C> {
    type Snapshot = C;
    type Record = ();
    fn identity(&self) -> [u8; 32] {
        *b"acyclic:evals:original-cases:v1\0"
    }
    fn lookup(
        &self,
        _: ViewCut,
        inputs: &C,
        path: &NamespacePath,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<SourceNode>, DemandError> {
        let selected = match sequence(path, ctx) {
            Ok(selected) => selected,
            Err(DemandError::Absent) => return Ok(None),
            Err(error) => return Err(error),
        };
        let Some(selected) = selected else {
            return Ok(Some(node(None, None)));
        };
        if selected >= inputs.tail() {
            return Ok(None);
        }
        let mut range = Range {
            sink: &mut std::io::sink(),
            position: 0,
            offset: 0,
            length: 0,
        };
        write_case(inputs, selected, &mut range)?;
        Ok(Some(node(Some(range.position), Some(selected))))
    }
    fn nodes(
        &self,
        _: ViewCut,
        inputs: &C,
        path: &NamespacePath,
        cursor: Option<SourceCursor>,
        maximum_entries: u32,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<SourceDirectoryPage, DemandError> {
        if sequence(path, ctx)?.is_some() {
            return Err(DemandError::NotDirectory);
        }
        let start = cursor.as_ref().map_or(0, SourceCursor::offset);
        if start > inputs.tail() {
            return Err(DemandError::InvalidRequest);
        }
        let end = start
            .saturating_add(u64::from(maximum_entries))
            .min(inputs.tail());
        let mut page = SourceDirectoryPageBuilder::new(ctx)?;
        for selected in start..end {
            let name = OwnedSourceChunk::produce(ctx, |sink| {
                write!(sink, "{selected:020}.json").map_err(DemandError::from)
            })?;
            let name = std::str::from_utf8(&name.bytes).map_err(|_| DemandError::InvalidRequest)?;
            page.push_utf8(name, node(None, Some(selected)), ctx)?;
        }
        page.finish(
            (end < inputs.tail()).then(|| SourceCursor::position(end)),
            SourceVersion([0; 32]),
            ctx,
        )
    }
    fn bytes(
        &self,
        _: ViewCut,
        inputs: &C,
        path: &NamespacePath,
        offset: u64,
        length: u64,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<OwnedSourceChunk, DemandError> {
        let selected = sequence(path, ctx)?.ok_or(DemandError::NotRegularFile)?;
        OwnedSourceChunk::produce(ctx, |sink| {
            write_case(
                inputs,
                selected,
                &mut Range {
                    sink,
                    position: 0,
                    offset,
                    length,
                },
            )
        })
    }
    fn affected(
        &self,
        _: &(),
        sink: &dyn SourceChangeSink,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        let cancellation = ctx.cancellation();
        let (root, mut allocation) = NamespacePath::parse_in_profile(
            "/",
            acyclic_fs::FilesystemProfile::Portable,
            acyclic_fs::VolumeLimits::default(),
            ctx,
            cancellation,
        )?;
        let bytes = root.logical_owned_bytes();
        let changes = [SourceChange::Entry(root)];
        let result = sink.source_changed(&changes, ctx);
        drop(changes);
        ctx.release_owned(&mut allocation, bytes)?;
        result
    }
}

#[cfg(all(test, feature = "test-support", not(target_arch = "wasm32")))]
mod tests;
