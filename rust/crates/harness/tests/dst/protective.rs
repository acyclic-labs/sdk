//! Protective limits: legitimate work of any size is chunked or paged and
//! never reaches a protective bound. Each world drives every size through the
//! codec or counter that owns its bound, so the proof is exhaustive over the
//! class's sizes and independent of a model turn's context.
use super::*;
use family::{Generator, Variant};

/// The protective bounds this class names, in seed order.
const BOUNDS: [&str; 7] = [
    "stream_frame",
    "content_bytes",
    "process_argv",
    "records_per_write",
    "first_byte_deadline",
    "output_reservation",
    "capacity_configuration",
];

/// The legitimate sizes this class names, in seed order.
const SIZES: [&str; 4] = ["below_chunk", "at_chunk", "multi_chunk", "very_large"];

/// One world per bound; every world drives every size.
pub(super) const GENERATOR: Generator = Generator {
    class: "J2",
    seeds: legitimate_work_never_reaches_a_protective_bound::VALUES,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = legitimate_work_never_reaches_a_protective_bound,
    values: u64,
    runner = super::run,
    cases = [
        seed_0 => 0, seed_1 => 1, seed_2 => 2, seed_3 => 3,
        seed_4 => 4, seed_5 => 5, seed_6 => 6,
    ],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    GENERATOR.run(seed)
}

fn bound(seed: u64) -> &'static str {
    BOUNDS
        .get(usize::try_from(seed).unwrap_or(0) % BOUNDS.len())
        .copied()
        .unwrap_or("stream_frame")
}

/// The stream frame is the smallest protective bound; every larger size is a
/// multiple of it, so a legitimate input that is chunked at the frame never
/// reaches the bound.
fn bytes(size: &str) -> usize {
    match size {
        "below_chunk" => 16 * 1024,
        "at_chunk" => acyclic_stream::MAX_RECORD_BYTES,
        "multi_chunk" => 3 * acyclic_stream::MAX_RECORD_BYTES,
        _ => 16 * acyclic_stream::MAX_RECORD_BYTES,
    }
}

fn variants(seed: u64) -> Vec<Variant> {
    SIZES
        .iter()
        .map(|size| Variant::from([("bound", bound(seed)), ("size", *size)]))
        .collect()
}

/// The content splitter is the codec that owns the stream-frame and
/// content-byte bounds: it splits a legitimate body into frame-sized parts
/// without losing a byte.
fn content_split(size: &str, bytes: usize) -> std::result::Result<(), String> {
    let body = "x".repeat(bytes);
    // Leave room for the record's JSON envelope inside the frame.
    let parts = acyclic_harness::model::ModelContent::bounded_text_parts(
        &body,
        Some((acyclic_stream::MAX_RECORD_BYTES - 1024) as u64),
    )
    .map_err(oracle::failed)?;
    let joined: String = parts
        .iter()
        .map(|part| match part {
            acyclic_harness::model::ModelContentPart::Text { text, .. } => text.as_str(),
            _ => "",
        })
        .collect();
    if joined != body {
        return Err(format!("{size} content splitter lost bytes"));
    }
    for part in &parts {
        let acyclic_harness::model::ModelContentPart::Text { text, .. } = part else {
            return Err(format!("{size} content splitter produced a non-text part"));
        };
        // The journal frame is the encoded record, not the raw text.
        let event = acyclic_harness::model::ModelEvent::TextPart {
            text: text.clone(),
            native: None,
        };
        let encoded = serde_json::to_vec(&event).map_err(oracle::failed)?;
        if encoded.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(format!("{size} encoded content record exceeded the frame"));
        }
    }
    Ok(())
}

/// The shell argument channel is the codec that owns the argv bound: a large
/// literal argument is preserved exactly.
fn argv_channel(size: &str, bytes: usize) -> std::result::Result<(), String> {
    use std::io::Write as _;
    use std::process::Stdio;
    // Large command content travels on stdin, never on argv. Launch the
    // selected platform shell for real and require it to receive every byte.
    let payload = "x".repeat(bytes);
    let (script, arguments) = if acyclic_harness_core::tools::shell::PLATFORM.windows_paths {
        (
            "$input | Measure-Object -Character | % Characters",
            vec!["-NoProfile", "-NonInteractive", "-Command"],
        )
    } else {
        ("wc -c", vec!["--noprofile", "--norc", "-c"])
    };
    // Resolve the host's supported shell through the same production fallback,
    // so a Windows host with only Windows PowerShell 5.1 uses `powershell.exe`.
    let shell = acyclic_native_runtime::platform_shell().map_err(oracle::failed)?;
    let mut argv = vec![shell.to_string_lossy().into_owned()];
    argv.extend(arguments.into_iter().map(str::to_owned));
    argv.push(script.to_owned());
    let command = acyclic_harness_core::tools::ShellCommand::Input {
        argv,
        stdin: payload.clone(),
    };
    let (program, arguments, stdin) = command
        .prepare(&acyclic_harness_core::tools::shell::PLATFORM)
        .map_err(oracle::failed)?;
    let program = program.ok_or_else(|| format!("{size} shell input lost its executable"))?;
    let stdin = stdin.ok_or_else(|| format!("{size} shell input lost its stdin"))?;
    let mut child = std::process::Command::new(program)
        .args(arguments)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .map_err(oracle::failed)?;
    child
        .stdin
        .take()
        .ok_or_else(|| format!("{size} shell child has no stdin"))?
        .write_all(&stdin)
        .map_err(oracle::failed)?;
    let output = child.wait_with_output().map_err(oracle::failed)?;
    if !output.status.success() {
        return Err(format!("{size} shell process failed: {:?}", output.status));
    }
    let count: usize = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .map_err(|_| format!("{size} shell did not report its received byte count"))?;
    if count != bytes {
        return Err(format!(
            "{size} shell received {count} bytes, expected {bytes}"
        ));
    }
    Ok(())
}

/// A batch of legitimate records is admitted whole; the journal pages it
/// rather than refusing the batch.
async fn record_batch(size: &str, bytes: usize) -> std::result::Result<(), String> {
    use acyclic_stream::{
        AppendRequest, IdempotencyKey, MemoryStream, OwnedBytes, ReadRequest, StreamPath,
        StreamProvider as _, StreamProviderExt as _,
    };
    use futures::TryStreamExt as _;
    // A batch at the record-count bound is written and read back whole.
    let provider = MemoryStream::default();
    let path = StreamPath::new(format!("j2/{size}")).map_err(oracle::failed)?;
    let count = acyclic_stream::MAX_ITEMS;
    let chunk = bytes.div_ceil(count).max(1);
    let records: Vec<_> = (0..count)
        .map(|_| OwnedBytes::from(vec![b'x'; chunk]).into())
        .collect();
    provider
        .append(AppendRequest {
            path: path.clone(),
            records,
            if_tail: Some(0),
            idempotency_key: IdempotencyKey::new(format!("j2-{size}").into_bytes())
                .map_err(oracle::failed)?,
        })
        .await
        .map_err(oracle::failed)?;
    let read = provider
        .read(ReadRequest {
            key: None,
            path,
            from: 0,
            limit: u32::try_from(count).map_err(oracle::failed)?,
            at_least: Default::default(),
        })
        .await
        .map_err(oracle::failed)?;
    let records: Vec<_> = read.try_collect().await.map_err(oracle::failed)?;
    if records.len() != count {
        return Err(format!(
            "{size} stream page returned {} of {count} records",
            records.len()
        ));
    }
    Ok(())
}

/// A large prompt is stored as an input file and read by the model; the
/// first-byte deadline is per request, not per prompt byte.
fn first_byte(size: &str, bytes: usize) -> std::result::Result<(), String> {
    let wait = acyclic_harness::model::ModelWait::Started {
        cause: None,
        delay: std::time::Duration::from_millis(0),
        retry_after: false,
    };
    let encoded = serde_json::to_vec(&wait).map_err(oracle::failed)?;
    let restored: acyclic_harness::model::ModelWait =
        serde_json::from_slice(&encoded).map_err(oracle::failed)?;
    if restored != wait {
        return Err(format!("{size} wait record lost its writer's value"));
    }
    content_split(size, bytes)
}

/// Output capacity never consumes the context reservation, and a large
/// configured window is admitted as configured.
fn capacity(bound: &str, size: &str) -> std::result::Result<(), String> {
    // The shared compaction policy owns the response reserve. A shared window
    // reserves only the observed response, never the published maximum.
    let compaction = acyclic_harness::context::CapacityCompaction::default();
    let (capacity, response) = match bound {
        "output_reservation" => (
            acyclic_harness::context::ModelContextCapacity {
                context_tokens: 131_072,
                output_tokens: Some(131_072),
                output_shares_context: true,
            },
            4_096,
        ),
        _ => (
            acyclic_harness::context::ModelContextCapacity {
                context_tokens: 1_048_576,
                output_tokens: Some(65_536),
                output_shares_context: false,
            },
            0,
        ),
    };
    let ceiling = compaction
        .input_ceiling(capacity, response)
        .map_err(oracle::failed)?;
    let expected = if capacity.output_shares_context {
        u64::from(capacity.context_tokens) - response
    } else {
        u64::from(capacity.context_tokens) - u64::from(capacity.output_tokens.unwrap_or(0))
    };
    if ceiling != expected {
        return Err(format!(
            "{size} capacity ceiling {ceiling} differs from {expected}"
        ));
    }
    Ok(())
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    let bound = bound(seed);
    oracle::simulate(seed, move || {
        entry(async move {
            for size in SIZES {
                let bytes = bytes(size);
                match bound {
                    "stream_frame" | "content_bytes" => content_split(size, bytes)?,
                    "process_argv" => argv_channel(size, bytes)?,
                    "records_per_write" => record_batch(size, bytes).await?,
                    "first_byte_deadline" => first_byte(size, bytes)?,
                    _ => capacity(bound, size)?,
                }
            }
            Ok(true)
        })
    })
}
