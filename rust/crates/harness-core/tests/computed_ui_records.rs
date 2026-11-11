//! Original-accounting UI record reads through real Streams.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_fs::{
    CancellationToken, WorkBudget, WorkCounters,
    kernel::{KernelAccounting, MetadataDecodeError, NativeAccounting, RetainedAllocation},
};
use acyclic_harness_core::agent::{
    AgentRecord, Error as AgentError, Phase, RECORD_VERSION, StatusRecord,
};
use acyclic_stream::{
    AppendRequest, HeadProjection, HeadsRequest, IdempotencyKey, MemoryStream,
    NativeStreamProvider, RevisionCut, StreamPath, StreamProviderExt as _,
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn accounting(cancellation: &CancellationToken, budget: WorkBudget) -> KernelAccounting<'_> {
    KernelAccounting::Standalone {
        allocations: Default::default(),
        work: WorkCounters::UNCHARGED,
        budget,
        cancellation,
    }
}

fn status(path: &str) -> StatusRecord {
    StatusRecord {
        phase: Phase::WaitingUser { path: path.into() },
        cursor: 3,
        conversation: 2,
        child_wakes: Vec::new(),
    }
}

#[tokio::test]
async fn native_status_projection_preserves_original_credit_and_cancellation() -> Result {
    let expected = status("reply/étude.md");
    let stream = MemoryStream::default();
    let path = StreamPath::new("computed/status")?;
    stream
        .append(AppendRequest {
            path: path.clone(),
            records: vec![expected.encode()?.into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    let cancellation = CancellationToken::new();
    let mut caller = accounting(&cancellation, WorkBudget::UNBOUNDED);
    drop(RetainedAllocation::claim(0, 0, &mut caller)?);
    let before = caller.remaining()?.peak_allocation_bytes;
    let owner = NativeAccounting::take(&mut caller)?;
    assert!(caller.remaining().is_err(), "caller must remain fenced");
    let output = stream
        .native_heads(
            HeadsRequest {
                at: None,
                retained: Default::default(),
                durable_through: None,
                projection: HeadProjection::History,
                paths: [path].into(),
                children: None,
                limit: 1,
                at_least: RevisionCut::default(),
            },
            owner.work.runtime_meter(),
        )
        .await??;
    let record = output.value().heads[0]
        .as_ref()
        .ok_or("head")?
        .last
        .as_ref()
        .ok_or("record")?;
    let decoded = StatusRecord::decode_accounted(
        &record.value,
        &mut KernelAccounting::NativeOriginal {
            work: &owner.work,
            cancellation: &cancellation,
        },
    )?;
    assert_eq!(*decoded, expected);
    owner.restore(&mut caller)?;
    drop(output);
    assert!(caller.remaining()?.peak_allocation_bytes < before);
    drop(decoded);
    assert_eq!(caller.remaining()?.peak_allocation_bytes, before);
    let held = StatusRecord::decode_accounted(&expected.encode()?, &mut caller)?;
    // Cancellation refuses further work, while destroying retained fields
    // still releases their original ownership without another admission.
    cancellation.cancel();
    drop(held);
    assert!(matches!(
        caller.remaining(),
        Err(acyclic_fs::FsError::Cancelled(_))
    ));
    Ok(())
}

#[test]
fn accounted_agent_records_share_version_shape_and_whole_input_validation() -> Result {
    let cancellation = CancellationToken::new();
    let mut caller = accounting(&cancellation, WorkBudget::UNBOUNDED);
    let expected = status("ask.md");
    let bytes = expected.encode()?;
    assert_eq!(
        *StatusRecord::decode_accounted(&bytes, &mut caller)?,
        expected
    );
    // Every trailing marker is refused, including an incomplete integer whose
    // payload is EOF. It cannot be confused with EOF at the next marker.
    for byte in 0..=u8::MAX {
        let mut trailing = bytes.clone();
        trailing.push(byte);
        assert!(StatusRecord::decode(&trailing).is_err());
        assert!(matches!(
            StatusRecord::decode_accounted(&trailing, &mut caller),
            Err(MetadataDecodeError::Codec(AgentError::Record(_)))
        ));
    }
    let mut foreign = (RECORD_VERSION + 1).to_be_bytes().to_vec();
    foreign.push(0xc1); // Malformed payload; version must still win.
    assert!(matches!(
        StatusRecord::decode(&foreign),
        Err(AgentError::UnsupportedRecordVersion(_))
    ));
    assert!(matches!(
        StatusRecord::decode_accounted(&foreign, &mut caller),
        Err(MetadataDecodeError::Codec(
            AgentError::UnsupportedRecordVersion(_)
        ))
    ));
    let malformed = rmp_serde::to_vec_named(&status("../escape.md"))?;
    let mut invalid = RECORD_VERSION.to_be_bytes().to_vec();
    invalid.extend(malformed);
    assert!(StatusRecord::decode(&invalid).is_err());
    assert!(StatusRecord::decode_accounted(&invalid, &mut caller).is_err());
    let mut refused = accounting(
        &cancellation,
        WorkBudget {
            peak_allocation_bytes: 0,
            ..WorkBudget::UNBOUNDED
        },
    );
    assert!(matches!(
        StatusRecord::decode_accounted(&bytes, &mut refused),
        Err(MetadataDecodeError::Work(_))
    ));
    // Dynamic serde values also own their strings; the shared any-value
    // visitor must fund them, just like a statically typed record field.
    let text = "dynamic ".repeat(1024);
    let dynamic = rmp_serde::to_vec(&serde_json::json!({"text": text}))?;
    drop(RetainedAllocation::claim(0, 0, &mut caller)?);
    let baseline = caller.remaining()?.peak_allocation_bytes;
    let mut decoder =
        rmp_serde::Deserializer::from_read_ref(dynamic.as_slice()).with_human_readable();
    let decoded = caller.deserialize::<serde_json::Value, _>(&mut decoder)?;
    assert_eq!(decoded["text"].as_str(), Some(text.as_str()));
    assert!(baseline - caller.remaining()?.peak_allocation_bytes >= text.len() as u64);
    drop(decoded);
    assert_eq!(caller.remaining()?.peak_allocation_bytes, baseline);
    Ok(())
}
