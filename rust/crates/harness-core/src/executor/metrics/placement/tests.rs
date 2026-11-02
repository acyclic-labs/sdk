use super::*;
use crate::{
    agent::AgentId,
    machines::MACHINE_BINDING_VERSION,
    resources::{ProviderRef, SandboxRef},
};
use acyclic_stream::{IdempotencyKey, MemoryStream, Stream};
use arrow_array::{Array, StringArray, UInt64Array};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::TryStreamExt as _;
use std::sync::Arc;

fn binding(namespace: &str) -> Result<MachineBinding> {
    Ok(MachineBinding {
        version: MACHINE_BINDING_VERSION,
        target: "compute".into(),
        machine: SandboxRef::new(
            ProviderRef::new(namespace.into(), "machines".into(), "2".into(), &mut None)?,
            vec![0, 255, 1],
            None,
        )?,
        forked_from: Some(acyclic_machines::MachineId::from_digest([2; 32])),
    })
}

fn text<'a>(batch: &'a Batch, name: &str) -> &'a str {
    batch
        .data
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
}

fn number(batch: &Batch, name: &str) -> u64 {
    batch
        .data
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0)
}

async fn at(stream: &Stream<MemoryStream>, from: u64) -> Result<Record> {
    stream
        .read(from, 1)
        .await
        .map_err(export_error)?
        .try_next()
        .await
        .map_err(export_error)?
        .ok_or_else(|| export_error("original placement control record is absent"))
}

#[tokio::test]
async fn original_binding_preserves_authority_and_refuses_foreign_sources() -> Result<()> {
    let agent = AgentPath::root(&AgentId::from_bytes([1; 16]));
    let account = AccountId::new(b"original\0\xff").map_err(export_error)?;
    let provider = Arc::new(MemoryStream::default());
    let source = Stream::new(
        provider,
        agent.machine("compute").map_err(export_error)?.as_str(),
    )
    .map_err(export_error)?;
    let original = binding("original.example")?;
    // These are SDK-format values committed by the real stream implementation,
    // not observations of a machine provider. Even a valid value at position 0
    // must not be mistaken for the settled binding.
    let value = serde_json::to_vec(&original).map_err(export_error)?;
    for _ in 0..2 {
        source
            .append(bytes::Bytes::from(value.clone()), IdempotencyKey::unique())
            .await
            .map_err(export_error)?;
    }
    let record = at(&source, 1).await?;
    let row = project(&agent, &account, source.path(), &record)?;
    assert_eq!(row.table, Table::Agents);
    assert_eq!(text(&row, "kind"), "placement");
    let retained: SandboxRef = serde_json::from_str(text(&row, "machine")).map_err(export_error)?;
    assert_eq!(retained, original.machine);
    assert_eq!(retained.as_resource().key(), [0, 255, 1]);
    let retained_provider: ProviderRef =
        serde_json::from_str(text(&row, "provider")).map_err(export_error)?;
    assert_eq!(
        &retained_provider,
        original.machine.as_resource().provider()
    );
    assert_eq!(
        text(&row, "forked_from"),
        original.forked_from.unwrap().to_string()
    );
    assert_eq!(text(&row, "coverage"), "logical_provider_binding");
    assert_eq!(number(&row, "sequence"), record.sequence);
    assert_eq!(
        number(&row, "source_committed_at_micros"),
        record.committed_at_micros
    );
    assert_eq!(text(&row, "source_origin"), record.origin.as_str());
    assert_eq!(
        text(&row, "source_commit_id"),
        super::super::digest(*record.commit_id.as_bytes())
    );
    assert_eq!(
        URL_SAFE_NO_PAD
            .decode(text(&row, "source_account"))
            .map_err(export_error)?,
        account.as_bytes()
    );
    for unknown in [
        "operation_id",
        "physical_host",
        "region",
        "core",
        "numa",
        "memory_bytes",
    ] {
        assert!(row.data.column_by_name(unknown).unwrap().is_null(0));
    }
    let other = AgentPath::root(&AgentId::from_bytes([3; 16]));
    let shared = source
        .fork(
            other.machine("compute").map_err(export_error)?.as_str(),
            Some(2),
            Vec::new(),
            IdempotencyKey::unique(),
        )
        .await
        .map_err(export_error)?;
    let inherited = at(&shared, 1).await?;
    assert_eq!(inherited, record);
    let child = project(&other, &account, shared.path(), &inherited)?;
    assert_eq!(text(&child, "source_origin"), record.origin.as_str());
    assert_eq!(text(&child, "source_stream"), shared.path().as_str());
    assert_eq!(
        text(&child, "source_commit_id"),
        text(&row, "source_commit_id")
    );
    assert!(project(&other, &account, source.path(), &record).is_err());
    assert!(
        project(
            &agent,
            &account,
            &agent.machine("foreign").map_err(export_error)?,
            &record
        )
        .is_err()
    );
    assert!(project(&agent, &account, source.path(), &at(&source, 0).await?).is_err());
    let mut invalid = record.clone();
    invalid.sequence = 2;
    assert!(project(&agent, &account, source.path(), &invalid).is_err());
    invalid = record.clone();
    let mut stale = original;
    stale.version = 1;
    invalid.value = serde_json::to_vec(&stale).map_err(export_error)?.into();
    assert!(project(&agent, &account, source.path(), &invalid).is_err());
    invalid.value = bytes::Bytes::from_static(b"{}").into();
    assert!(project(&agent, &account, source.path(), &invalid).is_err());
    // Same opaque key under another provider is a different logical authority.
    let replaced = binding("replaced.example")?;
    invalid.value = serde_json::to_vec(&replaced).map_err(export_error)?.into();
    let changed = project(&agent, &account, source.path(), &invalid)?;
    assert_ne!(text(&changed, "machine"), text(&row, "machine"));
    assert_ne!(text(&changed, "provider"), text(&row, "provider"));
    assert_eq!(
        text(&row, "provider"),
        serde_json::to_string(&retained_provider).map_err(export_error)?
    );

    #[cfg(feature = "provenance-export")]
    {
        let mut output = Vec::new();
        super::super::provenance::write(&row, &mut output)?;
        let triples = oxttl::NTriplesParser::new()
            .for_reader(output.as_slice())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(export_error)?;
        for (predicate, expected) in [
            ("providerReference", text(&row, "provider").to_owned()),
            ("machineReference", text(&row, "machine").to_owned()),
            ("sourceOrigin", record.origin.as_str().to_owned()),
            (
                "sourceCommittedAtMicros",
                record.committed_at_micros.to_string(),
            ),
            ("coverage", "logical_provider_binding".into()),
        ] {
            assert!(triples.iter().any(|triple| triple.predicate.as_str() == format!("urn:acyclic:research:{predicate}")
                && matches!(&triple.object, oxrdf::Term::Literal(value) if value.value() == expected)));
        }
        assert!(
            !triples
                .iter()
                .any(|triple| triple.predicate.as_str()
                    == "http://www.w3.org/ns/prov#wasGeneratedBy")
        );
    }
    Ok(())
}

#[tokio::test]
async fn placement_projection_allocation_does_not_grow_with_stream_history() -> Result<()> {
    let agent = AgentPath::root(&AgentId::from_bytes([1; 16]));
    let account = AccountId::new(b"original").map_err(export_error)?;
    let stream = Stream::new(
        Arc::new(MemoryStream::default()),
        agent.machine("compute").map_err(export_error)?.as_str(),
    )
    .map_err(export_error)?;
    let value = bytes::Bytes::from(
        serde_json::to_vec(&binding("original.example")?).map_err(export_error)?,
    );
    let mut previous = 0;
    let mut cost = None;
    let mut retained_history = None;
    for length in [2, 65] {
        for _ in previous..length {
            stream
                .append(value.clone(), IdempotencyKey::unique())
                .await
                .map_err(export_error)?;
        }
        previous = length;
        let record = at(&stream, 1).await?;
        let _ = project(&agent, &account, stream.path(), &record)?;
        let measured = acyclic_test_support::measure(|| {
            std::hint::black_box(project(&agent, &account, stream.path(), &record).unwrap());
        });
        let current = (measured.count_total, measured.bytes_total);
        if let Some(prior) = cost {
            assert_eq!(current, prior);
        }
        cost = Some(current);
        // Deliberately retaining history is the growing negative control.
        let growing = acyclic_test_support::measure(|| {
            std::hint::black_box(vec![record.clone(); length]);
        });
        if let Some(prior) = retained_history {
            assert!(growing.bytes_total > prior);
        }
        retained_history = Some(growing.bytes_total);
    }
    Ok(())
}
