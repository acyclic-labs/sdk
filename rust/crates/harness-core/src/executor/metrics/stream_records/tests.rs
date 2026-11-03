use super::super::digest;
use super::*;
use acyclic_stream::{IdempotencyKey, MemoryStream, Stream};
use arrow_array::{Array, StringArray, UInt64Array};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::TryStreamExt;
use std::sync::Arc;

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

async fn first(stream: &Stream<MemoryStream>) -> Result<Record> {
    stream
        .read(0, 1)
        .await
        .map_err(export_error)?
        .try_next()
        .await
        .map_err(export_error)?
        .ok_or_else(|| export_error("original test record is absent"))
}

#[tokio::test]
async fn real_forks_keep_one_logical_identity_without_claiming_physical_savings() -> Result<()> {
    let account = AccountId::new(b"original-account\0\xff").map_err(export_error)?;
    let provider = Arc::new(MemoryStream::default());
    let parent = Stream::new(provider.clone(), "source").map_err(export_error)?;
    parent
        .append("same value", IdempotencyKey::unique())
        .await
        .map_err(export_error)?;
    let original = first(&parent).await?;
    let local = project("agents/source", &account, parent.path(), &original)?;
    assert_eq!(text(&local, "kind"), "stream_record");
    assert_eq!(
        URL_SAFE_NO_PAD
            .decode(text(&local, "source_account"))
            .map_err(export_error)?,
        account.as_bytes()
    );
    assert_eq!(original.sequence, 0);
    // The growing union belongs to this offline query, never the projection.
    let mut unique = std::collections::HashSet::from([text(&local, "source").to_owned()]);
    for destination in ["left", "right"] {
        let child = parent
            .fork(destination, Some(1), Vec::new(), IdempotencyKey::unique())
            .await
            .map_err(export_error)?;
        let shared = first(&child).await?;
        let row = project("agents/consumer", &account, child.path(), &shared)?;
        assert_eq!(text(&row, "kind"), "stream_record_shared");
        assert_eq!(text(&row, "source"), text(&local, "source"));
        unique.insert(text(&row, "source").to_owned());
        assert_eq!(text(&row, "source_stream"), destination);
        assert_eq!(text(&row, "source_origin"), parent.path().as_str());
        assert_eq!(
            text(&row, "source_commit_id"),
            digest(*original.commit_id.as_bytes())
        );
        assert_eq!(text(&row, "coverage"), "logical_record_value_bytes");
        assert_eq!(
            row.data
                .column_by_name("logical_value_bytes")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            original.value.len() as u64
        );
        for unknown in [
            "physical_copied_bytes",
            "physical_shared_bytes",
            "operation_id",
        ] {
            assert!(row.data.column_by_name(unknown).unwrap().is_null(0));
        }
        let foreign = AccountId::new(b"other-account").map_err(export_error)?;
        let foreign = project("agents/consumer", &foreign, child.path(), &shared)?;
        assert_ne!(text(&row, "source"), text(&foreign, "source"));
    }
    let unrelated = Stream::new(provider, "independent").map_err(export_error)?;
    unrelated
        .append("same value", IdempotencyKey::unique())
        .await
        .map_err(export_error)?;
    let separate = project(
        "agents/other",
        &account,
        unrelated.path(),
        &first(&unrelated).await?,
    )?;
    assert_ne!(text(&separate, "source"), text(&local, "source"));
    unique.insert(text(&separate, "source").to_owned());
    assert_eq!(unique.len(), 2);
    Ok(())
}

#[tokio::test]
async fn projection_allocation_cost_is_independent_of_record_value_size() -> Result<()> {
    let account = AccountId::new(b"original-account").map_err(export_error)?;
    let stream =
        Stream::new(Arc::new(MemoryStream::default()), "payloads").map_err(export_error)?;
    let mut allocations = None;
    for (sequence, length) in [0, 1, acyclic_stream::MAX_RECORD_BYTES]
        .into_iter()
        .enumerate()
    {
        stream
            .append(
                bytes::Bytes::from(vec![7; length]),
                IdempotencyKey::unique(),
            )
            .await
            .map_err(export_error)?;
        let record = stream
            .read(sequence as u64, 1)
            .await
            .map_err(export_error)?
            .try_next()
            .await
            .map_err(export_error)?
            .ok_or_else(|| export_error("original allocation control record is absent"))?;
        let observed = project("agents/consumer", &account, stream.path(), &record)?;
        assert_eq!(
            observed
                .data
                .column_by_name("logical_value_bytes")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            length as u64
        );
        #[cfg(feature = "provenance-export")]
        {
            let mut encoded = Vec::new();
            super::super::provenance::write(&observed, &mut encoded)?;
            let triples = oxttl::NTriplesParser::new()
                .for_reader(encoded.as_slice())
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(export_error)?;
            assert!(triples.iter().any(|triple|
                triple.predicate.as_str() == "urn:acyclic:research:sourceOrigin"
                && matches!(&triple.object, oxrdf::Term::Literal(value) if value.value() == stream.path().as_str())));
            assert!(triples.iter().any(|triple|
                triple.predicate.as_str() == "urn:acyclic:research:logicalValueBytes"
                && matches!(&triple.object, oxrdf::Term::Literal(value) if value.value() == length.to_string())));
            assert!(
                !triples.iter().any(|triple| triple.predicate.as_str()
                    == "http://www.w3.org/ns/prov#wasGeneratedBy")
            );
        }
        // Warm the shared schema before observing caller-requested projection.
        let _ = project("agents/consumer", &account, stream.path(), &record)?;
        let measured = acyclic_test_support::measure(|| {
            std::hint::black_box(
                project("agents/consumer", &account, stream.path(), &record).unwrap(),
            );
        });
        let cost = (measured.count_total, measured.bytes_total);
        if let Some(previous) = allocations {
            assert_eq!(cost, previous);
        }
        allocations = Some(cost);
        // Copying the value is the deliberately growing negative control.
        if length != 0 {
            let copied = acyclic_test_support::measure(|| {
                std::hint::black_box(record.value.to_vec());
            });
            assert!(copied.bytes_total >= length as u64);
        }
    }
    Ok(())
}
