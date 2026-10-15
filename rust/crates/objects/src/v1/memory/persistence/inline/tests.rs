use super::super::tests::{bucket, put, reopen, seeded};
use super::*;
use crate::v1::tests::get;

#[tokio::test]
async fn small_bodies_survive_compaction() -> Result<(), Box<dyn std::error::Error>> {
    for length in [0, 1, LIMIT, LIMIT + 1] {
        let root = tempfile::tempdir()?;
        let core = seeded(root.path()).await?;
        let bytes = OwnedBytes::from(vec![42; length]);
        let receipt = core.put(put("boundary"), bytes.clone()).await?;
        drop(core);
        let core = reopen(root.path())?;
        assert_eq!(
            core.get(get(bucket(), "boundary"), length as u64)
                .await?
                .into_body(),
            bytes
        );
        assert_eq!(core.put(put("boundary"), bytes.clone()).await?, receipt);
        core.collect_local_garbage(10)?;
        drop(core);
        let core = reopen(root.path())?;
        assert_eq!(
            core.get(get(bucket(), "boundary"), length as u64)
                .await?
                .into_body(),
            bytes
        );
        assert_eq!(core.put(put("boundary"), bytes).await?, receipt);
    }
    Ok(())
}

#[tokio::test]
async fn every_incomplete_inline_frame_boundary_recovers_the_previous_state()
-> Result<(), Box<dyn std::error::Error>> {
    let template = tempfile::tempdir()?;
    let core = seeded(template.path()).await?;
    let path = template.path().join("mutations.log");
    let before = fs::read(&path)?;
    core.put(put("new"), OwnedBytes::from_static(b"payload"))
        .await?;
    let after = fs::read(&path)?;
    let frame = after.get(before.len()..).ok_or("missing appended frame")?;
    drop(core);
    for cut in 0..=frame.len() {
        let root = tempfile::tempdir()?;
        let mut journal = before.clone();
        journal.extend_from_slice(frame.get(..cut).ok_or("invalid cut")?);
        fs::write(root.path().join("mutations.log"), journal)?;
        let core = reopen(root.path())?;
        assert_eq!(
            core.get(get(bucket(), "retained"), 6).await?.into_body(),
            OwnedBytes::from_static(b"before")
        );
        if cut == frame.len() {
            assert_eq!(
                core.get(get(bucket(), "new"), 7).await?.into_body(),
                OwnedBytes::from_static(b"payload")
            );
        } else {
            assert_eq!(
                core.get(get(bucket(), "new"), 7)
                    .await
                    .err()
                    .map(|error| error.code),
                Some(NotFound)
            );
            assert_eq!(fs::read(root.path().join("mutations.log"))?, before);
        }
    }
    Ok(())
}

#[tokio::test]
async fn checksum_valid_inline_offsets_still_require_authenticated_body_binding()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let core = seeded(root.path()).await?;
    let path = root.path().join("mutations.log");
    let mut before = fs::read(&path)?;
    core.put(put("new"), OwnedBytes::from_static(b"payload"))
        .await?;
    let all = fs::read(&path)?;
    drop(core);
    let encoded = all.get(before.len() + 36..).ok_or("missing frame")?;
    let mut delta = Delta::decode(encoded)?;
    delta
        .objects
        .first_mut()
        .and_then(|object| object.bodies.first_mut())
        .ok_or("missing inline reference")?
        .offset += 1;
    let encoded = delta.encode_to_vec();
    before.extend_from_slice(&u32::try_from(encoded.len())?.to_le_bytes());
    before.extend_from_slice(blake3::hash(&encoded).as_bytes());
    before.extend_from_slice(&encoded);
    fs::write(&path, &before)?;
    assert!(matches!(reopen(root.path()), Err(LocalOpenError::Corrupt)));
    assert_eq!(fs::read(path)?, before);
    Ok(())
}

#[tokio::test]
async fn durable_batch_reopens_values_and_original_overwritten_receipts()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let core = seeded(root.path()).await?;
    let mut requests = (0..9)
        .map(|number| {
            (
                put(&format!("batch-{number}")),
                OwnedBytes::from(vec![42; LIMIT]),
            )
        })
        .collect::<Vec<_>>();
    requests.push((put("overwritten"), OwnedBytes::from_static(b"first")));
    let mut replacement = put("overwritten");
    replacement.mutation = Some(wire::MutationIdentity {
        idempotency_key: "replacement".into(),
    });
    requests.push((replacement, OwnedBytes::from_static(b"last")));
    let results = core.put_batch(requests).await;
    assert!(results.iter().all(Result::is_ok));
    let original = results
        .get(9)
        .cloned()
        .ok_or("missing original receipt")??;
    // Puts whose bounded bytes exceed one record split across records.
    let results = core
        .put_batch(
            (0..40)
                .map(|number| {
                    (
                        put(&format!("split-{number}")),
                        OwnedBytes::from(vec![7; LIMIT]),
                    )
                })
                .collect(),
        )
        .await;
    assert!(results.iter().all(Result::is_ok));
    drop(core);
    let core = reopen(root.path())?;
    assert_eq!(
        core.get(get(bucket(), "batch-8"), LIMIT as u64)
            .await?
            .into_body(),
        OwnedBytes::from(vec![42; LIMIT])
    );
    assert_eq!(
        core.get(get(bucket(), "split-39"), LIMIT as u64)
            .await?
            .into_body(),
        OwnedBytes::from(vec![7; LIMIT])
    );
    assert_eq!(
        core.get(get(bucket(), "overwritten"), 4).await?.into_body(),
        OwnedBytes::from_static(b"last")
    );
    assert_eq!(
        core.put(put("overwritten"), OwnedBytes::from_static(b"first"))
            .await?,
        original
    );
    Ok(())
}

#[tokio::test]
async fn one_durable_batch_preserves_independent_validation_and_quota_failures()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let options = LocalObjectsLimits {
        maximum_object_bytes: 4,
        maximum_bytes: 20,
        ..Default::default()
    };
    let core = open(root.path().to_path_buf(), options, None)?;
    core.create_bucket(wire::CreateBucketRequest {
        name: "recovery".into(),
        mutation: None,
    })
    .await?;
    let mut invalid = put("invalid");
    invalid.object_key = "\0".into();
    let results = core
        .put_batch(vec![
            (put("first"), OwnedBytes::from_static(b"abc")),
            (put("too-large"), OwnedBytes::from_static(b"12345")),
            (invalid, OwnedBytes::new()),
            (put("last"), OwnedBytes::from_static(b"last")),
        ])
        .await;
    assert_eq!(
        results
            .first()
            .and_then(|value| value.as_ref().ok())
            .map(|value| value.size),
        Some(3)
    );
    assert_eq!(
        results
            .get(1)
            .and_then(|value| value.as_ref().err())
            .map(|error| error.code),
        Some(QuotaExceeded)
    );
    assert_eq!(
        results
            .get(2)
            .and_then(|value| value.as_ref().err())
            .map(|error| error.code),
        Some(InvalidArgument)
    );
    assert_eq!(
        results
            .get(3)
            .and_then(|value| value.as_ref().ok())
            .map(|value| value.size),
        Some(4)
    );
    drop(core);
    let core = open(root.path().to_path_buf(), options, None)?;
    assert_eq!(
        core.get(get(bucket(), "first"), 3).await?.into_body(),
        OwnedBytes::from_static(b"abc")
    );
    assert_eq!(
        core.get(get(bucket(), "too-large"), 5)
            .await
            .err()
            .map(|error| error.code),
        Some(NotFound)
    );
    assert_eq!(
        core.get(get(bucket(), "last"), 4).await?.into_body(),
        OwnedBytes::from_static(b"last")
    );
    Ok(())
}
