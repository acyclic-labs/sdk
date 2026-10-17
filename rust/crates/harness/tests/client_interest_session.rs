//! Snapshot-plus-follow layout properties over the production Streams session.
use super::{path, runtime, status};
use acyclic_harness::{
    Error, Result,
    client::{SwarmChange, SwarmClientLimits},
    task_volume::{FileKey, Query, TaskVolume, Write},
};
use acyclic_stream::MemoryStream;
use futures::{StreamExt as _, channel::mpsc};
use std::sync::Arc;

async fn put(volume: &TaskVolume<MemoryStream>, path: &str, text: &str) -> Result<bool> {
    let before = volume
        .head()
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;
    let write = Write::new(
        FileKey::parse(path).map_err(|e| Error::Invalid(e.to_string()))?,
        text.as_bytes().to_vec(),
    );
    let after = volume
        .append(before.clone(), &write)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;
    Ok(after.position != before.position)
}

fn check_layout(value: &acyclic_harness::client::ClientInterests, visible: &[u8], nearby: &[u8]) {
    assert_eq!(
        super::ids(value.visible()),
        visible.iter().copied().collect()
    );
    assert_eq!(
        super::ids(value.full_fidelity()),
        visible.iter().chain(nearby).copied().collect()
    );
}

async fn check_snapshot_follow(
    old_visible: Vec<u8>,
    old_nearby: Vec<u8>,
    new_visible: Vec<u8>,
    new_nearby: Vec<u8>,
) {
    use acyclic_harness::client::InterestDocument;
    let provider = Arc::new(MemoryStream::default());
    let parent = TaskVolume::open(provider.clone(), path("swarm/files"))
        .await
        .unwrap();
    let config = TaskVolume::open(provider.clone(), path("client/files"))
        .await
        .unwrap();
    let mut agents = std::collections::BTreeMap::new();
    for id in 0..4 {
        status::status(&provider, &format!("swarm/{id}"), false).await;
        status::member(&parent, &format!("{id}")).await;
        let volume = TaskVolume::open(provider.clone(), path(&format!("swarm/{id}/files")))
            .await
            .unwrap();
        put(&volume, "GOAL.md", "").await.unwrap();
        agents.insert(path(&format!("swarm/{id}")), volume);
    }
    let document = |visible: &[u8], nearby: &[u8]| {
        serde_json::to_string(&InterestDocument {
            in_view: super::selected(visible).collect(),
            prefetch: super::selected(nearby).collect(),
        })
        .unwrap()
    };
    // Earlier layout must be superseded by the snapshot at C.
    put(&config, "workspace/layout.json", &document(&[], &[3]))
        .await
        .unwrap();
    put(
        &config,
        "workspace/layout.json",
        &document(&[], &old_nearby),
    )
    .await
    .unwrap();
    put(
        &config,
        "workspace/open-views.json",
        &document(&old_visible, &[]),
    )
    .await
    .unwrap();
    let client = super::client(SwarmClientLimits {
        agents: 4,
        selected: 4,
        layout_bytes: 4096,
        config_files: 10,
        snapshot_files: 10,
    })
    .unwrap();
    let views = client.views();
    let (session, mut runner, _) =
        runtime::open(provider.clone(), Arc::new(|s| s), Arc::new(|s| s));
    let (layouts, mut rendered) = mpsc::unbounded();
    let consumer = client.follow(
        &config,
        &parent,
        &session,
        |agent, maximum| agents[&agent].heads(&Query::Root, None, maximum),
        async |update| {
            if let SwarmChange::Layout = update {
                layouts
                    .unbounded_send(views.read(|v| v.layout().clone()).unwrap())
                    .unwrap();
            }
            Ok(())
        },
    );
    let writer = async {
        let mut current = rendered.next().await.unwrap();
        check_layout(&current, &old_visible, &old_nearby);
        if put(
            &config,
            "workspace/layout.json",
            &document(&[], &new_nearby),
        )
        .await
        .unwrap()
        {
            current = rendered.next().await.unwrap();
        }
        check_layout(&current, &old_visible, &new_nearby);
        if put(
            &config,
            "workspace/open-views.json",
            &document(&new_visible, &[]),
        )
        .await
        .unwrap()
        {
            current = rendered.next().await.unwrap();
        }
        check_layout(&current, &new_visible, &new_nearby);
    };
    let mut scenario = Box::pin(futures::future::select(
        Box::pin(consumer),
        Box::pin(writer),
    ));
    assert!(matches!(
        runner.until_idle(scenario.as_mut()).await,
        Some(futures::future::Either::Right(((), _)))
    ));
}

proptest::proptest! {
    // Property: a snapshot at C plus each accepted file change after C equals
    // folding the latest layout/open-view files from the complete write history.
    #[test]
    fn file_snapshot_plus_follow_equals_the_complete_fold(
        old_visible in proptest::collection::vec(0u8..4, 0..8),
        old_nearby in proptest::collection::vec(0u8..4, 0..8),
        new_visible in proptest::collection::vec(0u8..4, 0..8),
        new_nearby in proptest::collection::vec(0u8..4, 0..8),
    ) {
        futures::executor::block_on(check_snapshot_follow(old_visible, old_nearby, new_visible, new_nearby));
    }

    #[test]
    fn json_path_validation_equals_platform_validation(value in ".{0,100}") {
        let bytes = serde_json::to_vec(&serde_json::json!({"in_view": [&value]})).unwrap();
        let decoded = acyclic_harness::client::InterestDocument::decode(&bytes, bytes.len());
        proptest::prop_assert_eq!(decoded.is_ok(), acyclic_stream::StreamPath::new(&value).is_ok());
        if let Ok(document) = decoded {
            proptest::prop_assert_eq!(document.in_view[0].as_str(), value.as_str());
        }
    }

    #[test]
    fn layout_documents_round_trip_and_preserve_all_selections(
        visible in proptest::collection::vec(0u8..32, 0..32),
        nearby in proptest::collection::vec(0u8..32, 0..32),
    ) {
        let document = acyclic_harness::client::InterestDocument {
            in_view: super::selected(&visible).collect(),
            prefetch: super::selected(&nearby).collect(),
        };
        let bytes = serde_json::to_vec(&document).unwrap();
        let decoded = acyclic_harness::client::InterestDocument::decode(&bytes, bytes.len()).unwrap();
        proptest::prop_assert_eq!(decoded, document);
        proptest::prop_assert!(acyclic_harness::client::InterestDocument::decode(&bytes, bytes.len() - 1).is_err());
    }
}
