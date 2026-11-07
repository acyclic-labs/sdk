//! One indexed candidate selection and its lossless namespace boundary.
use crate::kernel::NamespacePath;
use crate::{VolumeConfig, VolumeId};
use acyclic_stream::{FollowFilter, StreamError, wire};

pub(super) struct Selection {
    resource: String,
    filter: FollowFilter,
}

impl Selection {
    pub(super) fn new(
        volume: VolumeId,
        path: &NamespacePath,
        config: VolumeConfig,
    ) -> Result<Self, StreamError> {
        let resource = super::metadata::canonical_resource(volume, path, config)?;
        let key = crate::publication_changes::entry::key(path, config);
        let key = key.slice();
        let filter = if path.is_root() {
            FollowFilter::default()
        } else {
            let mut ranges = Vec::with_capacity(2);
            if key.len() == acyclic_stream::MAX_RECORD_KEY_BYTES {
                // The original producer truncates the index, not the facts.
                // Select this whole candidate bucket and check the full path.
                let mut end = key.to_vec();
                while end.last() == Some(&255) {
                    end.pop();
                }
                if let Some(byte) = end.last_mut() {
                    *byte += 1;
                }
                ranges.push(wire::KeyRange {
                    start: bytes::Bytes::copy_from_slice(key),
                    end: end.into(),
                });
            } else {
                let mut exact_end = key.to_vec();
                exact_end.push(0);
                ranges.push(wire::KeyRange {
                    start: bytes::Bytes::copy_from_slice(key),
                    end: exact_end.into(),
                });
                let mut start = key.to_vec();
                start.push(b'/');
                let mut end = key.to_vec();
                end.push(b'0');
                ranges.push(wire::KeyRange {
                    start: start.into(),
                    end: end.into(),
                });
            }
            FollowFilter::new(wire::SessionFilter {
                keys: ranges,
                ..Default::default()
            })?
        };
        Ok(Self { resource, filter })
    }

    pub(super) fn filter(&self) -> &FollowFilter {
        &self.filter
    }

    pub(super) fn contains(&self, resource: &str) -> bool {
        resource
            .strip_prefix(self.resource.as_str())
            .is_some_and(|suffix| suffix.is_empty() || suffix.starts_with('/'))
    }
}

#[cfg(all(test, feature = "memory", not(loom)))]
mod tests {
    use super::*;
    use crate::session::ChangeProvider;
    use acyclic_stream::session::{ResourceFollowRequest, ResourceProvider};
    use acyclic_stream::{FollowUpdate, MemoryStream, StreamProviderExt};
    use futures::StreamExt;
    use std::sync::Arc;

    fn credential(resource: &str) -> Vec<u8> {
        use acyclic_auth::contract::*;
        acyclic_auth::testing::root(|block| {
            block.certificates.as_mut().unwrap().keys[0].key.grants = GrantSet {
                grants: vec![Grant {
                    kind: GrantKind::Resource(ResourceGrant {
                        resource: ResourcePattern {
                            name: resource.to_owned(),
                            prefix: true,
                        },
                        actions: vec!["fs/list".into()],
                    }),
                }],
            };
        })
    }

    #[test]
    fn volume_changes_subtree_boundaries_use_full_paths_after_index_selection() {
        let volume = VolumeId::from_bytes([17; 16]);
        for folded in [false, true] {
            let mut config = VolumeConfig::portable(crate::Lifecycle::Ephemeral);
            if folded {
                config.case_sensitivity = crate::model::CaseSensitivity::ProfileFolded;
            }
            let path = |text: &str| {
                NamespacePath::from_text_in_profile(text, config.profile, config.limits).unwrap()
            };
            let selected = Selection::new(volume, &path("/watched"), config).unwrap();
            for (text, expected) in [
                ("/watched", true),
                ("/watched/nested", true),
                ("/watched-other", false),
                ("/watch", false),
                ("/WATCHED/nested", folded),
            ] {
                let path = path(text);
                let resource =
                    super::super::metadata::canonical_resource(volume, &path, config).unwrap();
                let key = crate::publication_changes::entry::key(&path, config);
                let headers = acyclic_stream::RecordHeaders::new(
                    vec![bytes::Bytes::copy_from_slice(key.slice()).into()],
                    vec![],
                )
                .unwrap();
                assert_eq!(selected.filter().matches(&headers), expected);
                let measured = acyclic_test_support::measure(|| {
                    assert_eq!(selected.contains(&resource), expected);
                });
                assert_eq!(measured.count_total, 0);
                assert_eq!(measured.bytes_total, 0);
            }
            let common = vec!["component"; 130].join("/");
            let selected_path = path(&format!("/{common}/a"));
            let outside_path = path(&format!("/{common}/b"));
            let selected = Selection::new(volume, &selected_path, config).unwrap();
            let key = crate::publication_changes::entry::key(&outside_path, config);
            let headers = acyclic_stream::RecordHeaders::new(
                vec![bytes::Bytes::copy_from_slice(key.slice()).into()],
                vec![],
            )
            .unwrap();
            assert!(
                selected.filter().matches(&headers),
                "the index is truncated"
            );
            let outside =
                super::super::metadata::canonical_resource(volume, &outside_path, config).unwrap();
            assert!(
                !selected.contains(&outside),
                "negative control: the indexed key alone selects the wrong subtree"
            );
        }
    }

    #[tokio::test]
    async fn volume_changes_subtree_retains_commit_revision_replay_and_revocation()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::{CancellationToken, EmbeddedCapabilities, WorkBudget};
        let streams = Arc::new(MemoryStream::default());
        let fs = crate::Fs::new(
            crate::distributed::StreamAuthorityStore::new(streams.clone()),
            crate::MemoryObjectStore::default(),
            EmbeddedCapabilities::MEMORY,
        );
        let cancellation = CancellationToken::new();
        let workspace = fs
            .create_workspace("subtree-changes", WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value;
        crate::access::fixtures::publish_policy(
            &workspace,
            &serde_json::from_slice(crate::access::INITIAL_POLICY)?,
        )
        .await?;
        let mut transaction = workspace
            .begin_transaction(
                crate::IdempotencyKey::new(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        transaction
            .create_directory("/watched", WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?;
        transaction
            .commit(
                None,
                crate::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &cancellation,
                |_| async {
                    Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(
                        crate::OperationReceipt {
                            value: (),
                            work: crate::WorkCounters::UNCHARGED,
                        },
                    )
                },
            )
            .await
            .map_err(|failure| failure.error)?;
        let volume = workspace.id().volume_id();
        let resource = crate::session::resource(volume, "")?;
        let parent_credential = credential(&resource);
        let request = |from, preceding| -> Result<_, StreamError> {
            Ok(ResourceFollowRequest {
                resource: resource.clone(),
                from,
                preceding,
                filter: FollowFilter::default(),
                authorization: acyclic_stream::session::testing::resource_authorization(
                    &parent_credential,
                    &resource,
                )?,
            })
        };
        let path = || {
            NamespacePath::from_text_in_profile(
                "/watched",
                workspace.profile(),
                workspace.volume_config().limits,
            )
        };
        let changes = crate::session::changes_path(&fs, volume)?;
        let from = streams.tail(changes).await?;
        let provider = ChangeProvider::new(fs, streams)?;
        let mut requested = request(from, None)?;
        requested.filter = FollowFilter::new(wire::SessionFilter {
            keys: vec![wire::KeyRange {
                start: bytes::Bytes::from_static(b"watch"),
                end: bytes::Bytes::from_static(b"watci"),
            }],
            ..Default::default()
        })?;
        let subscription = provider.follow_subtree(requested, path()?)?;
        let broad = provider.follow(request(from, None)?)?;
        let mut selected = subscription.open.await?;
        let mut broad = broad.open.await?;
        assert!(matches!(
            selected.next().await.ok_or("missing opening")??,
            FollowUpdate::Opening { .. }
        ));
        assert!(matches!(
            broad.next().await.ok_or("missing broad opening")??,
            FollowUpdate::Opening { .. }
        ));
        workspace.write_text("/watched-other", "outside").await?;
        assert!(matches!(
            selected.next().await.ok_or("missing skipped progress")??,
            FollowUpdate::Progress { .. }
        ));
        assert!(matches!(
            broad
                .next()
                .await
                .ok_or("negative control lost outside record")??,
            FollowUpdate::Record(_)
        ));
        let published = workspace.write_text("/watched/file", "inside").await?;
        let crate::workspace::TransactionCommit::Committed(generation) = published else {
            return Err("publication did not commit".into());
        };
        let FollowUpdate::Record(record) = selected.next().await.ok_or("missing change")?? else {
            return Err("selected change was skipped".into());
        };
        eprintln!("subtree publication Changes={}", hex::encode(&record.value));
        let changes = acyclic_stream::decode_changes(record.value.as_ref())?;
        let context = crate::PublicationContext::from_bytes(changes.context.as_ref())?;
        assert_eq!(context.generation, generation.id());
        assert_eq!(context.volume, volume);
        assert_eq!(context.exclusive_end(record.sequence)?, record.sequence + 1);
        assert!(
            changes
                .entries
                .iter()
                .any(|entry| entry.key.as_ref() == b"watched/file")
        );
        for entry in &changes.entries {
            let facts = crate::PublicationEntry::from_parts(
                &entry.key,
                &entry.operation,
                &entry.metadata,
                workspace.volume_config(),
            )?;
            assert!(facts.path.is_within(&path()?));
        }
        let replay = provider.follow_subtree(request(record.sequence, None)?, path()?)?;
        let mut replay = replay.open.await?;
        assert!(matches!(
            replay.next().await.ok_or("missing replay opening")??,
            FollowUpdate::Opening { .. }
        ));
        let FollowUpdate::Record(replayed) = replay.next().await.ok_or("missing replay")?? else {
            return Err("replay skipped its original record".into());
        };
        assert_eq!(replayed, record);
        for index in 0..3 {
            let child = workspace
                .fork::<_, _, crate::WorkspaceError>(
                    format!("subtree-child-{index}"),
                    crate::ForkOptions::from_generation(
                        generation.clone(),
                        crate::IdempotencyKey::new(),
                    ),
                    crate::PublicationPermit::Unrestricted,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                    &mut |_| {
                        std::future::ready(Ok(crate::OperationReceipt {
                            value: (),
                            work: crate::WorkCounters::UNCHARGED,
                        }))
                    },
                )
                .await
                .map_err(|failure| failure.error)?
                .value;
            let child_resource = crate::session::resource(child.id().volume_id(), "")?;
            let child_credential = credential(&child_resource);
            let inherited = provider.follow_subtree(
                ResourceFollowRequest {
                    resource: child_resource.clone(),
                    from: record.sequence,
                    preceding: None,
                    filter: FollowFilter::default(),
                    authorization: acyclic_stream::session::testing::resource_authorization(
                        &child_credential,
                        &child_resource,
                    )?,
                },
                path()?,
            )?;
            let mut inherited = inherited.open.await?;
            assert!(matches!(
                inherited.next().await.ok_or("missing fork opening")??,
                FollowUpdate::Opening { .. }
            ));
            let FollowUpdate::Record(shared) =
                inherited.next().await.ok_or("missing inherited change")??
            else {
                return Err("inherited publication was skipped".into());
            };
            assert_eq!(shared, record, "fork changed the original source evidence");
            assert_ne!(child.id().volume_id(), context.volume);
        }
        let resumed = provider.follow_subtree(
            request(record.sequence + 1, Some(record.commit_id))?,
            path()?,
        )?;
        let revalidate = resumed.revalidate;
        let mut terminal = resumed.terminal;
        let mut resumed = resumed.open.await?;
        assert!(matches!(
            resumed.next().await.ok_or("missing resume opening")??,
            FollowUpdate::Opening { .. }
        ));
        assert!(futures::poll!(resumed.next()).is_pending());
        workspace.remove("/watched/file").await?;
        let FollowUpdate::Record(removed) = resumed.next().await.ok_or("missing removal")?? else {
            return Err("removal was skipped".into());
        };
        let changes = acyclic_stream::decode_changes(removed.value.as_ref())?;
        let entry = changes
            .entries
            .iter()
            .find(|entry| entry.key.as_ref() == b"watched/file")
            .ok_or("missing removal entry")?;
        let facts = crate::PublicationEntry::from_parts(
            &entry.key,
            &entry.operation,
            &entry.metadata,
            workspace.volume_config(),
        )?;
        assert!(facts.before_present && !facts.after_present);
        assert!(futures::poll!(terminal.as_mut()).is_pending());
        crate::access::fixtures::publish_policy(
            &workspace,
            &crate::access::fixtures::denied(crate::access::Action::List, "watched"),
        )
        .await?;
        assert!(matches!(revalidate(), Err(StreamError::AccessDenied)));
        assert!(matches!(
            futures::poll!(terminal.as_mut()),
            std::task::Poll::Ready(StreamError::AccessDenied)
        ));
        Ok(())
    }
}
