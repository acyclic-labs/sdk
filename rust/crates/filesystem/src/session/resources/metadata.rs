use super::*;
use crate::VolumeConfig;
use crate::kernel::{NameEncoding, NamespacePath};
use acyclic_auth::authority::PolicyDecision;
use acyclic_auth::contract::{Attribute, RecordAttributes};
use acyclic_stream::{AppendRecord, Record};

pub(super) fn project(
    mut record: Record,
    volume: VolumeId,
    scope: [u8; 16],
    config: VolumeConfig,
    trie: &PathTrie,
    authorization: &acyclic_stream::session::ResourceAuthorization,
) -> Result<FollowUpdate, StreamError> {
    let changes = acyclic_stream::decode_changes(record.value.as_ref())?;
    if !acyclic_stream::is_changes(&record.headers)
        || changes.context.len() != 56
        || !appended_by_context(&record, &changes.context, scope)
        || changes.entries.len() != record.headers.keys().len()
        || changes
            .entries
            .iter()
            .zip(record.headers.keys())
            .any(|(entry, key)| entry.key.as_ref() != key.as_ref())
    {
        return Err(StreamError::InvalidArgument);
    }
    let attributes = RecordAttributes::try_new(
        record
            .headers
            .attributes()
            .iter()
            .map(|a| Attribute {
                name: a.name.to_owned(),
                value: a.value.to_vec(),
            })
            .collect(),
    )
    .map_err(|_| StreamError::InvalidArgument)?;
    let now = authorization.now_unix_seconds()?;
    let mut visible = Vec::with_capacity(changes.entries.len());
    for entry in changes.entries {
        let path = crate::publication_changes::entry::path(
            &entry.key,
            &entry.operation,
            &entry.metadata,
            config,
        )
        .map_err(|_| StreamError::InvalidArgument)?;
        // The lossless full path is authoritative. A 1-KiB indexed key must
        // never authorize a longer path or an alternate native representation.
        let prepared = trie
            .prepare_namespace(&path)
            .map_err(|_| StreamError::AccessDenied)?;
        if !prepared.decide(Action::List, now).allowed {
            continue;
        }
        let name = canonical_resource(volume, &path, config)?;
        if authorization
            .authorize(
                &name,
                &[entry.key.to_vec()],
                &attributes,
                PolicyDecision::Allow,
            )
            .is_ok()
        {
            visible.push(entry);
        }
    }
    if visible.is_empty() {
        return Ok(FollowUpdate::Progress {
            next: record
                .sequence
                .checked_add(1)
                .ok_or(StreamError::OutOfRange)?,
            preceding: record.commit_id,
        });
    }
    let projected = AppendRecord::changes(
        &changes.context,
        &visible,
        record
            .headers
            .attributes()
            .iter()
            .filter(|a| a.name != "acyclic.record")
            .map(acyclic_stream::RecordAttribute::to_owned)
            .collect(),
    )?;
    record.value = projected.value;
    record.headers = projected.headers;
    Ok(FollowUpdate::Record(record))
}

/// A record's context names the volume whose changes stream appended it.
/// That is this volume's, or for a forked volume an ancestor's whose changes
/// it shares; Streams, not the record, names the appending stream.
fn appended_by_context(record: &Record, context: &[u8], scope: [u8; 16]) -> bool {
    let Some(appender) = context
        .get(..16)
        .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
    else {
        return false;
    };
    let mut text = [0; crate::publication_changes::CHANGES_PATH_LEN];
    crate::publication_changes::changes_path_text(scope, VolumeId::from_bytes(appender), &mut text)
        .is_some_and(|path| path == record.origin.as_str())
}

fn canonical_resource(
    volume: VolumeId,
    path: &NamespacePath,
    config: VolumeConfig,
) -> Result<String, StreamError> {
    let mut result = resource(volume, "")?;
    for name in path.components() {
        let mut text = String::new();
        name.append_unicode(&mut text)
            .map_err(|_| StreamError::AccessDenied)?;
        let text = if config.case_sensitivity == crate::model::CaseSensitivity::ProfileFolded {
            NameEncoding::native_for_profile(config.profile).unicode_case_fold_key(&text)
        } else {
            std::borrow::Cow::Borrowed(text.as_str())
        };
        result.push('/');
        result.push_str(&acyclic_auth::encode_component(text.as_bytes()));
    }
    Ok(result)
}

#[cfg(all(test, feature = "memory", not(loom)))]
mod tests {
    use super::*;

    fn changes_stream(scope: [u8; 16], volume: VolumeId) -> acyclic_stream::StreamPath {
        let mut text = [0; crate::publication_changes::CHANGES_PATH_LEN];
        let text = crate::publication_changes::changes_path_text(scope, volume, &mut text).unwrap();
        acyclic_stream::StreamPath::new(text.to_owned()).unwrap()
    }
    use acyclic_stream::wire;
    #[test]
    fn long_full_metadata_paths_cannot_hide_behind_an_allowed_truncated_key() {
        let config = crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral);
        let components = vec!["component"; 130];
        let text = components.join("/");
        assert!(text.len() > acyclic_stream::MAX_RECORD_KEY_BYTES);
        let mut metadata = Vec::new();
        prost::encoding::encode_varint(components.len() as u64 + 1, &mut metadata);
        for component in components {
            metadata.push(1);
            prost::encoding::encode_varint(component.len() as u64, &mut metadata);
            metadata.extend_from_slice(component.as_bytes());
        }
        metadata.extend_from_slice(&[7; 16]);
        metadata.push(1);
        metadata.extend_from_slice(&[8; 32]);
        let entry = wire::ChangeEntry {
            key: bytes::Bytes::copy_from_slice(
                &text.as_bytes()[..acyclic_stream::MAX_RECORD_KEY_BYTES],
            ),
            operation: "create".into(),
            metadata: metadata.into(),
        };
        let full = crate::publication_changes::entry::path(
            &entry.key,
            &entry.operation,
            &entry.metadata,
            config,
        )
        .unwrap();
        assert_eq!(full.depth(), 130);
        let volume = VolumeId::from_bytes([13; 16]);
        let full_resource = canonical_resource(volume, &full, config).unwrap();
        assert!(full_resource.ends_with(&text));
        assert!(full_resource.len() > entry.key.len());
        let policy = crate::access::fixtures::denied(Action::List, &text);
        let principal = crate::access::Principal {
            kind: crate::access::PrincipalKind::Anonymous,
            id: String::new(),
            lineage: vec![],
            groups: Default::default(),
            roles: Default::default(),
            tags: Default::default(),
            attributes: Default::default(),
        };
        let capability = crate::access::Capability {
            layers: vec![vec![crate::access::Grant {
                actions: [Action::List].into_iter().collect(),
                resource: crate::access::ResourcePattern {
                    lineage_root: None,
                    volume: None,
                    generations: crate::access::Generations::All,
                    path: "**".into(),
                },
            }]],
            expires_at: None,
        };
        let trie = PathTrie::compile(
            &policy,
            &principal,
            crate::access::Resource {
                config,
                lineage: &[],
                volume: "",
                generation: 1,
                head: 1,
                path: "",
            },
            crate::access::Context {
                now: 10,
                approvals: 0,
            },
            &capability,
        )
        .unwrap();
        assert!(
            !trie
                .prepare_namespace(&full)
                .unwrap()
                .decide(Action::List, 10)
                .allowed
        );
        // The deliberately truncated decision is the vulnerability's negative control.
        let clipped = std::str::from_utf8(entry.key.as_ref())
            .unwrap()
            .trim_end_matches('/');
        assert!(
            trie.prepare(clipped)
                .unwrap()
                .decide(Action::List, 10)
                .allowed
        );
        let name = resource(volume, "").unwrap();
        let encoded = acyclic_auth::testing::root(|block| {
            block.certificates.as_mut().unwrap().keys[0].key.grants =
                acyclic_auth::contract::GrantSet {
                    grants: vec![acyclic_auth::contract::Grant {
                        kind: acyclic_auth::contract::GrantKind::Resource(
                            acyclic_auth::contract::ResourceGrant {
                                resource: acyclic_auth::contract::ResourcePattern {
                                    name: name.clone(),
                                    prefix: true,
                                },
                                actions: vec!["fs/list".into()],
                            },
                        ),
                    }],
                };
        });
        let authorization =
            acyclic_stream::session::testing::resource_authorization(&encoded, &name).unwrap();
        let mut context = volume.into_bytes().to_vec();
        context.extend_from_slice(&[8; 32]);
        context.extend_from_slice(&1u64.to_le_bytes());
        let appended =
            AppendRecord::changes(&context, std::slice::from_ref(&entry), vec![]).unwrap();
        let record = Record {
            sequence: 3,
            value: appended.value,
            headers: appended.headers,
            commit_id: acyclic_stream::CommitId::from_bytes([9; 32]),
            committed_at_micros: 0,
            origin: changes_stream([0; 16], volume),
        };
        // The context must name the volume whose changes stream appended the
        // record, which Streams reports: another stream cannot claim it.
        for origin in [
            changes_stream([0; 16], VolumeId::from_bytes([7; 16])),
            changes_stream([1; 16], volume),
        ] {
            let mut forged = record.clone();
            forged.origin = origin;
            assert!(matches!(
                project(forged, volume, [0; 16], config, &trie, &authorization),
                Err(StreamError::InvalidArgument)
            ));
        }
        assert!(matches!(
            project(
                record.clone(),
                volume,
                [0; 16],
                config,
                &trie,
                &authorization
            )
            .unwrap(),
            FollowUpdate::Progress { next: 4, .. }
        ));
        let allowing: crate::access::Policy =
            serde_json::from_slice(crate::access::INITIAL_POLICY).unwrap();
        let allowing = PathTrie::compile(
            &allowing,
            &principal,
            crate::access::Resource {
                config,
                lineage: &[],
                volume: "",
                generation: 1,
                head: 1,
                path: "",
            },
            crate::access::Context {
                now: 10,
                approvals: 0,
            },
            &capability,
        )
        .unwrap();
        // The same record and credential are disclosed once the exact owner deny is removed.
        assert!(matches!(
            project(record, volume, [0; 16], config, &allowing, &authorization).unwrap(),
            FollowUpdate::Record(_)
        ));
        for cut in [0, 1, entry.metadata.len() - 1] {
            let mut malformed = entry.clone();
            malformed.metadata = malformed.metadata.slice(..cut);
            assert!(
                crate::publication_changes::entry::path(
                    &malformed.key,
                    &malformed.operation,
                    &malformed.metadata,
                    config
                )
                .is_err()
            );
        }
    }
}
