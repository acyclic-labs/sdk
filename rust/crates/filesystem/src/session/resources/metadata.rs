use super::*;
use crate::VolumeConfig;
use crate::kernel::{FileKind, LogicalName, NameEncoding, NamespacePath};
use acyclic_auth::authority::PolicyDecision;
use acyclic_auth::contract::{Attribute, RecordAttributes};
use acyclic_stream::{AppendRecord, Record, wire};
use prost::Message;

pub(super) fn project(
    mut record: Record,
    volume: VolumeId,
    config: VolumeConfig,
    trie: &PathTrie,
    authorization: &acyclic_stream::session::ResourceAuthorization,
) -> Result<FollowUpdate, StreamError> {
    if !record
        .headers
        .attributes()
        .iter()
        .any(|a| a.name == "acyclic.record" && a.value.as_ref() == b"changes/v1")
    {
        return Err(StreamError::InvalidArgument);
    }
    let changes = wire::ChangeRecord::decode(record.value.as_ref())
        .map_err(|_| StreamError::InvalidArgument)?;
    if changes.context.len() != 56
        || changes.context.get(..16) != Some(volume.into_bytes().as_slice())
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
                name: a.name.clone(),
                value: a.value.to_vec(),
            })
            .collect(),
    )
    .map_err(|_| StreamError::InvalidArgument)?;
    let now = authorization.now_unix_seconds()?;
    let mut visible = Vec::with_capacity(changes.entries.len());
    for entry in changes.entries {
        let path = path(&entry, config)?;
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
        changes.context,
        visible,
        record
            .headers
            .attributes()
            .iter()
            .filter(|a| a.name != "acyclic.record")
            .cloned()
            .collect(),
    )?;
    record.value = projected.value;
    record.headers = projected.headers;
    Ok(FollowUpdate::Record(record))
}

fn take<'a>(bytes: &mut &'a [u8], count: usize) -> Result<&'a [u8], StreamError> {
    let (first, remaining) = bytes
        .split_at_checked(count)
        .ok_or(StreamError::InvalidArgument)?;
    *bytes = remaining;
    Ok(first)
}
fn number(bytes: &mut &[u8]) -> Result<u32, StreamError> {
    Ok(u32::from_le_bytes(
        take(bytes, 4)?
            .try_into()
            .map_err(|_| StreamError::InvalidArgument)?,
    ))
}
fn path(entry: &wire::ChangeEntry, config: VolumeConfig) -> Result<NamespacePath, StreamError> {
    let mut bytes = entry.metadata.as_ref();
    if entry.operation == "publish" && bytes.is_empty() && entry.key.is_empty() {
        return NamespacePath::new(vec![], config.limits).map_err(|_| StreamError::InvalidArgument);
    }
    if !matches!(entry.operation.as_str(), "create" | "remove" | "update") {
        return Err(StreamError::InvalidArgument);
    }
    let depth = number(&mut bytes)? as usize;
    if depth > config.limits.maximum_path_depth as usize || depth > bytes.len() / 6 {
        return Err(StreamError::InvalidArgument);
    }
    let mut components = Vec::with_capacity(depth);
    for _ in 0..depth {
        let tag = *take(&mut bytes, 1)?
            .first()
            .ok_or(StreamError::InvalidArgument)?;
        let encoding = NameEncoding::from_tag(tag).map_err(|_| StreamError::InvalidArgument)?;
        let length = number(&mut bytes)? as usize;
        if length > config.limits.maximum_component_bytes as usize {
            return Err(StreamError::InvalidArgument);
        }
        components.push(
            LogicalName::new(
                encoding,
                take(&mut bytes, length)?.to_vec(),
                config.limits.maximum_component_bytes,
            )
            .map_err(|_| StreamError::InvalidArgument)?,
        );
    }
    let mut present = [false; 2];
    for seen in &mut present {
        match take(&mut bytes, 1)? {
            [0] => {}
            [1] => {
                *seen = true;
                take(&mut bytes, 16)?;
                let tag = *take(&mut bytes, 1)?
                    .first()
                    .ok_or(StreamError::InvalidArgument)?;
                FileKind::from_tag(tag).map_err(|_| StreamError::InvalidArgument)?;
                take(&mut bytes, 32)?;
            }
            _ => return Err(StreamError::InvalidArgument),
        }
    }
    let expected = match entry.operation.as_str() {
        "create" => [false, true],
        "remove" => [true, false],
        _ => [true, true],
    };
    if !bytes.is_empty() || present != expected {
        return Err(StreamError::InvalidArgument);
    }
    NamespacePath::new(components, config.limits).map_err(|_| StreamError::InvalidArgument)
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

#[cfg(all(test, feature = "memory", not(acyclic_fs_loom)))]
mod tests {
    use super::*;
    #[test]
    fn long_full_metadata_paths_cannot_hide_behind_an_allowed_truncated_key() {
        let config = crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral);
        let components = vec!["component"; 130];
        let text = components.join("/");
        assert!(text.len() > acyclic_stream::MAX_RECORD_KEY_BYTES);
        let mut metadata = Vec::new();
        metadata.extend_from_slice(&u32::try_from(components.len()).unwrap().to_le_bytes());
        for component in components {
            metadata.push(1);
            metadata.extend_from_slice(&u32::try_from(component.len()).unwrap().to_le_bytes());
            metadata.extend_from_slice(component.as_bytes());
        }
        metadata.push(0);
        metadata.push(1);
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
        let full = path(&entry, config).unwrap();
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
        let appended = AppendRecord::changes(context.into(), vec![entry.clone()], vec![]).unwrap();
        let record = Record {
            sequence: 3,
            value: appended.value,
            headers: appended.headers,
            commit_id: acyclic_stream::CommitId::from_bytes([9; 32]),
            committed_at_micros: 0,
        };
        assert!(matches!(
            project(record.clone(), volume, config, &trie, &authorization).unwrap(),
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
            project(record, volume, config, &allowing, &authorization).unwrap(),
            FollowUpdate::Record(_)
        ));
        for cut in [0, 1, entry.metadata.len() - 1] {
            let mut malformed = entry.clone();
            malformed.metadata = malformed.metadata.slice(..cut);
            assert!(path(&malformed, config).is_err());
        }
    }
}
