use super::*;
use crate::AgentId;
use acyclic_stream::{MemoryStream, StreamError};
use proptest::prelude::*;

use super::fixtures::{authority, volume};

#[tokio::test]
async fn opening_requires_the_exact_private_owner_and_both_volume_grants() {
    let agent = AgentId::from_bytes([1; 16]);
    let volume = volume(agent);
    let provider = Arc::new(MemoryStream::default());
    let path = StreamPath::new(format!(
        "harness/task-volumes/{}",
        volume.storage_name(&mut None).unwrap()
    ))
    .unwrap();
    let both = [VolumeOperation::Read, VolumeOperation::Write];
    for (owner, grants) in [
        (agent, &[VolumeOperation::Read][..]),
        (agent, &[VolumeOperation::Write][..]),
        (agent, &[][..]),
        (AgentId::from_bytes([3; 16]), &both[..]),
    ] {
        let (verifier, scope) = authority(Some(owner), &volume, grants);
        assert!(matches!(
            TaskFiles::open(provider.clone(), &volume, &verifier, &scope, 4096).await,
            Err(crate::Error::Unauthorized(_))
        ));
        assert!(matches!(
            provider.tail(path.clone()).await,
            Err(StreamError::NotFound)
        ));
    }
    let (verifier, scope) = authority(Some(agent), &volume, &both);
    assert!(
        TaskFiles::open(provider.clone(), &volume, &verifier, &scope, 4096)
            .await
            .is_ok()
    );
    assert_eq!(provider.tail(path).await.unwrap(), 1);
}

#[tokio::test]
async fn every_owner_scope_pair_is_checked_before_storage() {
    let agent = AgentId::from_bytes([1; 16]);
    let other = AgentId::from_bytes([2; 16]);
    let template = volume(agent);
    let both = [VolumeOperation::Read, VolumeOperation::Write];
    let mut broken_acceptances = 0;
    for owner in [
        VolumeOwner::Project("project".into()),
        VolumeOwner::Agent(agent),
        VolumeOwner::Session("session".into()),
    ] {
        for subject in [None, Some(agent), Some(other)] {
            let volume = VolumeRef::new(
                template.provider().clone(),
                "owner-table".into(),
                VolumeClass::AgentPrivate,
                owner.clone(),
                &mut None,
            )
            .unwrap();
            let (verifier, scope) = authority(subject, &volume, &both);
            let expected = owner == VolumeOwner::Agent(agent) && subject == Some(agent);
            // Negative control: the former grants-only boundary accepts all
            // six Project/Session cases. Signed grants do not prove ownership.
            let grants_only = both.into_iter().all(|operation| {
                ContentGrant::authorize_volume(
                    &verifier, &scope, &volume, operation, 4096, &mut None,
                )
                .is_ok()
            });
            broken_acceptances += usize::from(grants_only && !expected);
            let provider = Arc::new(MemoryStream::default());
            let path = StreamPath::new(format!(
                "harness/task-volumes/{}",
                volume.storage_name(&mut None).unwrap()
            ))
            .unwrap();
            let opened = TaskFiles::open(provider.clone(), &volume, &verifier, &scope, 4096).await;
            assert_eq!(opened.is_ok(), expected, "{owner:?}, {subject:?}");
            if expected {
                assert_eq!(provider.tail(path).await.unwrap(), 1);
            } else {
                assert!(matches!(opened, Err(crate::Error::Unauthorized(_))));
                assert!(matches!(
                    provider.tail(path).await,
                    Err(StreamError::NotFound)
                ));
            }
        }
    }
    assert_eq!(
        broken_acceptances, 6,
        "broken ownership control was not detected"
    );
}

fn files() -> Vec<(FileKey, bool)> {
    use crate::task_volume::{Channel, Component, InputPath, PlanName, TaskFile, ToolFile};
    let id = Component::new("id").unwrap();
    let mut files = vec![
        (FileKey::Goal, true),
        (FileKey::Plan(PlanName::new("id").unwrap()), true),
        (
            FileKey::Input(InputPath::try_from("workspace/note.txt".to_owned()).unwrap()),
            true,
        ),
        (FileKey::Agent(id.clone()), false),
    ];
    let task_files = [
        (TaskFile::Request(id.clone()), true),
        (TaskFile::Status, false),
        (TaskFile::TurnRequest(0), false),
        (TaskFile::Completion(0), false),
        (
            TaskFile::Chunk {
                turn: 0,
                sequence: 0,
            },
            false,
        ),
        (
            TaskFile::TextHead {
                turn: 0,
                channel: Channel::Text,
            },
            false,
        ),
        (
            TaskFile::TextHead {
                turn: 0,
                channel: Channel::Reasoning,
            },
            false,
        ),
        (TaskFile::ApprovalRequest(id.clone()), false),
        (
            TaskFile::ApprovalDecision {
                request: id.clone(),
                principal: id.clone(),
            },
            false,
        ),
        (TaskFile::Context(id.clone()), false),
        (TaskFile::Notification(id.clone()), false),
    ];
    files.extend(task_files.into_iter().map(|(file, allowed)| {
        (
            FileKey::Task {
                task: id.clone(),
                file,
            },
            allowed,
        )
    }));
    files.extend(
        [
            ToolFile::Request,
            ToolFile::Arguments(0),
            ToolFile::Stdout(0),
            ToolFile::Stderr(0),
            ToolFile::Result,
        ]
        .into_iter()
        .map(|file| {
            (
                FileKey::Task {
                    task: id.clone(),
                    file: TaskFile::Tool {
                        turn: 0,
                        call: id.clone(),
                        file,
                    },
                },
                false,
            )
        }),
    );
    files
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // The only accepted owner/scope state is Agent with exact identity equality.
    #[test]
    fn private_ownership_requires_exact_agent_identity(owner in any::<[u8;16]>(), subject in prop::option::of(any::<[u8;16]>())) {
        let agent = AgentId::from_bytes(owner);
        let template = volume(agent);
        for owner in [VolumeOwner::Project("project".into()), VolumeOwner::Agent(agent), VolumeOwner::Session("session".into())] {
            let volume = VolumeRef::new(template.provider().clone(), "owner-property".into(), VolumeClass::AgentPrivate, owner.clone(), &mut None).unwrap();
            let subject = subject.map(AgentId::from_bytes);
            prop_assert_eq!(owns_volume(&volume, subject), owner == VolumeOwner::Agent(agent) && subject == Some(agent));
        }
        // Always include the matching case, not just independently random IDs.
        prop_assert!(owns_volume(&template, Some(agent)));
    }

    // Exhaust every file role per generated byte/limit pair. Caller writes
    // cannot produce harness/approval records, even with a whole-volume grant.
    #[test]
    fn every_file_role_preserves_owner_separation_and_the_exact_byte_bound(bytes in any::<u64>(), maximum in any::<u64>()) {
        for (key, caller_owned) in files() {
            let result = authorize_write(&key, bytes, maximum);
            prop_assert_eq!(result.is_ok(), caller_owned && bytes <= maximum);
            if !caller_owned { prop_assert!(matches!(result, Err(Error::Unauthorized))); }
        }
    }
}
