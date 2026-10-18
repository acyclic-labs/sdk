use super::*;
use acyclic_test_support::differential;
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;

#[path = "binding_original.rs"]
mod original;
type Verifier = FilesystemContentVerifier<MemoryAuthorityBackend, MemoryObjectBackend>;
type State = (Result<MemoryContentPublisher>, Arc<Verifier>);
type Output = (
    std::result::Result<(Vec<FileRef>, Vec<Vec<u8>>), String>,
    acyclic_test_support::AllocationInfo,
);
const KEY: &str = "00000000-0000-0000-0000-000000000021";

async fn initialize(old: bool, denied: bool, unbound: bool, reference: bool) -> State {
    let agent = AgentId::from_bytes([7; 16]);
    let (mut host, volume, memory_store) = if old {
        original::content(KEY.to_owned(), agent, 4096, Fs::memory())
            .await
            .unwrap()
    } else {
        MEMORY_VOLUME_KEY.set(Some(KEY));
        new_memory_content(agent, 4096, Fs::memory()).await.unwrap()
    };
    if reference {
        // Independently bind the original fixture at its declared volume. This
        // does not call the candidate's memory-content constructor, and owns a
        // separate filesystem, proof cache, stream and publisher.
        let workspace =
            super::super::fork_preparer_io::content_workspace(&host.provider, &volume, None)
                .unwrap();
        let opened = host.open(&workspace, None).await.unwrap();
        host = host.for_workspace(&volume, opened).unwrap();
    }
    if !old && unbound {
        // Restore the removed opens through the actual original host path.
        host = FilesystemHost::new(host.filesystem.clone(), &host.provider, None).unwrap();
    }
    let issuer = AuthorityIssuer::new(
        "binding-test",
        [8; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: ConversationId::from_bytes([9; 16]).to_string(),
        },
    );
    let mut capabilities = vec![volume.capability(VolumeOperation::Read, &mut None).unwrap()];
    if !denied {
        capabilities.push(
            volume
                .capability(VolumeOperation::Write, &mut None)
                .unwrap(),
        );
    }
    let scope = issuer
        .issue(
            "owner",
            &Capabilities::new(capabilities),
            Some(agent),
            None,
            &mut None,
        )
        .unwrap();
    let verifier = issuer.verifier(&mut None).unwrap();
    let input =
        FilesystemContentVerifier::new(host.clone(), &verifier, &scope, 4096, None).unwrap();
    let publisher = FilesystemContentPublisher::new(host, &volume, &verifier, &scope, 4096, None)
        .map(|files| MemoryContentPublisher {
            files,
            memory_store,
        });
    (publisher, input)
}

async fn exercise(
    publisher: &MemoryContentPublisher,
    verifier: &Verifier,
    bytes: &[Vec<u8>; 2],
) -> Result<(Vec<FileRef>, Vec<Vec<u8>>)> {
    let first = publisher
        .stage(
            OperationId::from_bytes([1; 16]),
            "batch/content.bin",
            &bytes[0],
            "application/octet-stream",
            "content.bin",
            None,
        )?
        .await?;
    let second = publisher
        .stage(
            OperationId::from_bytes([2; 16]),
            "batch/content.bin",
            &bytes[1],
            "application/octet-stream",
            "content.bin",
            None,
        )?
        .await?;
    // Retain both holds until the authenticated historical reads complete.
    let a = verifier.read(&first.0, 4096, None)?.await?.to_vec();
    let b = verifier.read(&second.0, 4096, None)?.await?.to_vec();
    Ok((vec![first.0, second.0], vec![a, b]))
}

fn pair(
    bytes: &[Vec<u8>; 2],
    denied: bool,
    unbound: bool,
) -> [(Output, Vec<(&'static str, u64)>); 2] {
    paired(bytes, denied, unbound, false)
}

fn paired(
    bytes: &[Vec<u8>; 2],
    denied: bool,
    unbound: bool,
    reference: bool,
) -> [(Output, Vec<(&'static str, u64)>); 2] {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            MEMORY_VOLUME_KEY.set(None);
            crate::executor::json::baseline::LEGACY.set(false);
        }
    }
    let _reset = Reset;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut old = true;
    differential::pair::<_, _, (&'static str, u64)>(
        || {
            crate::executor::json::baseline::LEGACY.set(old && !reference);
            let state = runtime.block_on(initialize(old, denied, unbound, old && reference));
            old = false;
            state
        },
        |(publisher, verifier), old| {
            crate::executor::json::baseline::LEGACY.set(old && !reference);
            let mut result = None;
            let allocations = acyclic_test_support::measure(|| {
                if let Ok(publisher) = &publisher {
                    result = Some(runtime.block_on(exercise(publisher, &verifier, bytes)));
                }
            });
            let result = match (publisher, result) {
                (Err(error), None) | (Ok(_), Some(Err(error))) => Err(format!("{error:?}")),
                (Ok(_), Some(Ok(result))) => Ok(result),
                _ => unreachable!("only successful publishers execute"),
            };
            (result, allocations)
        },
    )
}

fn check(bytes: &[Vec<u8>; 2], denied: bool) -> TestCaseResult {
    let [(old, old_trace), (new, new_trace)] = pair(bytes, denied, false);
    let semantic = |trace: Vec<(&'static str, u64)>| {
        trace
            .into_iter()
            .filter(|(operation, _)| {
                matches!(
                    *operation,
                    "content stage" | "content read" | "read" | "retain" | "verify"
                )
            })
            .collect::<Vec<_>>()
    };
    let input_matches = if denied {
        new.0.is_err()
    } else {
        new.0
            .as_ref()
            .is_ok_and(|(_, read)| read.as_slice() == bytes.as_slice())
    };
    let opens = |trace: &Vec<(&'static str, u64)>| {
        trace
            .iter()
            .filter(|(op, _)| *op == "open workspace")
            .count()
    };
    let work_matches = denied || (opens(&old_trace) > 0 && opens(&new_trace) == 0);
    prop_assert!(
        old.0 == new.0
            && work_matches
            && semantic(old_trace) == semantic(new_trace)
            && input_matches,
        "independent workspace binding changes references, bytes, errors or operation trace"
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn bound_workspace_keeps_complete_publication_references_and_authenticated_reads(
        bytes in prop::array::uniform2(prop::collection::vec(any::<u8>(), 0..32)),
    ) {
        for denied in [false, true] { check(&bytes, denied)?; }
        exact_for(&bytes, false)?;
    }
}

fn exact(unbound: bool) -> TestCaseResult {
    let bytes = [b"first".to_vec(), b"second".to_vec()];
    exact_for(&bytes, unbound)
}

fn exact_for(bytes: &[Vec<u8>; 2], unbound: bool) -> TestCaseResult {
    let [(old, expected_trace), (new, trace)] = paired(bytes, false, unbound, true);
    let expected = differential::allocation_counts(old.1);
    let actual = differential::allocation_counts(new.1);
    prop_assert!(
        old.0 == new.0
            && actual == expected
            && trace == expected_trace
            && trace
                .iter()
                .filter(|(op, _)| *op == "open workspace")
                .count()
                == 0,
        "exact binding allocation differs: actual={actual:?}, expected={expected:?}"
    );
    Ok(())
}

#[test]
fn workspace_binding_has_exact_physical_allocations() {
    exact(false).unwrap();
}

#[test]
fn restoring_redundant_workspace_opens_fails_the_exact_guard() {
    let error = exact(true).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("exact binding allocation differs")
    );
}

#[test]
fn binding_accounting_measurements() {
    let bytes = [b"first".to_vec(), b"second".to_vec()];
    let [(old, _), (new, _)] = pair(&bytes, false, false);
    assert_eq!(old.0, new.0);
    eprintln!(
        "PERF_ACCOUNTING {}",
        serde_json::json!({
            "case": "binding", "baseline": differential::allocation_counts(old.1), "allocations": differential::allocation_counts(new.1),
        })
    );
}
