use super::*;
use crate::conversation::ContentPublisher;
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;

#[derive(Clone, Copy)]
enum Fault {
    None,
    ForeignWorkspace,
    RejectAll,
}

async fn read_workspace(storage: &MemoryHarnessStorage, path: &str) -> Option<Vec<u8>> {
    let cancellation = acyclic_fs::CancellationToken::new();
    // Read the backend namespace independently of both the publisher's binding
    // and MemoryStore's retained-reference shortcut.
    let workspace = storage
        .host
        .filesystem
        .open_workspace(
            storage.volume.storage_name(&mut None).unwrap(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .unwrap()
        .value;
    let absolute = format!("/{path}");
    workspace
        .read(
            &absolute,
            65536,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .ok()
        .map(|receipt| receipt.value.to_vec())
}

async fn check(text: &[String; 3], fault: Fault) -> TestCaseResult {
    let left = MemoryHarnessStorage::new(AgentId::new(), 65536)
        .await
        .unwrap();
    let right = MemoryHarnessStorage::new(AgentId::new(), 65536)
        .await
        .unwrap();
    let path = format!("batch/item-{}.txt", text[0]);
    let first = format!("first:{}", text[1]);
    let second = format!("second:{}", text[2]);
    right
        .stage(
            OperationId::new(),
            &path,
            second.as_bytes(),
            "text/plain",
            "right",
        )
        .await
        .unwrap();
    let initial = left
        .stage(
            OperationId::new(),
            &path,
            b"initial",
            "text/plain",
            "initial",
        )
        .await
        .unwrap();
    let host = if matches!(fault, Fault::ForeignWorkspace) {
        let reference = super::super::fork_preparer_io::content_workspace(
            &right.host.provider,
            &right.volume,
            None,
        )
        .unwrap();
        let foreign = right.host.open(&reference, None).await.unwrap();
        left.host.for_workspace(&left.volume, foreign).unwrap()
    } else {
        left.host.clone()
    };
    let publisher = FilesystemContentPublisher::new(
        host,
        &left.volume,
        &left.verifier(),
        &left.scope,
        65536,
        None,
    )
    .unwrap();
    let published = if matches!(fault, Fault::RejectAll) {
        Err(Error::Unsupported("deliberate blanket rejection".into()))
    } else {
        publisher
            .stage(
                OperationId::new(),
                &path,
                first.as_bytes(),
                "text/plain",
                "left",
                None,
            )
            .unwrap()
            .await
            .map(|(file, _hold)| file)
    };
    let left_bytes = read_workspace(&left, &path).await;
    let right_bytes = read_workspace(&right, &path).await;
    let initial_bytes = left
        .content_verifier
        .read(&initial, 65536, None)
        .unwrap()
        .await
        .ok()
        .map(|bytes| bytes.to_vec());
    prop_assert!(
        published
            .as_ref()
            .is_ok_and(|file| file.volume() == &left.volume && file.path() == path)
            && left_bytes.as_deref() == Some(first.as_bytes())
            && right_bytes.as_deref() == Some(second.as_bytes())
            && initial_bytes.as_deref() == Some(b"initial".as_slice()),
        "workspace binding rejected a healthy write or crossed independent namespaces"
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn publication_binding_preserves_independent_live_namespaces(
        name in "[a-z]{0,8}", left in ".{0,32}", right in ".{0,32}",
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(check(&[name, left, right], Fault::None))?;
    }
}

#[tokio::test]
async fn workspace_binding_negative_controls() {
    let input = std::array::from_fn(|_| String::new());
    check(&input, Fault::None).await.unwrap();
    for fault in [Fault::ForeignWorkspace, Fault::RejectAll] {
        let error = check(&input, fault).await.unwrap_err();
        assert!(error.to_string().contains(
            "workspace binding rejected a healthy write or crossed independent namespaces"
        ));
    }
}
