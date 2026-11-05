use super::accounting_tests::{ObservedAuthority, Prefunded, ReplayCustody};
use super::*;
use std::sync::Mutex;

#[tokio::test]
async fn fork_preserves_original_accounting_through_the_authority_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let authority = Arc::new(ObservedAuthority::default());
    let fs = crate::Fs::new(
        authority.clone(),
        Arc::new(crate::MemoryObjectStore::default()),
        crate::EmbeddedCapabilities::MEMORY,
    );
    let root = fs
        .create_workspace("fork-accounting-root", budget, &cancellation)
        .await?
        .value;
    root.write_text("/source", "original").await?;
    let mut workspaces = vec![root];
    for (index, name) in ["left", "right", "grandchild"].into_iter().enumerate() {
        let parent = &workspaces[usize::from(index == 2)];
        let generation = parent.head(budget, &cancellation).await?.value;
        let options = ForkOptions::from_generation(generation, IdempotencyKey::new());
        let options = if index == 2 {
            options.inherit_paths(vec!["/source".to_owned()])
        } else {
            options
        };
        let child = if index == 1 {
            let original = crate::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(error, _)| error)?;
            *authority.replay_custody.lock().unwrap() =
                Some(ReplayCustody::Native(original.clone()));
            let child = parent
                .fork::<_, _, WorkspaceError>(
                    name,
                    options,
                    crate::PublicationPermit::Unrestricted,
                    &original,
                    &cancellation,
                    (),
                )
                .await?;
            original.receipt().verify(budget)?;
            child
        } else {
            let original = Arc::new(Prefunded {
                budget,
                held: Mutex::new(None),
            });
            *authority.replay_custody.lock().unwrap() =
                Some(ReplayCustody::Standalone(original.clone()));
            let (result, spent, live, baseline) = parent
                .fork::<_, _, WorkspaceError>(
                    name,
                    options,
                    crate::PublicationPermit::Unrestricted,
                    original.as_ref(),
                    &cancellation,
                    (),
                )
                .await;
            spent.verify(budget)?;
            assert!(live >= baseline, "fork retired earlier original backing");
            result?
        };
        assert!(authority.replay_custody.lock().unwrap().is_none());
        assert_eq!(child.name().as_str(), name);
        let read = child.read("/source", 8, budget, &cancellation).await?;
        assert_eq!(read.value.as_ref(), b"original");
        workspaces.push(child);
    }
    let root = &workspaces[0];
    let generation = root.head(budget, &cancellation).await?.value;
    for cancelled in [false, true] {
        let cancellation = CancellationToken::new();
        if cancelled {
            cancellation.cancel();
        }
        let constrained = WorkBudget {
            allocation_operations: 0,
            ..budget
        };
        *authority.replay_minimum.lock().unwrap() = Some(0);
        let failure = root
            .fork::<_, _, WorkspaceError>(
                "refused-child",
                ForkOptions::from_generation(generation.clone(), IdempotencyKey::new()),
                crate::PublicationPermit::Unrestricted,
                constrained,
                &cancellation,
                (),
            )
            .await
            .err()
            .ok_or("fork escaped cancellation or its allocation allowance")?;
        if cancelled {
            assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
            assert_eq!(*failure.work, crate::WorkCounters::UNCHARGED);
        } else {
            assert!(matches!(
                failure.error,
                WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                    counter: "allocation_operations",
                    ..
                })
            ));
        }
        assert_eq!(failure.work.backend_read_operations, 0);
        assert!(authority.replay_minimum.lock().unwrap().take().is_some());
    }
    Ok(())
}
