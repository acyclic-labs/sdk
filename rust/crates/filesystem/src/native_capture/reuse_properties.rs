use super::*;
use proptest::prelude::*;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

async fn recapture(
    checkout: &mut crate::facade::MemoryCheckout,
    options: &CaptureOptions,
    work: &NativeMaterializationWork,
    reuse_override: Option<bool>,
) {
    let baseline = NativeViewBaseline {
        reuse_override,
        ..NativeViewBaseline::new(options.expected_root_identity, true)
    };
    let root = NamespacePath::new(Vec::new(), checkout.volume_config().limits).unwrap();
    capture_subtrees_with_native_work(
        checkout,
        &[root],
        options,
        &CapturePolicy::default(),
        work.cancellation(),
        Some(&baseline),
        work,
    )
    .await
    .unwrap();
}

fn candidate(
    checkout: &crate::facade::MemoryCheckout,
    work: &NativeMaterializationWork,
) -> crate::facade::MemoryCheckout {
    checkout
        .private_candidate(
            &mut crate::kernel::KernelAccounting::native(work, work.cancellation()),
            work.cancellation(),
        )
        .unwrap()
}

#[test]
fn reuse_matches_full_recapture_for_replacements_xattrs_links_and_owners()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 64,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    });
    // Property: both reuse predicates produce the exact authenticated generation
    // root of forced full recapture from the same pre-edit checkout. Each body
    // covers every edit, and an unconditional-reuse control must fail on owners.
    runner.run(
        &(
            prop_oneof![Just(Vec::new()), prop::collection::vec(1_u8..=255, 70..96)],
            prop::collection::vec(any::<u8>(), 0..17),
            1_u32..256,
            any::<bool>(),
        ),
        |(body, attribute, owner_delta, initial_link)| {
            runtime.block_on(async {
                for edit in 0..4 {
                    let source = tempfile::tempdir().unwrap();
                    let file = source.path().join("file");
                    std::fs::write(&file, &body).unwrap();
                    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600))
                        .unwrap();
                    if initial_link {
                        std::fs::hard_link(&file, source.path().join("oldlink")).unwrap();
                    }
                    let before = std::fs::metadata(&file).unwrap();
                    let cancellation = CancellationToken::new();
                    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                        .unwrap();
                    let mut initial = capture_policy_tests::native_checkout(&work).await.unwrap();
                    let options = CaptureOptions {
                        source_root: source.path().to_owned(),
                        expected_root_identity: capture_root_identity(source.path()).unwrap(),
                        maximum_paths: 8,
                        maximum_extent_spans: 8,
                    };
                    // Initial capture has no baseline timestamp normalization.
                    let root =
                        NamespacePath::new(Vec::new(), initial.volume_config().limits).unwrap();
                    capture_subtrees_with_native_work(
                        &mut initial,
                        &[root],
                        &options,
                        &CapturePolicy::default(),
                        &cancellation,
                        None,
                        &work,
                    )
                    .await
                    .unwrap();
                    match edit {
                        0 => {
                            let replacement = source.path().join("replacement");
                            std::fs::write(&replacement, &body).unwrap();
                            std::fs::set_permissions(&replacement, before.permissions()).unwrap();
                            std::fs::rename(&replacement, &file).unwrap();
                            prop_assert_ne!(std::fs::metadata(&file).unwrap().ino(), before.ino());
                        }
                        1 => rustix::fs::setxattr(
                            &file,
                            c"user.rev693",
                            &attribute,
                            rustix::fs::XattrFlags::empty(),
                        )
                        .unwrap(),
                        2 => std::fs::hard_link(&file, source.path().join("zlink")).unwrap(),
                        _ => {
                            let path = NamespacePath::from_portable(
                                &crate::path::PortablePath::parse(
                                    "/file",
                                    initial.volume_config().limits,
                                )
                                .unwrap(),
                                initial.volume_config().limits,
                            )
                            .unwrap();
                            let (lookup, custody) = initial
                                .lookup_batch_no_follow(
                                    std::slice::from_ref(&path),
                                    &work,
                                    &cancellation,
                                )
                                .await
                                .unwrap();
                            let record = lookup.entries[0].record.unwrap();
                            drop((lookup, custody));
                            let mut metadata = initial
                                .read_metadata(record.file_id, &work, &cancellation)
                                .await
                                .unwrap();
                            // An old canonical owner followed by the real current host
                            // owner exercises the transition without privileged chown.
                            metadata.posix_uid =
                                MetadataField::Value(before.uid().wrapping_add(owner_delta));
                            metadata.posix_gid =
                                MetadataField::Value(before.gid().wrapping_add(owner_delta));
                            let changes = vec![AuthoredMutation::SetMetadata { path, metadata }];
                            let slots = work
                                .claim(
                                    (changes.capacity() * std::mem::size_of::<AuthoredMutation>())
                                        as u64,
                                    1,
                                    WorkCounters::UNCHARGED,
                                )
                                .unwrap();
                            initial
                                .apply_borrowed_authored_transaction_with_native_work(
                                    &changes,
                                    &slots,
                                    &work,
                                    &cancellation,
                                )
                                .await
                                .unwrap();
                        }
                    }
                    std::fs::File::open(&file)
                        .unwrap()
                        .set_times(
                            std::fs::FileTimes::new()
                                .set_modified(before.modified().unwrap())
                                .set_accessed(before.accessed().unwrap()),
                        )
                        .unwrap();
                    let mut reused = candidate(&initial, &work);
                    let mut full = candidate(&initial, &work);
                    recapture(&mut reused, &options, &work, None).await;
                    recapture(&mut full, &options, &work, Some(false)).await;
                    prop_assert_eq!(reused.root(), full.root(), "edit={}", edit);
                    if edit == 3 {
                        let mut broken = candidate(&initial, &work);
                        recapture(&mut broken, &options, &work, Some(true)).await;
                        prop_assert_ne!(
                            broken.root(),
                            full.root(),
                            "negative control accepted stale owners"
                        );
                    }
                }
                Ok(())
            })
        },
    )?;
    Ok(())
}
