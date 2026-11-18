//! Public-contract error and source-retention tests.

mod contract_errors {
    mod cleanup_tests {
        use acyclic_harness_core::Error;
        #[cfg(not(target_arch = "wasm32"))]
        use acyclic_harness_core::wire::ErrorCode;

        #[test]
        fn classified_refusals_retain_original_parser_categories()
        -> std::result::Result<(), Box<dyn std::error::Error>> {
            use std::error::Error as _;
            for refusal in [
                Error::Unauthorized("request refused".into()),
                Error::Invalid("request refused".into()),
            ] {
                let expected = refusal.classification();
                let source = serde_json::from_slice::<serde_json::Value>(b"{").unwrap_err();
                let refusal = refusal.with_source(source);
                assert_eq!(refusal.classification(), expected);
                let mut cause = refusal.source();
                let original = loop {
                    let source = cause.ok_or("classified refusal discarded the JSON cause")?;
                    if let Some(original) = source.downcast_ref::<serde_json::Error>() {
                        break original;
                    }
                    cause = source.source();
                };
                assert_eq!(original.classify(), serde_json::error::Category::Eof);
                assert!(refusal.settles_tool_call());
            }
            Ok(())
        }

        #[cfg(not(target_arch = "wasm32"))]
        #[test]
        fn stream_conversion_retains_original_io_cause_and_uncertainty()
        -> std::result::Result<(), Box<dyn std::error::Error>> {
            use std::error::Error as _;
            let directory = tempfile::tempdir()?;
            for category in
                acyclic_stream::StreamCategory::ALL.map(acyclic_stream::StreamError::from)
            {
                let original = std::fs::read(directory.path().join("missing")).unwrap_err();
                let stream = category.clone().with_source(original);
                let retained = stream.clone();
                let uncertain = stream.uncertain();
                let error = Error::from(stream);
                assert_eq!(error.code(), ErrorCode::Storage);
                assert_eq!(error.uncertain(), uncertain);
                assert_eq!(error.leaves_outcome_uncertain(), uncertain);
                let classification = serde_json::to_value(error.classification())?;
                assert_eq!(
                    classification
                        .get("effect_uncertain")
                        .and_then(serde_json::Value::as_bool),
                    Some(uncertain)
                );
                let mut before = retained.source();
                let original = loop {
                    let cause = before.ok_or("original stream source disappeared")?;
                    if let Some(original) = cause.downcast_ref::<std::io::Error>() {
                        break original;
                    }
                    before = cause.source();
                };
                let mut source = error.source();
                let mut shared = false;
                while let Some(cause) = source {
                    if let Some(cause) = cause.downcast_ref::<std::io::Error>() {
                        assert!(
                            std::ptr::eq(cause, original),
                            "adapter reconstructed the original cause"
                        );
                        assert_eq!(cause.kind(), std::io::ErrorKind::NotFound);
                        shared = true;
                        break;
                    }
                    source = cause.source();
                }
                assert!(shared, "adapter discarded the original cause");
            }
            Ok(())
        }

        #[cfg(not(target_arch = "wasm32"))]
        #[test]
        fn nested_source_causes_preserve_classification_and_settlement() {
            use acyclic_fs::kernel::{NamespacePathError as Path, PathLookupError};
            use acyclic_fs::{FsError, LazyWorkspaceError as Lazy, WorkspaceError as Workspace};
            for (source, code, settles) in [
                (Path::Cancelled, ErrorCode::Cancelled, false),
                (
                    Path::Work(acyclic_fs_work::performance::WorkError::Overflow),
                    ErrorCode::Storage,
                    false,
                ),
                (Path::AllocationFailed, ErrorCode::Storage, false),
                (Path::PrefixOutOfRange, ErrorCode::Invalid, true),
            ] {
                for source in [
                    Lazy::NamespacePath(source.clone()),
                    Lazy::WorkspaceCause(Workspace::NamespacePath(source.clone())),
                    Lazy::Filesystem(FsError::Path(PathLookupError::NamespacePath(
                        source.clone(),
                    ))),
                    Lazy::WorkspaceCause(Workspace::Filesystem(FsError::Path(
                        PathLookupError::NamespacePath(source.clone()),
                    ))),
                ] {
                    let error = Error::from(source);
                    assert_eq!(error.code(), code, "{error:?}");
                    assert_eq!(error.settles_tool_call(), settles, "{error:?}");
                }
                let workspace = Error::from(Workspace::NamespacePath(source));
                assert_eq!(workspace.code(), code);
                assert_eq!(workspace.settles_tool_call(), settles);
            }
            for source in [
                Lazy::NativeWork(acyclic_fs::NativeTreeMaterializationError::MissingEntry),
                Lazy::Filesystem(FsError::NativeKernel(
                    acyclic_fs::NativeTreeMaterializationError::MissingEntry,
                )),
                Lazy::WorkspaceCause(Workspace::Filesystem(FsError::NativeKernel(
                    acyclic_fs::NativeTreeMaterializationError::MissingEntry,
                ))),
            ] {
                let error = Error::from(source);
                assert_eq!(error.code(), ErrorCode::NotFound);
                assert!(
                    !error.settles_tool_call(),
                    "native work became a tool refusal: {error:?}"
                );
            }
            let workspace = Error::from(Workspace::Filesystem(FsError::NativeKernel(
                acyclic_fs::NativeTreeMaterializationError::MissingEntry,
            )));
            assert!(!workspace.settles_tool_call());
            let work = Error::from(Lazy::WorkspaceCause(Workspace::Work(
                acyclic_fs_work::performance::WorkError::Overflow,
            )));
            assert_eq!(work.code(), ErrorCode::Storage);
            assert!(!work.settles_tool_call());
        }
    }
}
