use super::*;
use crate::executor::{ToolFailureCause, ToolFailureKind};
use proptest::prelude::*;

fn work() -> acyclic_fs::NativeMaterializationWork {
    acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(16_384, 16 * 1024 * 1024),
        &acyclic_fs::CancellationToken::new(),
    )
    .unwrap()
}

fn assert_recoverable(error: Error, original: &acyclic_fs::NativeMaterializationWork) {
    assert!(!error.is_native_failure(), "{error:?}");
    assert!(!error.leaves_effect_uncertain());
    let io = crate::executor::PreparationIo::from_native_work(original).unwrap();
    let cause = crate::executor::with_history_io(Some(&io), |history| {
        ToolFailureCause::of(&error, history)
    })
    .unwrap();
    assert!(cause.is_canonical());
    let content =
        crate::executor::tool_failure_content(ToolFailureKind::ExecutorRejected, &cause, Some(&io))
            .unwrap();
    assert!(matches!(
        content,
        crate::model::ToolResultContent::Error { .. }
    ));
    assert!(
        original.live_allocation_bytes()
            > acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // Every byte selection, including an empty interval inside a scalar, either returns
    // exact text or a definitive rejection funded by the original failure-result ledger.
    #[test]
    fn every_native_read_selection_is_text_or_a_recoverable_rejection(
        bytes in prop::collection::vec(any::<u8>(), 0..128), empty in any::<bool>(),
    ) {
        let original = work();
        let expected = if empty {
            if bytes.first().is_some_and(|byte| byte & 0xc0 == 0x80) { None } else { Some("") }
        } else { std::str::from_utf8(&bytes).ok() };
        match executor::read_text(&bytes, empty, Some(&original)) {
            Ok(text) => prop_assert_eq!(Some(text), expected),
            Err(error) => {
                prop_assert!(expected.is_none());
                prop_assert_eq!(error.code(), crate::wire::ErrorCode::Invalid);
                assert_recoverable(error, &original);
            }
        }
        prop_assert_eq!(original.live_allocation_bytes(), acyclic_fs::NativeMaterializationWork::accounting_state_bytes());
    }

    // The shared text algorithms retain the same classifications in portable and native
    // execution, including every invalid patch grammar and every missing/ambiguous edit.
    #[test]
    fn text_algorithm_rejections_preserve_class_without_becoming_infrastructure(
        source in "[abé\n]{0,24}", needle in "[abé]{0,6}", diff in ".{0,40}",
    ) {
        let original = work();
        let limits = super::patch::PatchLimits { maximum_bytes: 256, maximum_work: 4096, maximum_hunks: 8 };
        let options = super::text::SearchOptions { maximum_input_bytes:256, maximum_query_bytes:8, maximum_work:4096, maximum_matches:8 };
        for kind in 0..3 {
            let execute = |custody: &mut crate::NativeCustody| -> Result<()> {
                match kind {
                    0 => super::edit::exact_replace(&source, &needle, "x", 256, custody).map(|_| ()),
                    1 => super::patch::apply_update(&source, &diff, limits, custody).map(|_| ()),
                    _ => super::text::literal_search(&source, &needle, options, custody).map(|_| ()),
                }
            };
            let portable = execute(&mut None);
            let mut custody = super::edit::allocation(Some(&original), 0).unwrap();
            let native = execute(&mut custody);
            match (portable, native) {
                (Ok(()), Ok(())) => (),
                (Err(portable), Err(native)) => {
                    prop_assert_eq!(portable.code(), native.code());
                    assert_recoverable(native, &original);
                }
                pair => prop_assert!(false, "different outcomes: {pair:?}"),
            }
        }
        prop_assert_eq!(original.live_allocation_bytes(), acyclic_fs::NativeMaterializationWork::accounting_state_bytes());
    }
}

#[test]
fn the_former_boundary_classification_and_real_budget_exhaustion_cannot_be_settled() {
    let original = work();
    let error = executor::read_text(&[0xa9], true, Some(&original)).unwrap_err();
    assert_recoverable(error, &original);
    // Negative control: restore the former production classification, keeping the exact cause.
    let broken = Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid,
        reason: "read range splits UTF-8",
    };
    assert!(broken.is_native_failure());
    let mut custody = super::edit::allocation(Some(&original), 0).unwrap();
    let infrastructure =
        super::edit::reserve_vec::<u64>(&mut custody, &mut Vec::new(), usize::MAX).unwrap_err();
    assert!(infrastructure.is_native_failure());
    let exhausted = original
        .claim(u64::MAX, 0, acyclic_fs::WorkCounters::UNCHARGED)
        .unwrap_err();
    let exhausted: Error = exhausted.into();
    assert!(exhausted.is_native_failure());
}

#[test]
fn native_file_selection_bounds_use_the_recoverable_preflight_class() -> Result<()> {
    let original = work();
    let file = crate::tool::tests::file("é".as_bytes())?;
    let scope = RuntimeScope::new(
        crate::Capabilities::new([file.read_capability(&mut None)?]),
        crate::conversation::Limits::default(),
    )?;
    for (start, end) in [(2, 1), (0, 3)] {
        let invocation = ToolInvocation {
            operation_id: crate::OperationId::new(),
            call_id: "selection".into(),
            name: "read".into(),
            arguments: json!({"file":file,"range":{"start":start,"end":end}}),
        };
        let error = FileTool::Read
            .preflight(&scope, &invocation, Some(&original))
            .unwrap_err();
        assert_recoverable(error, &original);
    }
    Ok(())
}
