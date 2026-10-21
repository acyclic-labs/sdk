use super::*;

#[test]
fn refusals_cannot_hide_fatal_errors_or_bypass_original_work_and_output_limits() -> Result<()> {
    for error in [
        Error::Invalid("private input".into()),
        Error::Storage("private transport".into()),
        Error::NativeBoundary {
            code: ErrorCode::Unauthorized,
            reason: "unauthorized",
        },
        Error::ToolRejected {
            code: ErrorCode::Cancelled,
            reason: "cancelled",
        },
        Error::NativeBoundary {
            code: ErrorCode::Unsupported,
            reason: "unsupported producer",
        },
    ] {
        let code = error.code();
        assert_eq!(
            rejection(error, 4096, None)
                .expect_err("fatal failure became a tool result")
                .code(),
            code
        );
    }
    let invalid = || Error::ToolRejected {
        code: ErrorCode::Invalid,
        reason: "fetch HTTP request rejected",
    };
    assert!(rejection(invalid(), 1, None).is_err());
    for cancelled in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut budget = acyclic_fs::performance::BOUNDARY_BUDGET;
        if !cancelled {
            budget.items_examined = 0;
        }
        let work = NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let before = work.live_allocation_bytes();
        if cancelled {
            cancellation.cancel();
        }
        let error =
            rejection(invalid(), 4096, Some(&work)).expect_err("refusal bypassed original work");
        assert!(error.is_native_failure(), "{error:?}");
        if cancelled {
            assert_eq!(error.code(), ErrorCode::Cancelled);
        }
        drop(error);
        assert_eq!(work.live_allocation_bytes(), before);
    }
    Ok(())
}
