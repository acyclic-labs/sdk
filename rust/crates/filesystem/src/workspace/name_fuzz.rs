use super::*;

fn outcome(result: Result<WorkspaceName, WorkspaceError>) -> Result<String, String> {
    result
        .map(|name| name.as_str().to_owned())
        .map_err(|error| format!("{error:?}"))
}

/// Checks canonical workspace names and denied effects through the real parser.
#[doc(hidden)]
pub fn check_workspace_name(data: &[u8]) {
    if data.len() > 4096 {
        return;
    }
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let cancellation = CancellationToken::new();
    let mut actual = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut old = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let decoded = WorkspaceName::with_accounting(text, &mut actual);
    let mut validation = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    assert_eq!(
        WorkspaceName::validate_with_accounting(text, &mut validation)
            .map_err(|error| format!("{error:?}")),
        decoded
            .as_ref()
            .map(|_| ())
            .map_err(|error| format!("{error:?}")),
        "borrowed validation changed canonical syntax"
    );
    if text.is_ascii() {
        assert_eq!(
            validation.spent().bytes_copied,
            0,
            "borrowed validation copied a name"
        );
        assert_eq!(
            validation.spent().allocation_operations,
            0,
            "borrowed validation allocated a name"
        );
    }
    let expected = reference::decode(text, &mut old, false, &mut |_, _, _| {});
    assert_eq!(
        outcome(decoded),
        outcome(expected),
        "canonical bytes and semantic errors must match main"
    );
    let mut full = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    if let Ok(name) = WorkspaceName::with_accounting(text, &mut full) {
        let mut again = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let reparsed = WorkspaceName::with_accounting(name.as_str(), &mut again);
        assert_eq!(
            Ok(name.as_str().to_owned()),
            outcome(reparsed),
            "canonical names must round-trip"
        );
    }

    for validate in [false, true] {
        let effects = check_effects(data, text, &cancellation, validate);
        assert!(effects.is_ok(), "{effects:?}");
    }
    cancellation.cancel();
    let mut cancelled = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    assert!(matches!(
        WorkspaceName::validate_with_accounting(text, &mut cancelled),
        Err(WorkspaceError::Cancelled(_))
    ));
    assert_eq!(cancelled.spent(), crate::WorkCounters::UNCHARGED);
}

fn check_effects(
    data: &[u8],
    text: &str,
    cancellation: &CancellationToken,
    validate: bool,
) -> Result<(), FsError> {
    let selector = data.first().copied().unwrap_or(0);
    let limit = WorkBudget {
        items_examined: if selector & 1 == 0 {
            u64::MAX
        } else {
            u64::from(selector)
        },
        bytes_copied: if selector & 2 == 0 {
            u64::MAX
        } else {
            u64::from(data.last().copied().unwrap_or(0))
        },
        ..WorkBudget::UNBOUNDED
    };
    #[cfg(not(target_arch = "wasm32"))]
    let root = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, cancellation)
        .map_err(|(error, _)| error)?;
    #[cfg(not(target_arch = "wasm32"))]
    let child = root.native_volume_work(limit)?;
    #[cfg(not(target_arch = "wasm32"))]
    let mut accounting = if selector & 4 != 0 {
        KernelAccounting::native(&child, cancellation)
    } else {
        KernelAccounting::standalone(limit, cancellation)
    };
    #[cfg(target_arch = "wasm32")]
    let mut accounting = KernelAccounting::standalone(limit, cancellation);
    let before = accounting.spent();
    let (mut examined, mut copied) = (0, 0);
    let mut probe = |tag, value, bytes| match tag {
        0 | 1 => examined += 1,
        2 => copied += bytes,
        4 => copied += value,
        _ => (),
    };
    if validate {
        let _ = WorkspaceName::parse_with(
            text,
            &mut accounting,
            char::is_ascii,
            &mut probe,
            |_, _, _| Ok(()),
        );
    } else {
        let _ = WorkspaceName::with_probe(text, &mut accounting, char::is_ascii, &mut probe);
    }
    let after = accounting.spent();
    assert_eq!(
        after.items_examined - before.items_examined,
        examined,
        "every actual canonical visit stays charged"
    );
    assert_eq!(
        after.bytes_copied - before.bytes_copied,
        copied,
        "every actual copy stays charged"
    );
    assert!(
        examined <= limit.items_examined,
        "denied canonical visits cannot execute"
    );
    assert!(copied <= limit.bytes_copied, "denied copies cannot execute");
    Ok(())
}
