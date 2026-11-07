//! The shipped argument adapters, their original ledger and destruction paths.
use super::*;
use crate::tool::ToolInvocation;

fn tools() -> &'static [Arc<Tool>] {
    static TOOLS: std::sync::OnceLock<Vec<Arc<Tool>>> = std::sync::OnceLock::new();
    TOOLS.get_or_init(|| {
        let mut tools = Vec::new();
        for profile in [
            NativeToolProfile::Anthropic,
            NativeToolProfile::Openai,
            NativeToolProfile::Google,
        ] {
            let mut registry = crate::tools::defaults().unwrap();
            bind(&mut registry, profile).unwrap();
            for definition in definitions(profile).unwrap() {
                if let Some(tool) = registry.get(&definition.name, None, None).unwrap() {
                    if tool.executor.capability().is_some() {
                        tools.push(Arc::new(tool.clone()));
                    }
                }
            }
        }
        tools
    })
}

fn native_work(
    deny: bool,
    cancellation: &acyclic_fs::CancellationToken,
) -> acyclic_fs::NativeMaterializationWork {
    let mut budget = acyclic_fs::WorkBudget::UNBOUNDED;
    if deny {
        budget.peak_allocation_bytes =
            acyclic_fs::NativeMaterializationWork::accounting_state_bytes();
    }
    acyclic_fs::NativeMaterializationWork::new(budget, cancellation).unwrap()
}

/// The input is the JSON argument object passed to the production registry.
pub fn check(input: &[u8]) {
    if let Ok(arguments) = serde_json::from_slice(input) {
        check_value(arguments);
    }
}

fn check_value(arguments: Value) {
    use futures::FutureExt as _;
    let mut invocation = ToolInvocation {
        operation_id: crate::OperationId::from_bytes([7; 16]),
        call_id: "native-call".into(),
        name: String::new(),
        arguments,
    };
    if invocation.arguments.is_object() {
        for (selector, text) in [
            ("@operation_id", invocation.operation_id.to_string()),
            ("@call_id", invocation.call_id.clone()),
        ] {
            let mut template = serde_json::json!({"identity":{"$case":selector}});
            template["identity"][&text] = serde_json::json!(format!("${selector}"));
            let arguments = arguments::Arguments::new(template, Vec::new()).unwrap();
            let expected = serde_json::json!({"identity":text});
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = native_work(false, &cancellation);
            let baseline = work.live_allocation_bytes();
            for budget in [None, Some(&work)] {
                let result = arguments.adapt(&invocation, budget);
                assert!(
                    result.is_ok(),
                    "identity selector rejected its emitted text"
                );
                let (actual, custody) = result.unwrap();
                assert_eq!(actual, expected);
                drop(actual);
                drop(custody);
            }
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }
    for tool in tools() {
        invocation.name.clone_from(&tool.definition.name);
        let expected = tool.executor.canonical(&invocation, None);
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = native_work(false, &cancellation);
        let baseline = work.live_allocation_bytes();
        let actual = tool.executor.canonical(&invocation, Some(&work));
        match (expected, actual) {
            (Ok(Some(expected)), Ok(Some(actual))) => {
                assert_eq!(*actual, *expected, "{}", invocation.name);
                let retained =
                    std::mem::size_of::<Value>() as u64 + actual.retained_payload_bytes().unwrap();
                assert_eq!(
                    work.live_allocation_bytes(),
                    baseline + retained,
                    "{} lost adapted argument custody",
                    invocation.name
                );
                assert!(
                    actual
                        .custody
                        .iter()
                        .flatten()
                        .all(|custody| custody.work().shares_ledger(&work))
                );
                drop(actual);
            }
            (Err(_), Err(_)) => {}
            (expected, actual) => panic!(
                "{} ordinary/native admission differs: {expected:?} / {actual:?}",
                invocation.name
            ),
        }
        assert_eq!(work.live_allocation_bytes(), baseline);
        // These original entry points must also pass the ledger to adaptation;
        // none may fall back to an uncharged copy when credit is exhausted.
        let denied = native_work(true, &cancellation);
        let denied_baseline = denied.live_allocation_bytes();
        assert!(tool.executor.canonical(&invocation, Some(&denied)).is_err());
        assert!(
            tool.executor
                .authorize(None, None, &invocation, Some(&denied))
                .is_err()
        );
        assert!(
            tool.executor
                .dispatch(&invocation, None, Some(&denied))
                .is_err()
        );
        assert!(
            tool.executor
                .cancellation(&invocation, None, Some(&denied))
                .is_err()
        );
        let replay = tool
            .executor
            .authorize_replay(None, &invocation, Some(&denied))
            .unwrap();
        assert!(
            replay
                .now_or_never()
                .expect("credit refusal is synchronous")
                .is_err()
        );
        assert_eq!(denied.live_allocation_bytes(), denied_baseline);
        cancellation.cancel();
        assert_eq!(
            tool.executor
                .canonical(&invocation, Some(&work))
                .unwrap_err()
                .code(),
            crate::wire::ErrorCode::Cancelled
        );
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    proptest::proptest! {
        #[test]
        fn native_adapter_preserves_original_allocation_custody(text in ".{0,128}", first in 0u64..8, last in 0u64..8, flag in proptest::bool::ANY) {
            // Generated argument values exercise every shipped operator and
            // source family through the registry; these are not host fixtures.
            for mode in ["insert", "replace", "delete", "unsupported"] {
                check_value(serde_json::json!({
                    "file_path":"a", "notebook_path":"n", "content":text,
                    "new_source":text, "cell_type":"code", "edit_mode":mode,
                    "old_string":text, "new_string":text, "replace_all":flag,
                    "command":["echo",text], "input":text,
                    "start_line":first, "end_line":last, "offset":first,
                    "limit":last, "timeout":last,
                }));
            }
            check_value(Value::String(text));
            check_value(Value::Null);
        }
    }
}
