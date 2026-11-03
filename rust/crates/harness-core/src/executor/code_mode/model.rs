//! The production interpreter with real declarations, allocation and drop paths.
use super::{allocator::EngineAllocator, interpreter};

/// Arbitrary programs must retire their engine; quoted input also has an
/// independent value oracle. No host tool, provider or external process runs.
pub async fn check(input: &[u8]) {
    let definitions: Vec<_> = crate::tools::catalog()
        .map(|tool| tool.bind().expect("catalog declaration").definition)
        .collect();
    let definitions: acyclic_stream::OwnedBytes = serde_json::to_vec(&definitions).unwrap().into();
    let text = String::from_utf8_lossy(input);
    let literal = serde_json::to_string(text.as_ref()).unwrap();
    for source in [std::str::from_utf8(input).ok(), Some(literal.as_str())]
        .into_iter()
        .flatten()
    {
        for deny_allocation in [false, true] {
            let mut budget = acyclic_fs::WorkBudget::UNBOUNDED;
            budget.peak_allocation_bytes = if deny_allocation {
                acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
            } else {
                // Fuzz credit bounds hostile expansion; it changes no product default.
                64 * 1024 * 1024
            };
            let work = acyclic_fs::NativeMaterializationWork::new(
                budget,
                &acyclic_fs::CancellationToken::new(),
            )
            .expect("fund the original accounting owner");
            let baseline = work.live_allocation_bytes();
            let allocator = EngineAllocator::new(Some(&work));
            let failure = allocator.failure();
            let (send, receive) = tokio::sync::mpsc::channel(1);
            // Dropping the actual dispatcher is the producer-loss case. RPC
            // send/receive errors use the same channel path as a retired host.
            drop(receive);
            let result = interpreter::run(
                source.to_owned(),
                definitions.clone(),
                send,
                tokio_util::sync::CancellationToken::new(),
                literal.len().max(4096),
                allocator,
                16,
            )
            .await;
            assert_eq!(work.live_allocation_bytes(), baseline);
            if deny_allocation {
                assert!(result.is_err());
                assert!(failure.lock().unwrap().is_some());
            } else if source == literal.as_str() && failure.lock().unwrap().is_none() {
                assert_eq!(result.unwrap(), serde_json::Value::String(text.to_string()));
            }
        }
    }
}
