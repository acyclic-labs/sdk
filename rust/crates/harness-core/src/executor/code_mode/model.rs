//! The production interpreter with real declarations, allocation and drop paths.
use super::{allocator::EngineAllocator, interpreter};

fn native_work(deny_allocation: bool) -> acyclic_fs::NativeMaterializationWork {
    let mut budget = acyclic_fs::WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes = if deny_allocation {
        acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
    } else {
        // Fuzz credit bounds hostile expansion; it changes no product default.
        64 * 1024 * 1024
    };
    acyclic_fs::NativeMaterializationWork::new(budget, &acyclic_fs::CancellationToken::new())
        .expect("fund the original accounting owner")
}

/// Programs and reply frames must retire their engine and original credit.
/// Quoted input and production-encoded replies have independent value oracles.
/// No host tool, provider or external process runs.
pub async fn check(input: &[u8]) {
    let definitions: Vec<_> = crate::tools::catalog()
        .map(|tool| tool.bind().expect("catalog declaration").definition)
        .collect();
    let tool_name = serde_json::to_string(&definitions[0].name).unwrap();
    let definitions: acyclic_stream::OwnedBytes = serde_json::to_vec(&definitions).unwrap().into();
    let text = String::from_utf8_lossy(input);
    let literal = serde_json::to_string(text.as_ref()).unwrap();
    for source in [std::str::from_utf8(input).ok(), Some(literal.as_str())]
        .into_iter()
        .flatten()
    {
        for deny_allocation in [false, true] {
            let work = native_work(deny_allocation);
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
    // Exercise the original reply producer and decoder together. The dispatcher
    // transports one frame; it does not stand in for tool admission or execution.
    let source = format!(
        "let result; try {{ result = {{ok:true,value:await tools[{tool_name}]({{}})}}; }} \
         catch (error) {{ result = {{ok:false,value:String(error.message)}}; }} result"
    );
    for kind in 0..3 {
        let work = native_work(false);
        let baseline = work.live_allocation_bytes();
        let io = crate::filesystem::PreparationIo::from_native_work(&work).unwrap();
        let value = serde_json::Value::String(text.to_string());
        let reply = if kind < 2 {
            super::encode_reply(kind == 0, &value, Some(&io))
        } else {
            // Arbitrary malformed frames retain their input credit through the
            // same OwnedBytes entrance as the encoder's valid frames.
            (|| {
                let mut custody =
                    Some(work.claim(input.len() as u64, 1, acyclic_fs::WorkCounters::UNCHARGED)?);
                let mut bytes = input.to_vec();
                acyclic_stream::OwnedBytes::from_vec_with_guard_in_place(
                    &mut bytes,
                    &mut custody,
                    &work.runtime_meter(),
                )
                .map_err(|cause| crate::Error::from(work.runtime_error(cause)))
            })()
        };
        let Ok(reply) = reply else {
            drop(io);
            assert_eq!(work.live_allocation_bytes(), baseline);
            continue;
        };
        let allocator = EngineAllocator::new(Some(&work));
        let failure = allocator.failure();
        let (send, mut receive) = tokio::sync::mpsc::channel(1);
        let run = interpreter::run(
            source.clone(),
            definitions.clone(),
            send,
            tokio_util::sync::CancellationToken::new(),
            literal.len().saturating_add(4096),
            allocator,
            16,
        );
        let dispatch = async move {
            // The interpreter owns the sender. Early engine retirement closes
            // recv; sending the only reply settles the only admitted RPC wait.
            if let Some(super::Request::Tool(call)) = receive.recv().await {
                let _ = call.reply.send(Some(reply));
            }
        };
        let (result, ()) = futures::join!(run, dispatch);
        drop(io);
        assert_eq!(work.live_allocation_bytes(), baseline);
        let failed = failure.lock().unwrap().is_some();
        if !failed && kind < 2 {
            assert_eq!(
                result.unwrap(),
                serde_json::json!({"ok": kind == 0, "value": value}),
                "the original host reply must preserve its value and refusal"
            );
        } else if !failed && std::str::from_utf8(input).is_err() {
            assert_eq!(result.unwrap()["ok"], false, "invalid UTF-8 must refuse");
        }
    }
}
