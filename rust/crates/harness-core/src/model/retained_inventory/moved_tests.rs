use super::*;

#[test]
fn producer_and_replayed_events_keep_the_same_resident_json_custody() -> Result<()> {
    let sparse = serde_json::json!({"outer": [{"inner": "value"}]});
    let sources = [
        ModelEvent::ToolCall(
            ToolCall::new("call".into(), "tool".into(), sparse.clone())
                .with_metadata(sparse.clone()),
        ),
        ModelEvent::Completed { metadata: sparse },
    ];
    for source in sources {
        let bytes = crate::contract::canonical_json_bytes(&source)?;
        for replay in [false, true] {
            let cancellation = acyclic_fs::CancellationToken::new();
            let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let baseline = work.live_allocation_bytes();
            let io = PreparationIo::from_native_work(&work)?;
            let event = if replay {
                crate::executor::retain_preparation(crate::executor::decode_json::<ModelEvent>(
                    &bytes,
                    Some(&io),
                )?)?
            } else {
                let (event, custody) = ModelEvent::decode_with_native_work(&bytes, &work)?;
                io.absorb_native_custody(custody)?;
                event
            };
            assert_eq!(event, source);
            let retained = event.retained_owned_bytes()?;
            assert_eq!(
                io.owned_bytes()?,
                retained,
                "event handoff lost or retained unrelated custody"
            );
            assert_eq!(work.live_allocation_bytes(), baseline + retained);
            let logical = std::mem::size_of::<ModelEvent>() as u64
                + match &event {
                    ModelEvent::ToolCall(call) => call.payload_bytes()?,
                    ModelEvent::Completed { metadata } => {
                        crate::executor::json::json_value_logical_owned_bytes(metadata)?
                    }
                    _ => return Err(Error::Invalid("unexpected fixture event".into())),
                };
            assert!(
                logical < retained,
                "the old handoff inventory must omit sparse slots"
            );
            let custody = io.guard_owned(retained)?;
            drop(event);
            drop(custody);
            assert_eq!(io.owned_bytes()?, 0);
            drop(io);
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }
    Ok(())
}
