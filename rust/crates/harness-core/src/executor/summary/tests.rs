use super::*;
use crate::conversation::{FileDescriptor, VolumeOwner, VolumeRef};
use crate::executor::content::validate_model_content_scope;
use crate::resources::ProviderRef;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    // A readable summary never requires exposing any host-journal payload.
    #[test]
    fn summary_read_authority_is_independent_of_the_host_journal(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
        let volume = |name: &str| VolumeRef::new(
            ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None).unwrap(),
            name.into(), VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([1; 16])), &mut None,
        ).unwrap();
        let private = volume("private");
        let journal = volume("journal");
        let scope = RuntimeScope::new(crate::Capabilities::new([
            private.capability(crate::conversation::VolumeOperation::Read, &mut None).unwrap(),
            journal.directory_read_capability("inputs", &mut None).unwrap(),
        ]), Limits::default()).unwrap();
        let content = |volume| ModelContent::Part(crate::model::ModelContentPart::File {
            file: FileRef::new(volume, "summaries/turn/0.txt".into(), "1".into(),
                FileDescriptor::from_bytes(&bytes, "text/plain", &mut None).unwrap(),
                "summary.txt".into(), &mut None).unwrap(),
            policy: crate::model::FileProjectionPolicy::BoundedFull,
        });
        let private = content(private);
        let journal = content(journal);
        prop_assert!(validate_model_content_scope(&private, Limits::default(), Some(&scope), None).is_ok());
        prop_assert!(matches!(validate_model_content_scope(&journal, Limits::default(), Some(&scope), None),
            Err(Error::Unauthorized(_))));
        // Broken control: omitting the task scope admits that same journal file.
        prop_assert!(validate_model_content_scope(&journal, Limits::default(), None, None).is_ok());
    }
}

#[cfg(all(
    feature = "filesystem",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
#[test]
fn completed_summary_moves_original_text_and_retires_consumed_backing() -> Result<()> {
    let mut stock = StockExecutor::new(
        Model::new("test".into(), "summary-collection".into(), "1".into())?,
        Arc::new(crate::test_support::ScriptedModelProvider::new([])),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    stock.limits.render_bytes = 8;
    for native in [false, true] {
        let observed = crate::test_support::accounted::accounted(native, 65_536, false, |io| {
            let execution = StockExecution {
                io: Some(io),
                ..StockExecution::ordinary(&stock)
            };
            // These are generated collection operands, not provider fixtures.
            let mut events = Vec::new();
            stock_vec_push(
                &mut events,
                ModelEvent::Content {
                    delta: stock_string_copy("ignored", Some(io))?,
                },
                Some(io),
            )?;
            stock_vec_push(
                &mut events,
                ModelEvent::MessageCompleted {
                    text: stock_string_copy("earlier", Some(io))?,
                },
                Some(io),
            )?;
            let final_text = stock_string_copy("summary!", Some(io))?;
            let original = final_text.as_ptr();
            let capacity = final_text.capacity() as u64;
            stock_vec_push(
                &mut events,
                ModelEvent::MessageCompleted { text: final_text },
                Some(io),
            )?;
            let copied = io.receipt().bytes_copied;
            let text = execution.collect_summary_text(events)?;
            assert_eq!(text, "summary!");
            assert_eq!(text.as_ptr(), original);
            assert_eq!(
                io.receipt().bytes_copied,
                copied,
                "completed summary collection copied its already owned text"
            );
            assert_eq!(io.owned_bytes()?, capacity);
            execution.retire_step_text(text)?;
            assert_eq!(io.owned_bytes()?, 0);
            Ok::<_, Error>(())
        })
        .map_err(|(cause, _)| Error::Storage(cause))?;
        observed.0?;
        assert_eq!(observed.2, 0);
        if native {
            assert_eq!(
                observed.5,
                acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
            );
        }
    }
    let execution = StockExecution::ordinary(&stock);
    assert_eq!(
        execution.collect_summary_text(vec![
            ModelEvent::Content {
                delta: "sum".into()
            },
            ModelEvent::Content {
                delta: "mary".into()
            },
        ])?,
        "summary"
    );
    for events in [
        vec![ModelEvent::MessageCompleted {
            text: "too large".into(),
        }],
        vec![ModelEvent::Content {
            delta: "too large".into(),
        }],
        vec![ModelEvent::MessageCompleted {
            text: String::new(),
        }],
        vec![ModelEvent::ToolCall(crate::model::ToolCall::new(
            "call".into(),
            "shell".into(),
            serde_json::Value::Null,
        ))],
    ] {
        assert!(matches!(
            execution.collect_summary_text(events),
            Err(Error::Invalid(_))
        ));
    }
    Ok(())
}
