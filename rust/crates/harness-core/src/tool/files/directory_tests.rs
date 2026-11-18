use super::*;
use crate::{Capabilities, OperationId, conversation::VolumeOperation, runtime::Bindings};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn delivered_entry_resumption_preserves_its_cut_under_changing_page_credit(
        seed in any::<u64>(), original_limit in 1..7u32, position in 0..6usize,
        credits in prop::collection::vec(1..7u32, 1..8),
    ) {
        acyclic_native_runtime::testing::seeded_runtime(seed)
            .map_err(|error| TestCaseError::fail(error.to_string()))?.block_on(async {
            let storage = crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65536).await?;
            let names = (0..6).map(|index| format!("a{index:02}")).collect::<Vec<_>>();
            for name in &names {
                storage.stage(OperationId::new(), &format!("notes/{name}"), b"old", "text/plain", name).await?;
            }
            storage.stage(OperationId::new(), "other/entry", b"other", "text/plain", "entry").await?;
            let task = Arc::new(Bindings {
                scope: RuntimeScope::new(Capabilities::new([
                    "tool:call:read".into(),
                    storage.volume().capability(VolumeOperation::Read, &mut None)?,
                ]), crate::conversation::Limits::default())?,
                ..Bindings::local()
            }.build()?.agent_context(&Capabilities::default(), storage.content_bindings())?);
            let read_page = |path: &'static str, after: Option<String>, limit: u32| {
                let task = Arc::clone(&task);
                async move {
                    let invocation = ToolInvocation {
                        operation_id: OperationId::new(), call_id: "page".into(), name: "read".into(),
                        arguments: json!({"path":path, "selection":{"kind":"directory", "after":after, "limit":limit}}),
                    };
                    let context = ToolContext::new(task, invocation.operation_id, &invocation.call_id)?;
                    let result = crate::tool::files::read()?.executor.execute(invocation, Some(context)).await?;
                    serde_json::from_value::<DirectoryReadResult>(result.value)
                        .map_err(|error| Error::Invalid(error.to_string()))
                }
            };
            let original = read_page("notes", None, original_limit).await?;
            let delivered = &original.entries[position % original.entries.len()];
            let foreign = read_page("other", Some(delivered.cursor.clone()), original_limit).await;
            assert!(
                matches!(foreign, Err(Error::Invalid(reason)) if reason == "directory cursor belongs to another selection"),
                "a cursor from another directory must be refused before reading"
            );
            let mut next = Some(delivered.cursor.clone());
            storage.stage(OperationId::new(), "notes/a00a", b"new", "text/plain", "later").await?;
            let mut actual = Vec::new();
            let mut pages = 0;
            while let Some(after) = next {
                assert!(pages <= names.len(), "continuation did not advance");
                let limit = credits[pages % credits.len()];
                let page = read_page("notes", Some(after), limit).await?;
                assert!(page.entries.len() <= limit as usize, "new row credit was exceeded");
                actual.extend(page.entries.into_iter().map(|entry| entry.name));
                next = page.next;
                pages += 1;
            }
            assert_eq!(actual, names.into_iter().filter(|name| name > &delivered.name).collect::<Vec<_>>());
            Ok::<_, crate::Error>(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
