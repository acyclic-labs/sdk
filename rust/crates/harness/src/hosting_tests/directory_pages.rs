//! Real session byte credit selects immutable directory prefixes.
use super::*;
use crate::conversation::{DirectoryQuery, DirectoryTarget};

#[tokio::test]
async fn directory_byte_credit_resumes_exact_prefixes_and_rechecks_authority() -> Result {
    acyclic_native_runtime::initialize_native_io_services()?;
    let bound = binding()?;
    let stream = bound.agents().provider().clone();
    let owner = Arc::new(Owner::default());
    let agent = AgentId::from_bytes([42; 16]);
    let Serving::Served(server) = serve(
        bound,
        owner.clone(),
        &IntentId::try_from("directory-prefixes")?,
        RootRequest {
            peer: true,
            agent: agent.clone(),
            fresh: vec![WorkspaceEntry::initial_volume(
                crate::agent::WorkspaceRole::Private,
                PRIVATE_VOLUME.into(),
                VolumeRef::new(
                    provider()?,
                    "private".into(),
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(crate::AgentId::from_bytes(agent.to_bytes())),
                    &mut None,
                )?,
            )],
            retained: Vec::new(),
            brief: None,
            policy: None,
        },
    )
    .await?
    else {
        return Err("uncertain root publication".into());
    };
    let root = server.root().clone();
    let names: Vec<_> = (0..8)
        .map(|index| format!("{index:02}-{}", "x".repeat(192)))
        .collect();
    let changes: Vec<_> = names
        .iter()
        .map(|name| crate::client_files::ClientFileChange {
            path: format!("ui/{name}"),
            expected: None,
            text: Some("original".into()),
        })
        .collect();
    server
        .bound()
        .publish_agent_files(&root, OperationId::new(), &changes)
        .await?;
    let prototype = server
        .bound()
        .directory_page_select(
            &root,
            DirectoryQuery {
                target: DirectoryTarget::Private {},
                path: "ui",
                generation: None,
                after_generation: None,
                after_name: None,
                maximum_entries: 8,
            },
            |page, _| {
                assert_eq!(page.entries.len(), 8);
                Ok(1)
            },
        )
        .await?;
    let generation = prototype.generation.clone();
    let credit = u32::try_from(acyclic_harness_client::encoded_agent_reply_size(
        serde_json::to_vec(&AgentReply::Directory { page: prototype })?.len() as u64,
    )?)?;
    let client = Client::in_process_with_products(
        stream,
        acyclic_stream::session::testing::authority(),
        None,
        server.clone(),
        Some(server.clone()),
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
        None,
    );
    client.register_credential(0, credential(server.resource()))?;
    let mut cursor = None;
    let mut first_cursor = None;
    let mut seen = Vec::new();
    loop {
        let command = AgentCommand::Directory {
            agent: root.clone(),
            target: DirectoryTarget::Private {},
            path: "ui".into(),
            generation: Some(generation.clone()),
            after_generation: None,
            page: AgentPageRequest::new(8, credit, cursor)?,
        };
        let Ok(reply @ AgentReply::Directory { .. }) = call(&client, "agents", &command).await?
        else {
            return Err("directory page was refused".into());
        };
        assert!(
            acyclic_harness_client::encode_agent_reply(&Ok(reply.clone()), &command)?.len()
                <= credit as usize
        );
        let AgentReply::Directory { page } = reply else {
            unreachable!()
        };
        assert_eq!(page.generation, generation);
        assert!(!page.entries.is_empty());
        if seen.is_empty() {
            assert!(
                page.entries.len() < 8,
                "byte credit must force an interior cut"
            );
            assert!(page.has_more);
            first_cursor = page.next.clone();
            server
                .bound()
                .publish_agent_files(
                    &root,
                    OperationId::new(),
                    &[crate::client_files::ClientFileChange {
                        path: "ui/later".into(),
                        expected: None,
                        text: Some("outside the cut".into()),
                    }],
                )
                .await?;
        }
        seen.extend(page.entries.into_iter().map(|entry| entry.name));
        assert!(
            seen.len() <= names.len(),
            "directory continuation did not advance"
        );
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(seen, names);
    owner.denied.store(true, Ordering::SeqCst);
    assert!(
        !matches!(
            call(
                &client,
                "agents",
                &AgentCommand::Directory {
                    agent: root,
                    target: DirectoryTarget::Private {},
                    path: "ui".into(),
                    generation: Some(generation),
                    after_generation: None,
                    page: AgentPageRequest::new(8, credit, first_cursor)?,
                }
            )
            .await,
            Ok(Ok(_))
        ),
        "a saved prefix must not bypass current authority"
    );
    Ok(())
}
