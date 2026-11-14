//! Directory scale is independent of actors that are not selected or running.
use super::*;
use crate::agent::Phase;
use acyclic_stream::{CommitMutation, CommitOutcome, CommitRequest, IdempotencyKey};

#[tokio::test]
async fn session_and_peer_pages_keep_discovery_at_two_bounded_head_views() -> Result {
    async fn work(count: u128) -> Result<acyclic_stream::testing::Requests> {
        let bound = binding()?;
        let stream = bound.agents().provider().clone();
        let owner = Arc::new(Owner::default());
        owner.sessions.store(true, Ordering::SeqCst);
        let Serving::Served(server) = serve(
            bound,
            owner,
            &IntentId::try_from("directory-scale")?,
            RootRequest {
                peer: true,
                agent: AgentId::from_bytes([1; 16]),
                fresh: vec![WorkspaceEntry::initial_volume(
                    crate::agent::WorkspaceRole::Private,
                    PRIVATE_VOLUME.into(),
                    VolumeRef::new(
                        provider()?,
                        "directory-owner".into(),
                        VolumeClass::AgentPrivate,
                        VolumeOwner::Agent(crate::AgentId::from_bytes([1; 16])),
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
            return Err("directory owner publication was uncertain".into());
        };
        let roots = server.bound().session_roots()?;
        let status = StatusRecord {
            cancelled_input: None,
            child_settlement: None,
            child_wakes: None,
            phase: Phase::Idle,
            cursor: 0,
            conversation: 0,
        }
        .append_record()?;
        // Seed committed directory/status facts, without starting ten thousand
        // idle runtimes. Root creation and actor admission have separate real
        // lifecycle controls; this query must read only these original facts.
        let mut mutations = Vec::new();
        for index in 0..count {
            let agent = AgentPath::root(&AgentId::from_bytes((index + 2).to_be_bytes()));
            mutations.extend([
                CommitMutation::Append {
                    path: acyclic_stream::StreamPath::new(format!(
                        "{}/{}",
                        roots.as_str(),
                        agent.id()
                    ))?,
                    records: vec![
                        serde_json::to_vec(&serde_json::json!({
                            "kind": "root", "version": 1, "agent": agent,
                        }))?
                        .into(),
                    ],
                },
                CommitMutation::Append {
                    path: agent.status()?,
                    records: vec![status.clone()],
                },
                CommitMutation::Append {
                    path: agent.peer_membership()?,
                    records: vec![
                        crate::agent::PeerMembership {
                            epoch: (index >= 16).then_some([7; 32]),
                        }
                        .encode()?
                        .into(),
                    ],
                },
            ]);
            if mutations.len() >= 128 || index + 1 == count {
                assert!(matches!(
                    stream
                        .commit(CommitRequest {
                            deadline_unix_millis: None,
                            effects: vec![acyclic_stream::CommitEffect::hard(
                                Vec::new(),
                                std::mem::take(&mut mutations)
                            )],
                            idempotency_key: IdempotencyKey::unique(),
                        })
                        .await?,
                    CommitOutcome::Committed(_)
                ));
            }
        }
        let client = Client::in_process_with_products(
            stream.clone(),
            acyclic_stream::session::testing::authority(),
            None,
            server.clone(),
            Some(server.clone()),
            Arc::new(acyclic_stream::runtime::PlatformExecutor),
            None,
        );
        client.register_credential(
            0,
            credentials(&[server.resource(), &server.session_resource()]),
        )?;
        let before = stream.requests();
        let Ok(AgentReply::Children { items, next, .. }) = call(
            &client,
            "agents",
            &AgentCommand::Sessions {
                requester: None,
                scope: acyclic_harness_client::SessionsScope::Owned,
                page: AgentPageRequest::new(16, 64 * 1024, None)?,
            },
        )
        .await?
        else {
            return Err("session discovery returned another result".into());
        };
        let work = stream.requests().since(before);
        assert_eq!(items.len(), 16);
        assert!(next.is_some());
        assert!(items.iter().all(|item| item.status.is_some()
            && item.created_at_micros.is_some()
            && item.updated_at_micros.is_some()));
        assert_eq!(
            work.control(),
            2,
            "one hierarchy page and one bounded row view"
        );
        assert_eq!(
            work.data(),
            0,
            "discovery must not read transcripts or volumes"
        );
        // Exercise ordinary client enrollment, rather than supplying the peer
        // grant in a test credential. It must not enroll directory authority.
        let enrolled = server
            .bound()
            .client_credential(
                server.root(),
                server.resource(),
                Arc::new(acyclic_stream::SystemUnixMillisClock),
            )
            .await?;
        let client = Client::in_process_with_products(
            stream.clone(),
            enrolled.authority,
            None,
            server.clone(),
            Some(server.clone()),
            Arc::new(acyclic_stream::runtime::PlatformExecutor),
            None,
        );
        client.register_credential(0, enrolled.credential.into())?;
        assert!(!matches!(
            call(
                &client,
                "agents",
                &AgentCommand::Sessions {
                    requester: None,
                    scope: acyclic_harness_client::SessionsScope::Owned,
                    page: AgentPageRequest::default(),
                }
            )
            .await,
            Ok(Ok(_))
        ));
        let mut cursor = None;
        for expected in [0, 16] {
            let before = stream.requests();
            let AgentReply::Children { items, next, .. } = call(
                &client,
                "agents",
                &AgentCommand::Sessions {
                    requester: None,
                    scope: acyclic_harness_client::SessionsScope::Peers,
                    page: AgentPageRequest::new(16, 64 * 1024, cursor)?,
                },
            )
            .await??
            else {
                return Err("peer discovery returned another result".into());
            };
            let reads = stream.requests().since(before);
            assert_eq!(reads.control(), 2);
            assert_eq!(reads.data(), 0);
            assert_eq!(items.len(), expected);
            if expected == 0 {
                assert!(
                    next.is_some(),
                    "filtered pages must advance without scanning ahead"
                );
                use base64::{Engine as _, engine::general_purpose::STANDARD};
                let mut sealed = STANDARD.decode(
                    next.as_ref()
                        .and_then(|cursor| cursor.strip_prefix(peers::LOCAL_CURSOR))
                        .ok_or("missing local continuation")?,
                )?;
                let hidden = AgentId::from_bytes(17_u128.to_be_bytes()).to_string();
                assert!(
                    !sealed
                        .windows(hidden.len())
                        .any(|bytes| bytes == hidden.as_bytes())
                );
                sealed[25] ^= 1;
                assert!(!matches!(
                    call(
                        &client,
                        "agents",
                        &AgentCommand::Sessions {
                            requester: None,
                            scope: acyclic_harness_client::SessionsScope::Peers,
                            page: AgentPageRequest::new(
                                16,
                                64 * 1024,
                                Some(format!(
                                    "{}{}",
                                    peers::LOCAL_CURSOR,
                                    STANDARD.encode(sealed)
                                ))
                            )?,
                        }
                    )
                    .await,
                    Ok(Ok(_))
                ));
            }
            cursor = next;
        }
        let state = server.bound().agents().state(server.root()).await?;
        let authority = crate::filesystem::agent_authority(
            server.bound().issuer(),
            server.bound().admission(),
            server.root(),
            &state.volumes,
            &[],
        )
        .await?;
        let observation = server
            .bound()
            .volumes()
            .agent_observation(
                &authority,
                server.root(),
                authority.private.volume()?,
                Arc::new(acyclic_stream::SystemUnixMillisClock),
                Some(server.resource()),
            )
            .await?;
        let observer = Client::in_process_with_products(
            stream.clone(),
            observation.authority,
            None,
            server.clone(),
            Some(server.clone()),
            Arc::new(acyclic_stream::runtime::PlatformExecutor),
            None,
        );
        observer.register_credential(0, observation.credential.into())?;
        assert!(matches!(
            call(&observer, "agents", &AgentCommand::Root {}).await?,
            Ok(AgentReply::Root { .. })
        ));
        assert!(!matches!(
            call(
                &observer,
                "agents",
                &AgentCommand::Sessions {
                    requester: None,
                    scope: acyclic_harness_client::SessionsScope::Peers,
                    page: AgentPageRequest::default(),
                }
            )
            .await,
            Ok(Ok(_))
        ));
        server
            .bound()
            .agents()
            .set_peer_membership(
                server.root(),
                &crate::agent::Sender::User {
                    principal: "owner".into(),
                },
                &IntentId::try_from("discovery-off")?,
                false,
            )
            .await?;
        assert!(!matches!(
            call(
                &client,
                "agents",
                &AgentCommand::Sessions {
                    requester: None,
                    scope: acyclic_harness_client::SessionsScope::Peers,
                    page: AgentPageRequest::default(),
                }
            )
            .await,
            Ok(Ok(_))
        ));
        Ok(work)
    }
    assert_eq!(work(32).await?, work(100_000).await?);
    Ok(())
}
