//! Seeded deterministic simulation of the hosted `serve` path.
//!
//! One world is a real memory stream and memory objects behind one admitted
//! local root, served by the production [`serve`] entry point and driven
//! through the session client. A seed draws the probe order, page sizes, the
//! denial point and the staged body; the oracle checks after every command
//! that a denied command touched no storage, an allowed command answered
//! within the admitted tree, a replay returned the original reply, and the
//! read bytes matched the staged body. Replaying a seed must produce the same
//! trace.
use crate::{
    Bound, Placement, RootRequest, Turns,
    agent::{AgentId, AgentPath, IntentId, WorkspaceEntry},
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityRoot},
    filesystem::PRIVATE_VOLUME,
    hosting::{Serving, serve},
    hosting_test_support::{Never, Owner, access_denied, call, credential, provider},
    model::ModelContent,
};
use acyclic_actors::{
    addressing::ActorReference,
    domain::ActorLimits,
    local::{Clock, ControlledClock},
};
use acyclic_fs::{EmbeddedCapabilities, LogicalObjectStore};
use acyclic_harness_client::{
    AgentCommand, AgentControlFailure, AgentPageRequest, AgentReply, CommandError, PreparedCommand,
    ReadTarget, SessionsScope,
};
use acyclic_harness_core::filesystem::TurnLimits;
use acyclic_objects::Blobs;
use acyclic_stream::{MemoryStream, session::Client, testing::Counted};
use std::sync::Arc;
use std::sync::atomic::Ordering;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Seeds in the fast suite. `DST_SEEDS=<a>..<b>` runs a deeper sweep.
const SEEDS: std::ops::Range<u64> = 0..16;

/// An agent outside every world's admission. A world's root repeats one
/// seed byte, so these distinct bytes never name it, whatever the seed.
const OUTSIDE: [u8; 16] = *b"outside-agent-id";

/// A seeded draw stream, independent of the runtime's own entropy.
struct Draws(blake3::OutputReader);

impl Draws {
    fn new(seed: u64) -> Self {
        let mut hash = blake3::Hasher::new_derive_key("acyclic.dst.hosting.serve.v1");
        hash.update(&seed.to_le_bytes());
        Self(hash.finalize_xof())
    }

    fn below(&mut self, bound: usize) -> usize {
        let mut bytes = [0; 8];
        self.0.fill(&mut bytes);
        usize::try_from(u64::from_le_bytes(bytes) % bound as u64).unwrap_or(0)
    }

    fn bool(&mut self) -> bool {
        self.below(2) == 0
    }
}

/// One command the world drives through the served tree.
#[derive(Clone, Copy, Debug)]
enum Step {
    Root,
    Bind,
    Children,
    Usage,
    Events,
    History,
    Approvals,
    Diff,
    Sessions,
    ClientFiles,
    FilePreviews,
    ProjectHeads,
    ChildWakes,
    Settlement,
    PeerAdmission,
    SetPeerMembership(bool),
    PublishAgentFiles,
    Prompt,
    Cancel,
    Spawn,
    MergeSession,
    NewSession,
    ForkSession,
    Pause(bool),
    Reply,
    Decide,
    Archive,
}

/// The seed's probe order: the read-only commands in a seeded permutation,
/// then the mutations in a fixed order so each has its precondition.
fn script(seed: u64) -> Vec<Step> {
    let mut draws = Draws::new(seed);
    let mut probes = vec![
        Step::Root,
        Step::Bind,
        Step::Children,
        Step::Usage,
        Step::Events,
        Step::History,
        Step::Approvals,
        Step::Diff,
        Step::Sessions,
        Step::ClientFiles,
        Step::FilePreviews,
        Step::ProjectHeads,
        Step::ChildWakes,
        Step::Settlement,
        Step::PeerAdmission,
    ];
    for index in (1..probes.len()).rev() {
        let swap = draws.below(index + 1);
        probes.swap(index, swap);
    }
    probes.extend([
        Step::SetPeerMembership(draws.bool()),
        Step::SetPeerMembership(draws.bool()),
        Step::PublishAgentFiles,
        Step::Prompt,
        Step::Cancel,
        Step::Spawn,
        Step::MergeSession,
        Step::NewSession,
        Step::ForkSession,
        Step::Pause(draws.bool()),
        Step::Pause(!draws.bool()),
        Step::Reply,
        Step::Decide,
        Step::Archive,
    ]);
    probes
}

/// Whether replaying `step` must return the original reply without a new
/// record. A replayed query reads again; a replayed intent finds its original
/// outcome. Mutations that commit a new intent are excluded.
const fn idempotent(step: Step) -> bool {
    matches!(
        step,
        Step::Root
            | Step::Bind
            | Step::Children
            | Step::Usage
            | Step::Events
            | Step::History
            | Step::Approvals
            | Step::Diff
            | Step::Sessions
            | Step::ClientFiles
            | Step::FilePreviews
            | Step::ProjectHeads
            | Step::ChildWakes
            | Step::Settlement
            | Step::PeerAdmission
            | Step::SetPeerMembership(_)
            | Step::PublishAgentFiles
            | Step::Archive
    )
}

/// The refusal of a command that must not answer, from whichever layer
/// refused it: the command's own reply, or the session call that carries it.
/// A revocation found after dispatch fails the call itself, before any reply
/// byte, so both layers are the same refusal.
fn refusal(
    result: Result<std::result::Result<AgentReply, AgentControlFailure>>,
    answered: impl FnOnce(&AgentReply) -> String,
) -> Result<Box<dyn std::error::Error>> {
    match result {
        Ok(Ok(reply)) => Err(answered(&reply).into()),
        Ok(Err(refused)) => Ok(Box::new(refused)),
        Err(refused) => Ok(refused),
    }
}

/// One served world: the counted stream, the object store, the platform, the
/// server and the client that calls it.
struct World {
    stream: Arc<Counted<MemoryStream>>,
    objects: Arc<Counted<acyclic_objects::MemoryObjects>>,
    owner: Arc<Owner>,
    server: Arc<crate::Server<Counted<MemoryStream>, LogicalObjectStore<dyn Blobs>>>,
    client: Client,
    /// Holds the session directory's authority as well as the tree's.
    directory: Client,
    root: AgentPath,
    trace: Vec<String>,
}

impl World {
    async fn new(seed: u64) -> Result<Self> {
        let stream = Arc::new(Counted::new(MemoryStream::default()));
        let objects = Arc::new(Counted::new(acyclic_objects::MemoryObjects::memory()));
        let blobs: Arc<dyn Blobs> = objects.clone();
        let (clock, _control) = ControlledClock::new(1_700_000_000_000);
        let bound: Bound<Counted<MemoryStream>, LogicalObjectStore<dyn Blobs>> = Bound::local_root(
            AuthorityRoot::new(
                "agents",
                [7; 32],
                Authority {
                    kind: AggregateKind::Conversation,
                    id: "local".into(),
                },
            ),
            Placement {
                filesystem: acyclic_fs::Fs::new(
                    acyclic_fs::StreamAuthorityStore::new(stream.clone()),
                    LogicalObjectStore::new(blobs),
                    EmbeddedCapabilities::MEMORY,
                ),
                provider: provider()?,
                within: ActorReference::new(b"account", b"agents", b"owner")?,
            },
            Turns {
                executors: Arc::new(Never),
                limits: TurnLimits {
                    maximum_bytes: Some(64 * 1024),
                    history_turns: 8,
                },
                actors: ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
                clock: Arc::new(clock) as Arc<dyn Clock>,
                approval_policy: Default::default(),
            },
        )?;
        let agent = AgentId::from_bytes([seed.to_le_bytes()[0]; 16]);
        let private = WorkspaceEntry::initial_volume(
            crate::agent::WorkspaceRole::Private,
            PRIVATE_VOLUME.into(),
            VolumeRef::new(
                provider()?,
                format!("agent-private-{seed}"),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(crate::AgentId::from_bytes(agent.to_bytes())),
                &mut None,
            )?,
        );
        let owner = Arc::new(Owner::default());
        owner.sessions.store(true, Ordering::SeqCst);
        let server = serve(
            bound,
            owner.clone(),
            &IntentId::try_from(format!("root-{seed}").as_str())?,
            RootRequest {
                peer: true,
                agent,
                fresh: vec![private],
                retained: Vec::new(),
                brief: None,
                policy: None,
            },
        )
        .await?;
        let Serving::Served(server) = server else {
            return Err("serving the root was uncertain".into());
        };
        let client = Client::in_process_with_products(
            stream.clone(),
            acyclic_stream::session::testing::authority(),
            None,
            server.clone(),
            Some(server.clone()),
            Arc::new(acyclic_stream::runtime::PlatformExecutor),
            None,
        );
        client.register_credential(0, credential(server.resource()))?;
        // A tree credential never acquires directory authority, so the
        // session directory's owner pages sessions through its own client.
        let directory = Client::in_process_with_products(
            stream.clone(),
            acyclic_stream::session::testing::authority(),
            None,
            server.clone(),
            Some(server.clone()),
            Arc::new(acyclic_stream::runtime::PlatformExecutor),
            None,
        );
        directory.register_credential(
            0,
            crate::hosting_test_support::credentials(&[
                server.resource(),
                &server.session_resource(),
            ]),
        )?;
        let root = server.root().clone();
        Ok(Self {
            stream,
            objects,
            owner,
            server,
            client,
            directory,
            root,
            trace: Vec::new(),
        })
    }

    fn page(&self, items: u32) -> Result<AgentPageRequest> {
        Ok(AgentPageRequest::new(items, 32 * 1024, None)?)
    }

    fn command(&self, step: Step) -> Result<AgentCommand> {
        let root = self.root.clone();
        Ok(match step {
            Step::Root => AgentCommand::Root {},
            Step::Bind => AgentCommand::Bind { agent: None },
            Step::Children => AgentCommand::Children {
                agent: root,
                page: self.page(1)?,
            },
            Step::Usage => AgentCommand::Usage {
                agent: root,
                input: 0,
                after: 0,
                wait: false,
                observed_children: Some(0),
            },
            Step::Events => AgentCommand::Events {
                agent: root,
                input: 0,
                after: 0,
                wait: false,
            },
            Step::History => AgentCommand::History {
                agent: root,
                page: self.page(1)?,
            },
            Step::Approvals => AgentCommand::Approvals {
                agent: root,
                page: self.page(1)?,
            },
            Step::Diff => AgentCommand::Diff {
                agent: root,
                page: self.page(1)?,
            },
            Step::Sessions => AgentCommand::Sessions {
                requester: Some(root),
                scope: SessionsScope::Owned,
                page: self.page(1)?,
            },
            Step::ClientFiles => AgentCommand::ClientFiles {
                agent: root,
                root: acyclic_harness_core::client_files::FileRoot::Private {},
                publication: None,
                paths: vec!["ui/title".into()],
                after: None,
            },
            Step::FilePreviews => AgentCommand::AgentFilePreviews {
                agent: root,
                paths: vec!["ui/title".into()],
                after: None,
                bytes: 64,
            },
            Step::ProjectHeads => AgentCommand::ProjectHeads { agent: root },
            Step::ChildWakes => AgentCommand::ChildWakes {
                agent: root,
                status: 0,
                page: self.page(1)?,
            },
            Step::Settlement => AgentCommand::Settlement {
                agent: root,
                wait: false,
            },
            Step::PeerAdmission => AgentCommand::PreparePeer {
                sender: root.clone(),
                agent: root,
            },
            Step::SetPeerMembership(enabled) => AgentCommand::SetPeerMembership {
                agent: root,
                id: IntentId::try_from(if enabled { "peers-on" } else { "peers-off" })?,
                enabled,
            },
            Step::PublishAgentFiles => AgentCommand::PublishAgentFiles {
                agent: root,
                operation: crate::OperationId::from_bytes([3; 16]),
                changes: vec![crate::client_files::ClientFileChange {
                    path: "ui/title".into(),
                    expected: None,
                    text: Some("seeded title".into()),
                }],
            },
            Step::Prompt => AgentCommand::Prompt {
                peer: None,
                agent: root,
                id: IntentId::try_from("prompt-1")?,
                body: ModelContent::Text("hello".into()),
            },
            Step::Cancel => AgentCommand::Cancel {
                conversation: 0,
                input: 1,
                agent: root,
                id: IntentId::try_from("cancel-1")?,
            },
            Step::Spawn => AgentCommand::Spawn {
                parent: root,
                id: IntentId::try_from("spawn-1")?,
                brief: ModelContent::Text("child".into()),
                role: "worker".into(),
            },
            Step::MergeSession => AgentCommand::MergeSession {
                agent: root,
                id: IntentId::try_from("merge-1")?,
            },
            Step::NewSession => AgentCommand::NewSession {
                questions: None,
                id: IntentId::try_from("new-session-1")?,
                peer: true,
            },
            Step::ForkSession => AgentCommand::ForkSession {
                agent: root,
                id: IntentId::try_from("fork-session-1")?,
                at: 0,
                peer: true,
            },
            Step::Pause(paused) => AgentCommand::Pause {
                agent: root,
                paused,
            },
            Step::Reply => AgentCommand::Reply {
                agent: root,
                id: IntentId::try_from("reply-1")?,
                inbox: 0,
            },
            Step::Decide => AgentCommand::Decide {
                agent: root,
                id: IntentId::try_from("approval-1")?,
                binding: crate::interaction::ApprovalBinding {
                    operation_id: crate::OperationId::from_bytes([3; 16]),
                    action_digest: [4; 32],
                },
                verdict: true.into(),
                reason: None,
            },
            Step::Archive => AgentCommand::Archive {
                agent: root,
                id: IntentId::try_from("archive-1")?,
            },
        })
    }

    /// The stream's authority revision. It advances on every commit, so an
    /// unchanged revision proves that a command wrote nothing. It is read
    /// from the uncounted provider, so it costs no counted request.
    async fn revision(&self) -> Result<acyclic_stream::Revision> {
        let heads = acyclic_stream::StreamProvider::heads(
            self.stream.inner(),
            acyclic_stream::HeadsRequest {
                at: None,
                retained: Default::default(),
                durable_through: None,
                projection: acyclic_stream::HeadProjection::Tail,
                paths: Default::default(),
                children: None,
                limit: 1,
                at_least: Default::default(),
            },
        )
        .await?;
        Ok(heads.snapshot)
    }

    /// Drives one step, optionally denying it, and checks the storage fence.
    async fn step(&mut self, step: Step, deny: bool) -> Result<()> {
        let command = self.command(step)?;
        let before = self.stream.requests();
        self.owner.denied.store(deny, Ordering::SeqCst);
        // A spawn is answered by the parent's own step, so the server runs
        // while it is in flight, as it does for every admitted spawn.
        let runner = (!deny && matches!(step, Step::Spawn)).then(|| {
            let server = self.server.clone();
            tokio::spawn(async move { server.run(|_, _| {}).await })
        });
        let client = if matches!(
            step,
            Step::Sessions | Step::MergeSession | Step::NewSession | Step::ForkSession
        ) {
            &self.directory
        } else {
            &self.client
        };
        let result = call(client, "agents", &command).await;
        if let Some(runner) = runner {
            runner.abort();
            let _ = runner.await;
        }
        self.owner.denied.store(false, Ordering::SeqCst);
        let requests = self.stream.requests().since(before);
        if deny {
            assert!(
                !matches!(&result, Ok(Ok(_))),
                "denied {step:?} answered: {result:?}"
            );
            assert_eq!(
                (requests.data(), requests.control()),
                (0, 0),
                "denied {step:?} touched storage"
            );
            self.trace.push(format!("{step:?}:denied"));
            return Ok(());
        }
        let reply = result?;
        self.trace.push(format!("{step:?}:{reply:?}"));
        if idempotent(step) {
            let before = self.revision().await?;
            let replay = call(client, "agents", &command).await?;
            assert_eq!(
                format!("{reply:?}"),
                format!("{replay:?}"),
                "replaying {step:?} changed its reply"
            );
            assert_eq!(
                before,
                self.revision().await?,
                "replaying {step:?} committed a new record"
            );
        }
        Ok(())
    }

    /// The dispatch scenario: every probe and mutation in the seed's order,
    /// with one seeded denial.
    async fn dispatch(seed: u64) -> Result<Vec<String>> {
        let mut world = Self::new(seed).await?;
        let steps = script(seed);
        let mut draws = Draws::new(seed ^ 0x5eed);
        let denied = draws.below(steps.len());
        for (index, step) in steps.into_iter().enumerate() {
            world.step(step, index == denied).await?;
        }
        Ok(world.trace)
    }

    /// The read scenario: stage a seeded body, read it at seeded offsets, and
    /// check that a denied read and a denied staged head touch no storage.
    async fn reads(seed: u64) -> Result<Vec<String>> {
        use crate::conversation::{ContentAccess, ContentPublisher};
        use acyclic_harness_client::MAX_READ_BYTES;

        let world = Self::new(seed).await?;
        let mut draws = Draws::new(seed ^ 0x4ead);
        let length = 1 + draws.below(usize::try_from(MAX_READ_BYTES)? + 17);
        let body: Vec<u8> = (0..length).map(|index| index.to_le_bytes()[0]).collect();
        let publisher = world
            .server
            .bound()
            .tool_output_publisher(&world.root)
            .await?;
        let path = "tools/turn/stdout";
        let (file, _) = publisher
            .stage_once(
                crate::OperationId::from_bytes([4; 16]),
                path,
                &body,
                "application/octet-stream",
                "stdout",
                ContentAccess::default(),
            )?
            .await?;
        let offsets = [0, draws.below(length), length.saturating_sub(1), length];
        for offset in offsets {
            for target in [
                ReadTarget::ToolOutput {
                    path: path.into(),
                    at: None,
                },
                ReadTarget::File { file: file.clone() },
            ] {
                let reply = call(
                    &world.client,
                    "agents",
                    &AgentCommand::Read {
                        agent: world.root.clone(),
                        target,
                        offset: offset as u64,
                    },
                )
                .await??;
                let (bytes, total) = reply.content()?;
                assert_eq!(total, body.len() as u64);
                let expected: Vec<u8> = body
                    .get(offset..)
                    .ok_or("a read offset past the body")?
                    .iter()
                    .take(usize::try_from(MAX_READ_BYTES)?)
                    .copied()
                    .collect();
                assert_eq!(bytes, expected, "read at {offset} of {length}");
            }
        }
        // A read revoked after admission may read its source, but the
        // revalidation before disclosure refuses it, and it commits nothing.
        world.owner.revoke_read.store(true, Ordering::SeqCst);
        let before = world.revision().await?;
        let denied = AgentCommand::Read {
            agent: world.root.clone(),
            target: ReadTarget::ToolOutput {
                path: path.into(),
                at: None,
            },
            offset: 0,
        };
        let refused = refusal(call(&world.client, "agents", &denied).await, |reply| {
            format!("content disclosed after owner revocation: {reply:?}")
        })?;
        assert!(access_denied(&*refused), "{refused:?}");
        assert_eq!(
            before,
            world.revision().await?,
            "a revoked read committed a record"
        );
        world.owner.denied.store(false, Ordering::SeqCst);
        // A journal file outside the tool output prefix is never tool output.
        publisher
            .stage_once(
                crate::OperationId::from_bytes([5; 16]),
                "notes/private",
                b"not tool output",
                "text/plain",
                "private",
                ContentAccess::default(),
            )?
            .await?;
        world.refused_reads(&file, path).await?;
        world.denied_staged_head().await?;
        Ok(world.trace)
    }

    /// Reads the agent or its admission refuses: traversal, unpublished paths, foreign agents, foreign files and foreign credentials.
    async fn refused_reads(&self, file: &crate::conversation::FileRef, path: &str) -> Result {
        let world = self;
        // Traversal is refused by the original command admission before transport.
        assert!(matches!(
            PreparedCommand::new(&AgentCommand::Read {
                agent: world.root.clone(),
                target: ReadTarget::ToolOutput {
                    path: "../outside".into(),
                    at: None,
                },
                offset: 0,
            }),
            Err(CommandError::Invalid)
        ));
        let read = |agent: AgentPath, path: &str| AgentCommand::Read {
            agent,
            target: ReadTarget::ToolOutput {
                path: path.into(),
                at: None,
            },
            offset: 0,
        };
        // The agent itself refuses paths it never published as tool output.
        for path in ["tools/missing", "notes/private"] {
            assert!(
                call(&world.client, "agents", &read(world.root.clone(), path))
                    .await?
                    .is_err()
            );
        }
        // An agent outside the served admission is refused by the command admission.
        let outside = read(AgentPath::root(&AgentId::from_bytes(OUTSIDE)), path);
        let refused = refusal(call(&world.client, "agents", &outside).await, |reply| {
            format!("an agent outside the admission answered a read: {reply:?}")
        })?;
        assert!(access_denied(&*refused), "{refused:?}");
        // A file from another agent's volume is refused by the agent.
        let foreign = VolumeRef::new(
            provider()?,
            "another-private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes(OUTSIDE)),
            &mut None,
        )?;
        let foreign = crate::conversation::FileRef::new(
            foreign,
            file.path().into(),
            file.version().into(),
            file.descriptor().clone(),
            file.display_name().into(),
            &mut None,
        )?;
        assert!(
            call(
                &world.client,
                "agents",
                &AgentCommand::Read {
                    agent: world.root.clone(),
                    target: ReadTarget::File { file: foreign },
                    offset: 0,
                }
            )
            .await?
            .is_err()
        );
        // A credential for another server is refused by the command admission.
        let denied = Client::in_process_with_products(
            world.stream.clone(),
            acyclic_stream::session::testing::authority(),
            None,
            world.server.clone(),
            Some(world.server.clone()),
            Arc::new(acyclic_stream::runtime::PlatformExecutor),
            None,
        );
        denied.register_credential(0, credential("actor:/another-server"))?;
        assert!(
            call(
                &denied,
                "agents",
                &AgentCommand::Read {
                    agent: world.root.clone(),
                    target: ReadTarget::ToolOutput {
                        path: path.into(),
                        at: None
                    },
                    offset: 0,
                }
            )
            .await
            .is_err()
        );
        Ok(())
    }

    /// A denied staged head reads no part, however many parts it names.
    async fn denied_staged_head(&self) -> Result {
        let world = self;
        let digest = blake3::hash(b"{}").to_hex().to_string();
        let stage = AgentCommand::Stage {
            agent: world.root.clone(),
            digest: digest.clone(),
            part: 0,
            text: "[]".into(),
        };
        assert!(matches!(
            call(&world.client, "agents", &stage).await?,
            Ok(AgentReply::Staged {})
        ));
        let head = AgentCommand::Staged {
            agent: world.root.clone(),
            command: "prompt".into(),
            digest,
            parts: 1,
            bytes: 2,
        };
        let before = world.objects.requests();
        assert!(call(&world.client, "agents", &head).await?.is_err());
        assert!(
            world.objects.requests() != before,
            "an admitted head reads its parts"
        );
        world.owner.denied.store(true, Ordering::SeqCst);
        let before = world.objects.requests();
        assert!(call(&world.client, "agents", &head).await.is_err());
        assert!(
            world.objects.requests() == before,
            "a denied head reads no part"
        );
        world.owner.denied.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// The authority scenario: a peer credential, a foreign tree, a revoked
    /// query, a suspended admission, a suspended diff source, a childless
    /// spawn, a spawn retry and a closed input all refuse before storage or
    /// settle.
    async fn authority(seed: u64) -> Result<Vec<String>> {
        let world = Self::new(seed).await?;
        world.peer_and_foreign_refusals().await?;
        world.revoked_child_queries().await?;
        world.revoked_diff_query().await?;
        world.empty_pages_and_spawns().await?;
        world.closed_input_settles().await?;
        Ok(world.trace)
    }

    /// A peer credential reaches no user command, and a foreign tree needs its own credential; neither touches storage.
    async fn peer_and_foreign_refusals(&self) -> Result {
        let world = self;
        // A peer credential may discover peers but never a user command.
        let peer = Client::in_process_with_products(
            world.stream.clone(),
            acyclic_stream::session::testing::authority(),
            None,
            world.server.clone(),
            Some(world.server.clone()),
            Arc::new(acyclic_stream::runtime::PlatformExecutor),
            None,
        );
        peer.register_credential(0, credential("actors:/peer-directory/account"))?;
        for command in [
            AgentCommand::SetPeerMembership {
                agent: world.root.clone(),
                id: IntentId::try_from("peer-cannot-opt-out")?,
                enabled: false,
            },
            AgentCommand::Archive {
                agent: world.root.clone(),
                id: IntentId::try_from("peer-cannot-archive")?,
            },
            AgentCommand::Approvals {
                agent: world.root.clone(),
                page: world.page(1)?,
            },
            AgentCommand::Children {
                agent: world.root.clone(),
                page: world.page(1)?,
            },
            AgentCommand::PublishAgentFiles {
                agent: world.root.clone(),
                operation: crate::OperationId::from_bytes([3; 16]),
                changes: vec![crate::client_files::ClientFileChange {
                    path: "ui/title".into(),
                    expected: None,
                    text: Some("forged title".into()),
                }],
            },
        ] {
            let before = world.stream.requests();
            let denied = call(&peer, "agents", &command).await;
            assert!(
                !matches!(&denied, Ok(Ok(_))),
                "peer authority reached a user command: {denied:?}"
            );
            let requests = world.stream.requests().since(before);
            assert_eq!(
                (requests.data(), requests.control()),
                (0, 0),
                "peer authority observed storage"
            );
        }
        // A foreign tree needs its own credential: refuse before dispatch.
        let stranger = AgentPath::root(&AgentId::from_bytes(OUTSIDE));
        let before = world.stream.requests();
        // The session's credential does not cover the stranger's actor, so the
        // refusal is the call's own, before any command reply.
        let cancel = AgentCommand::Cancel {
            conversation: 0,
            input: 1,
            agent: stranger,
            id: IntentId::try_from("foreign-cancel")?,
        };
        let refused = refusal(call(&world.client, "agents", &cancel).await, |reply| {
            format!("a foreign tree needs its own credential: {reply:?}")
        })?;
        assert!(access_denied(&*refused), "{refused:?}");
        let requests = world.stream.requests().since(before);
        assert_eq!(
            (requests.data(), requests.control()),
            (0, 0),
            "a foreign tree observed storage"
        );
        Ok(())
    }

    /// A revoked query, and a suspended child admission revoked while it waits, both refuse.
    async fn revoked_child_queries(&self) -> Result {
        let world = self;
        // A revoked query is refused after admission.
        world.owner.revoke_query.store(true, Ordering::SeqCst);
        let children = AgentCommand::Children {
            agent: world.root.clone(),
            page: world.page(1)?,
        };
        let refused = refusal(call(&world.client, "agents", &children).await, |reply| {
            format!("a revoked query answered: {reply:?}")
        })?;
        assert!(access_denied(&*refused), "{refused:?}");
        world.owner.denied.store(false, Ordering::SeqCst);
        // A suspended admission that is revoked refuses the original query.
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let (release, resume) = tokio::sync::oneshot::channel();
        *world
            .owner
            .admission
            .lock()
            .map_err(|_| "admission control poisoned")? =
            Some(crate::hosting_test_support::QueryPause {
                entered,
                release: resume,
            });
        let command = AgentCommand::Children {
            agent: world.root.clone(),
            page: world.page(1)?,
        };
        let query = call(&world.client, "agents", &command);
        tokio::pin!(query);
        tokio::select! {
            _ = &mut query => return Err("child query ended before admission suspended".into()),
            entered = waiting => entered?,
        }
        world.owner.denied.store(true, Ordering::SeqCst);
        release
            .send(())
            .map_err(|()| "child admission closed before release")?;
        let refused = refusal(query.await, |reply| {
            format!("a suspended child query disclosed after revocation: {reply:?}")
        })?;
        assert!(access_denied(&*refused), "{refused:?}");
        world.owner.denied.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// A suspended diff source that is revoked refuses the original query.
    async fn revoked_diff_query(&self) -> Result {
        let world = self;
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let (release, resume) = tokio::sync::oneshot::channel();
        *world
            .owner
            .diff
            .lock()
            .map_err(|_| "diff control poisoned")? =
            Some(crate::hosting_test_support::QueryPause {
                entered,
                release: resume,
            });
        let command = AgentCommand::Diff {
            agent: world.root.clone(),
            page: world.page(1)?,
        };
        let query = crate::hosting_test_support::diff_call(&world.client, "agents", &command);
        tokio::pin!(query);
        tokio::select! {
            _ = &mut query => return Err("diff query ended before its source suspended".into()),
            entered = waiting => entered?,
        }
        world.owner.denied.store(true, Ordering::SeqCst);
        drop(release);
        let refused = refusal(query.await, |reply| {
            format!("a suspended diff query disclosed after revocation: {reply:?}")
        })?;
        assert!(access_denied(&*refused), "{refused:?}");
        world.owner.denied.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// Empty pages reserve their envelope, a childless host refuses spawns, and a spawn retry names the same child.
    async fn empty_pages_and_spawns(&self) -> Result {
        let world = self;
        // An empty page must reserve its complete canonical envelope.
        for query in [
            AgentCommand::History {
                agent: world.root.clone(),
                page: AgentPageRequest::new(1, 1, None)?,
            },
            AgentCommand::Children {
                agent: world.root.clone(),
                page: AgentPageRequest::new(1, 1, None)?,
            },
        ] {
            let before = world.stream.requests();
            assert!(matches!(
                call(&world.client, "agents", &query).await?,
                Err(AgentControlFailure::Refused { code, .. })
                    if code == crate::agent::Error::Stream(acyclic_stream::StreamError::Capacity).code()
            ));
            assert_eq!(
                world.stream.requests().since(before),
                Default::default(),
                "insufficient empty-page credit must refuse before source requests"
            );
        }
        // A childless host refuses the spawn itself, typed, and commits
        // nothing: no child is created to wait forever.
        let spawn = AgentCommand::Spawn {
            parent: world.root.clone(),
            id: IntentId::try_from("s1")?,
            brief: ModelContent::Text("child".into()),
            role: "worker".into(),
        };
        world.owner.childless.store(true, Ordering::SeqCst);
        assert!(matches!(
            call(&world.client, "agents", &spawn).await?,
            Err(AgentControlFailure::Refused { code, .. }) if code == "spawn_refused"
        ));
        world.owner.childless.store(false, Ordering::SeqCst);
        // A spawn is the parent's own step: the reply names the child, and a
        // retry names the same child.
        let runner = {
            let server = world.server.clone();
            tokio::spawn(async move { server.run(|_, _| {}).await })
        };
        let Ok(AgentReply::Spawned { agent: child, .. }) =
            call(&world.client, "agents", &spawn).await?
        else {
            return Err("spawn was not delivered".into());
        };
        assert!(
            matches!(call(&world.client, "agents", &spawn).await?, Ok(AgentReply::Spawned { agent, .. }) if agent == child),
            "a retried spawn names the same child"
        );
        runner.abort();
        let _ = runner.await;
        // The committed result already names the child, with its status.
        assert!(matches!(
            call(
                &world.client,
                "agents",
                &AgentCommand::Children {
                    agent: world.root.clone(),
                    page: world.page(1)?,
                }
            )
            .await?,
            Ok(AgentReply::Children { items, next: None })
                if items.len() == 1 && items.first().is_some_and(|item| item.agent == child && item.status.is_some())
        ));
        Ok(())
    }

    /// A closed input settles both live queries.
    async fn closed_input_settles(&self) -> Result {
        let world = self;
        // The archive settles in the server's own step, so the server runs
        // until both live queries have observed it.
        let runner = {
            let server = world.server.clone();
            tokio::spawn(async move { server.run(|_, _| {}).await })
        };
        call(
            &world.client,
            "agents",
            &AgentCommand::Archive {
                agent: world.root.clone(),
                id: IntentId::try_from("close-usage-source")?,
            },
        )
        .await??;
        let end = crate::agent::inbox_tail(&*world.stream, &world.root).await?;
        assert!(matches!(
            call(
                &world.client,
                "agents",
                &AgentCommand::Usage {
                    agent: world.root.clone(),
                    input: end,
                    after: 0,
                    wait: true,
                    observed_children: Some(0),
                }
            )
            .await?,
            Ok(AgentReply::Usage {
                page: None,
                settled: true,
                ..
            })
        ));
        assert!(matches!(
            call(
                &world.client,
                "agents",
                &AgentCommand::Events {
                    agent: world.root.clone(),
                    input: end,
                    after: 0,
                    wait: true,
                }
            )
            .await?,
            Ok(AgentReply::Events {
                page: None,
                settled: true,
                ..
            })
        ));
        runner.abort();
        let _ = runner.await;
        Ok(())
    }
}

/// Runs `scenario` once per seed, each in its own seeded world, so its ids,
/// `select!` order and time come from that seed.
fn each_seed<F: Future<Output = Result<Vec<String>>>>(scenario: impl Fn(u64) -> F) -> Result {
    for seed in SEEDS {
        acyclic_native_runtime::testing::seeded_test(seed, || scenario(seed))??;
    }
    Ok(())
}

#[test]
fn seeded_serve_dispatch_holds_the_oracle() -> Result {
    each_seed(World::dispatch)
}

#[test]
fn seeded_serve_reads_hold_the_oracle() -> Result {
    each_seed(World::reads)
}

#[test]
fn seeded_serve_authority_holds_the_oracle() -> Result {
    each_seed(World::authority)
}

#[test]
fn seeded_serve_replays_identically() -> Result {
    for seed in SEEDS {
        let run = || acyclic_native_runtime::testing::seeded_test(seed, || World::dispatch(seed));
        assert_eq!(run()??, run()??, "seed {seed} did not replay identically");
    }
    Ok(())
}
