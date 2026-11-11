use super::*;
use crate::agent::{AgentId, AgentPath, AgentRecord, Phase, StatusRecord};
use crate::ui::{Children, Result as UiResult};
use acyclic_fs::{
    CancellationToken, Fs, IdempotencyKey, LazyWorkspace, Lifecycle, MemoryLazyWorkspaceStore,
    NativeMaterializationWork, OperationId, PublicationPermit, VolumeConfig, VolumeInitialization,
    VolumeLimits, WorkBudget,
};
use acyclic_stream::{
    AppendRequest, ChildrenPageRequest, MemoryStream, ReadRequest, StreamPath, StreamProvider,
};
use futures::StreamExt;
use std::{borrow::Cow, collections::BTreeMap, sync::Arc};

type TestResult = Result<(), Box<dyn std::error::Error>>;
struct Data {
    status: StatusRecord,
    children: Vec<StreamPath>,
    links: BTreeMap<String, Vec<u8>>,
}
#[derive(Clone)]
struct Snapshot(Arc<Data>);
impl Cut for Snapshot {
    fn status(&self) -> UiResult<Cow<'_, StatusRecord>> {
        Ok(Cow::Borrowed(&self.0.status))
    }
    fn tail(&self) -> Option<u64> {
        Some(0)
    }
    fn turn(&self, _: u64) -> UiResult<Cow<'_, str>> {
        Err(Error::Pending)
    }
    fn children(&self, page: u32) -> UiResult<Option<Children<'_>>> {
        Ok((page == 0).then_some(Children {
            children: &self.0.children,
            more: false,
        }))
    }
}
impl FilesystemCut for Snapshot {
    fn identity(&self) -> [u8; 16] {
        [7; 16]
    }
    fn child_ui(&self, id: &str) -> UiResult<Option<&[u8]>> {
        Ok(self.0.links.get(id).map(Vec::as_slice))
    }
}

// A frozen cut prepared from the real stream below. This tests the shipping
// View/Computed/LazyWorkspace paths independently of transport cache policy.
#[derive(Clone)]
struct Frozen(Snapshot);
impl ViewProvider for Frozen {
    type Snapshot = Snapshot;
    type Record = Affected;
    fn identity(&self) -> [u8; 16] {
        self.0.identity()
    }
    async fn reference(
        &self,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<SourceReference, DemandError> {
        ctx.check()?;
        Ok(SourceReference::new(self.identity(), 1))
    }
    fn cut(
        &self,
        reference: &SourceReference,
        _: &mut KernelAccounting<'_>,
    ) -> Result<ViewCut, DemandError> {
        if reference != &SourceReference::new(self.identity(), 1) {
            return Err(DemandError::StaleSource);
        }
        Ok(ViewCut {
            sequence: 0,
            generation: [0; 32],
        })
    }
    async fn snapshot(
        &self,
        _: SourceReference,
        _: ViewQuery<'_>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Snapshot, DemandError> {
        ctx.check()?;
        Ok(self.0.clone())
    }
    fn watch(
        &self,
        _: Arc<dyn ViewChangeSink<Affected>>,
        _: &mut KernelAccounting<'_>,
    ) -> Result<Option<Box<dyn SourceWatch>>, DemandError> {
        Ok(None)
    }
}

async fn prepared() -> Result<Snapshot, Box<dyn std::error::Error>> {
    let memory = MemoryStream::default();
    let agent = AgentPath::root(&AgentId::from_bytes([1; 16]));
    let child = agent.child(&AgentId::from_bytes([2; 16]))?;
    let status = StatusRecord {
        child_wakes: Vec::new(),
        phase: Phase::WaitingUser {
            path: "réponse.md".into(),
        },
        cursor: 0,
        conversation: 0,
    };
    for (path, payload) in [
        (agent.status()?, status.encode()?),
        (StreamPath::new(child.as_str())?, Vec::new()),
    ] {
        memory
            .append(AppendRequest {
                path,
                records: vec![payload.into()],
                if_tail: None,
                idempotency_key: acyclic_stream::IdempotencyKey::unique(),
            })
            .await?;
    }
    let mut records = memory
        .read(ReadRequest {
            key: None,
            path: agent.status()?,
            from: 0,
            limit: 1,
            at_least: Default::default(),
        })
        .await?;
    let record = records.next().await.ok_or("missing committed status")??;
    let status = StatusRecord::decode(&record.value)?;
    let page = memory
        .children_page(ChildrenPageRequest {
            tail_stream: None,
            parent: Some(agent.children()?),
            after: None,
            hierarchy_version: None,
            limit: 1,
        })
        .await?;
    let children = page
        .children
        .into_iter()
        .map(|child| child.path)
        .collect::<Vec<_>>();
    let id = child.id().to_string();
    Ok(Snapshot(Arc::new(Data {
        status,
        children,
        links: BTreeMap::from([(id.clone(), format!("/agent/{id}/ui").into_bytes())]),
    })))
}
fn computed(snapshot: Snapshot) -> Computed<FilesystemView<Snapshot>, Frozen> {
    Computed::new(
        Arc::new(FilesystemView::new(View::new(2).unwrap())),
        Frozen(snapshot),
    )
}
fn path(text: &str) -> NamespacePath {
    NamespacePath::from_text_in_profile(
        text,
        acyclic_fs::FilesystemProfile::Portable,
        VolumeLimits::default(),
    )
    .unwrap()
}
fn accounting(work: &NativeMaterializationWork) -> KernelAccounting<'_> {
    KernelAccounting::NativeOriginal {
        work,
        cancellation: work.cancellation(),
    }
}

#[tokio::test]
async fn computed_ui_native_ranges_links_and_original_custody() -> TestResult {
    let snapshot = prepared().await?;
    let expected = View::new(2)?.project(&snapshot, &"status.md")?;
    let source = computed(snapshot.clone());
    let cancel = CancellationToken::new();
    let native = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancel)
        .map_err(|(error, _)| error)?;
    let baseline = native.live_allocation_bytes();
    let reference = source.reference(&mut accounting(&native)).await?;
    let status = path("/status.md");
    let node = source
        .lookup(reference.clone(), &status, &native, &cancel)
        .await?
        .ok_or("missing default")?;
    assert_eq!(node.logical_bytes, Some(expected.len() as u64));
    assert_eq!(native.live_allocation_bytes(), baseline);
    let output = source
        .read_range(
            reference.clone(),
            &status,
            node.version,
            0,
            u64::MAX,
            &native,
            &cancel,
        )
        .await?;
    assert_eq!(output.bytes.as_slice(), expected.as_bytes());
    let alias = output.bytes.clone();
    drop(output);
    assert!(
        native.live_allocation_bytes() > baseline,
        "last output alias must retain its original credit"
    );
    drop(alias);
    assert_eq!(native.live_allocation_bytes(), baseline);
    let start = expected.find('é').ok_or("UTF8 fixture")? + 1;
    let output = source
        .read_range(
            reference.clone(),
            &status,
            node.version,
            start as u64,
            1,
            &native,
            &cancel,
        )
        .await?;
    assert_eq!(
        output.bytes.as_slice(),
        &expected.as_bytes()[start..start + 1]
    );
    drop(output);

    let children = path("/children");
    let mut first = source
        .list_page(reference.clone(), &children, None, 1, &native, &cancel)
        .await?;
    assert_eq!(first.entries[0].node.kind, SourceNodeKind::SymbolicLink);
    let link = path(&format!(
        "/children/{}",
        snapshot.0.links.first_key_value().ok_or("child")?.0
    ));
    let target = source
        .read_link(
            reference.clone(),
            &link,
            first.entries[0].node.version,
            4096,
            &native,
            &cancel,
        )
        .await?;
    assert_eq!(
        target.bytes.as_slice(),
        snapshot.0.links.first_key_value().ok_or("target")?.1
    );
    drop(target);
    let next = first.next.take().ok_or("pages navigation")?;
    drop(first);
    let last = source
        .list_page(
            reference.clone(),
            &children,
            Some(next),
            1,
            &native,
            &cancel,
        )
        .await?;
    assert_eq!(last.entries[0].name.as_bytes(), b"pages");
    assert!(last.next.is_none());
    drop(last);
    let pages = source
        .list_page(
            reference.clone(),
            &path("/children/pages"),
            None,
            1,
            &native,
            &cancel,
        )
        .await?;
    assert_eq!(pages.entries[0].name.as_bytes(), b"0.md");
    drop(pages);
    assert!(
        source
            .lookup(reference.clone(), &path("/turns/0.md"), &native, &cancel)
            .await?
            .is_none()
    );
    assert!(matches!(
        source
            .read_link(
                reference.clone(),
                &link,
                SourceVersion([0; 32]),
                4096,
                &native,
                &cancel
            )
            .await,
        Err(DemandError::StaleVersion)
    ));
    assert_eq!(native.live_allocation_bytes(), baseline);

    let denied = NativeMaterializationWork::new(
        WorkBudget {
            peak_allocation_bytes: baseline,
            ..WorkBudget::UNBOUNDED
        },
        &cancel,
    )
    .map_err(|(error, _)| error)?;
    let result = source
        .read_range(reference, &status, node.version, 0, 1, &denied, &cancel)
        .await;
    assert!(matches!(
        result,
        Err(DemandError::Work(WorkError::BudgetExceeded { .. }))
    ));
    Ok(())
}

#[tokio::test]
async fn computed_ui_defaults_overlay_and_fork_use_the_real_filesystem() -> TestResult {
    let snapshot = prepared().await?;
    let expected = View::new(2)?.project(&snapshot, &"status.md")?;
    let source = Arc::new(Routed::new(
        Empty,
        LogicalName::new(NameEncoding::Utf8, b"ui".to_vec(), 255)?,
        computed(snapshot),
        VolumeLimits::default(),
    ));
    let fs = Fs::memory();
    let cancel = CancellationToken::new();
    let mut ctx = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancel);
    let (root, _) = LazyWorkspace::attach(
        &fs,
        "ui-root",
        source,
        MemoryLazyWorkspaceStore::default(),
        VolumeInitialization {
            config: VolumeConfig::portable(Lifecycle::Ephemeral),
            access_policy: None,
        },
        OperationId::new(),
        &mut ctx,
    )
    .await?;
    assert_eq!(
        root.read("/ui/status.md", 4096, &mut ctx)
            .await?
            .bytes
            .as_slice(),
        expected.as_bytes()
    );
    root.write(
        "/ui/status.md",
        acyclic_stream::OwnedBytes::from_static(b"authored"),
        IdempotencyKey::new(),
        PublicationPermit::Unrestricted,
        &mut ctx,
    )
    .await?;
    let (fork, _) = root
        .fork(
            "ui-fork",
            IdempotencyKey::new(),
            PublicationPermit::Unrestricted,
            &mut ctx,
        )
        .await?;
    root.write(
        "/ui/status.md",
        acyclic_stream::OwnedBytes::from_static(b"parent"),
        IdempotencyKey::new(),
        PublicationPermit::Unrestricted,
        &mut ctx,
    )
    .await?;
    assert_eq!(
        fork.read("/ui/status.md", 4096, &mut ctx)
            .await?
            .bytes
            .as_slice(),
        b"authored"
    );
    fork.remove("/ui/status.md", None, &mut ctx).await?;
    assert_eq!(
        fork.read_latest("/ui/status.md", 4096, &mut ctx)
            .await?
            .bytes
            .as_slice(),
        expected.as_bytes()
    );
    assert_eq!(
        root.read("/ui/status.md", 4096, &mut ctx)
            .await?
            .bytes
            .as_slice(),
        b"parent"
    );
    Ok(())
}
