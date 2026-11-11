//! Agent tree and history views as pure renders over the [`Reader`].
//!
//! A swarm is a stream subtree and an agent fork is a stream fork, so this
//! layer adds no data API: it lists `harness/agents` and `<agent>/children`
//! with `children_page` (projecting each child's `conversation` head), reads
//! ancestry-scoped status heads and conversation ranges with `read`, follows
//! only the displayed live tails. The reader learns each stream's exact
//! inherited cut from the primitive Head/origin, so opening a forked child
//! reuses cached history without reconstructing lineage from agent records.
//!
//! Per frame the work is bounded by the viewport, overscan and fixed
//! budgets; agents that are not displayed cost nothing.

use crate::reader::RecordBytes;
use crate::reader::{Priority, Reader, ReaderPolicy, Want};
use acyclic_harness_core::agent::{AGENTS, AgentPath, AgentRecord, StatusRecord};
use acyclic_stream::{StreamError, StreamPath};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::task::{Context, Poll};

/// A scoped explorer driven by its original provider events. Native and web
/// renderers share its reader, scheduling, cache and follow ownership.
pub struct LiveExplorer<P: acyclic_stream::StreamProvider + ?Sized> {
    /// The only tree collector; callers set the viewport and navigation intent.
    pub view: Explorer,
    driver: crate::reader::Driver<P>,
    reported: u64,
}

impl<P: acyclic_stream::StreamProvider + ?Sized> LiveExplorer<P> {
    /// Open a scoped tree without reading or collecting any children yet.
    pub fn children(
        policy: ReaderPolicy,
        agent: &AgentPath,
        provider: Arc<P>,
    ) -> Result<Self, StreamError> {
        let mut view = Explorer::children(policy, agent)?;
        let mut driver = crate::reader::Driver::new(provider);
        driver.attach(view.reader(), 0);
        let reported = view.reader_ref().generation();
        Ok(Self {
            view,
            driver,
            reported,
        })
    }

    /// Wait on the existing source work or follows. Calling this with an
    /// unchanged source registers its wake and returns Pending; no probe runs.
    pub fn poll_changed(&mut self, now: u64, cx: &mut Context<'_>) -> Poll<()> {
        self.driver.step(self.view.reader(), now, cx);
        let generation = self.view.reader_ref().generation();
        if generation == self.reported {
            Poll::Pending
        } else {
            self.reported = generation;
            Poll::Ready(())
        }
    }

    /// Existing read deadlines and transient transport backoff, when needed.
    pub fn next_deadline(&self) -> Option<u64> {
        self.view.reader_ref().next_deadline()
    }
}

/// Rows of lookahead in the scroll direction, at most.
const MAX_LOOKAHEAD: usize = 256;
/// Agents remembered as recently active, for prefetch.
const MAX_RECENT: usize = 16;
/// Rows in one frame, at most.
const MAX_ROWS: usize = 512;
/// History rows in one window, at most.
const MAX_HISTORY_ROWS: usize = 1024;

/// A tree position: a listed child, or a listing page not loaded yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Pos {
    /// `children[offset]` of listing page `page` of `parent`.
    Row {
        /// `children_page` parent.
        parent: StreamPath,
        /// Page index.
        page: u32,
        /// Index in the page.
        offset: u32,
    },
    /// Page `page` of `parent`, not cached: a skeleton row.
    Gap {
        /// `children_page` parent.
        parent: StreamPath,
        /// Page index.
        page: u32,
    },
}

/// One explicit sibling navigation, resumed by the same Reader's page events.
/// It retains a cursor, never a list of siblings or a second tree collector.
pub struct SiblingNavigation {
    agent: AgentPath,
    parent: StreamPath,
    page: u32,
    forward: bool,
    neighbor: bool,
    seek_forward: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::{ListedChild, Listing, Op, Outcome};
    use acyclic_harness_core::agent::AgentId;
    use acyclic_stream::{
        AppendRequest, ChildrenPageRequest, IdempotencyKey, MemoryStream, StreamProviderExt as _,
    };

    #[tokio::test]
    async fn scoped_ten_thousand_child_tree_reads_only_the_visible_window()
    -> Result<(), Box<dyn std::error::Error>> {
        use acyclic_harness_core::agent::Phase;
        use acyclic_stream::{CommitMutation, CommitRequest, testing::Counted};
        async fn open(
            count: u64,
        ) -> Result<
            (
                LiveExplorer<Counted<MemoryStream>>,
                acyclic_stream::testing::Requests,
                Arc<Counted<MemoryStream>>,
            ),
            Box<dyn std::error::Error>,
        > {
            let root = AgentPath::root(&AgentId::from_bytes([1; 16]));
            let memory = MemoryStream::default();
            let status = StatusRecord {
                child_settlement: None,
                child_wakes: None,
                phase: Phase::Idle,
                cursor: 0,
                conversation: 0,
            };
            memory
                .append(AppendRequest {
                    path: root.status()?,
                    records: vec![status.append_record()?],
                    if_tail: None,
                    idempotency_key: IdempotencyKey::new(b"parent".to_vec())?,
                })
                .await?;
            memory
                .append(AppendRequest {
                    path: root.membership()?,
                    records: vec![b"root membership".as_slice().into()],
                    if_tail: None,
                    idempotency_key: IdempotencyKey::new(b"root membership".to_vec())?,
                })
                .await?;
            // Batch fixture publication, never a per-agent task or thread.
            for first in (0..count).step_by(100) {
                let mut mutations = Vec::new();
                for number in first..(first + 100).min(count) {
                    let id = AgentId::try_from(format!("{:032x}", (number + 1) * 2))?;
                    let child = root.child(&id)?;
                    mutations.push(CommitMutation::Append {
                        path: StreamPath::new(child.as_str())?,
                        records: vec![b"child".as_slice().into()],
                    });
                    mutations.push(CommitMutation::Append {
                        path: child.status()?,
                        records: vec![status.append_record()?],
                    });
                    mutations.push(CommitMutation::Append {
                        path: child.membership()?,
                        records: vec![b"self membership".as_slice().into()],
                    });
                }
                mutations.push(CommitMutation::Append {
                    path: root.membership()?,
                    records: vec![b"children created".as_slice().into()],
                });
                let committed = memory
                    .commit(CommitRequest {
                        deadline_unix_millis: None,
                        effects: vec![acyclic_stream::CommitEffect::hard(Vec::new(), mutations)],
                        idempotency_key: IdempotencyKey::new(first.to_be_bytes().to_vec())?,
                    })
                    .await?;
                assert!(matches!(
                    committed,
                    acyclic_stream::CommitOutcome::Committed(_)
                ));
            }
            let provider = Arc::new(Counted::new(memory));
            let mut live =
                LiveExplorer::children(ReaderPolicy::default(), &root, provider.clone())?;
            live.view.set_viewport(5, 0, 16);
            loop {
                live.view.frame(0);
                if live.view.rows().len() == 5
                    && live.view.rows().iter().all(|(_, row)| {
                        matches!(
                            row,
                            Row::Agent {
                                status: Some(_),
                                ..
                            }
                        )
                    })
                {
                    break;
                }
                futures::future::poll_fn(|cx| live.poll_changed(0, cx)).await;
            }
            assert!(live.view.reader_ref().issues().next().is_none());
            assert!(live.view.reader_ref().cache().indexed_children() <= 64);
            let requests = provider.requests();
            // Redraw at the start cannot escape into the root's parent listing.
            live.view.scroll(-100);
            live.view.frame(0);
            assert!(live.view.rows().iter().all(|(_, row)| matches!(row, Row::Agent { path, .. } if root.contains(path) && path != &root)));
            Ok((live, requests, provider))
        }
        let (mut small, small_work, small_provider) = open(10).await?;
        let (mut large, large_work, _) = open(10_000).await?;
        let snapshot = |view: &Explorer| -> Result<String, crate::ui::Error> {
            let projection = crate::ui::View::new(1)?;
            let mut text = String::new();
            for (_, row) in view.rows() {
                let Row::Agent { path, .. } = row else {
                    return Err(crate::ui::Error::Pending);
                };
                let cut = crate::ui::ReaderCut {
                    reader: view.reader_ref(),
                    agent: path,
                    file: &|_| None,
                };
                let row = projection.row(&cut, |_| None)?;
                text.push_str(&format!(
                    "{} {} · {} · {} · {}\n",
                    row.glyph, row.title, row.model, row.cost, row.age
                ));
            }
            Ok(text)
        };
        let mut golden = snapshot(&large.view)?;
        assert_eq!(
            small.view.reader_ref().issued(),
            large.view.reader_ref().issued()
        );
        assert_eq!(small_work, large_work);
        // One root witness and page; each visible row admits its status,
        // children's witness and first page. No request depends on total size.
        assert!(
            large.view.reader_ref().issued() <= 2 + 3 * u64::try_from(large.view.rows().len())?
        );
        let before = large.view.reader_ref().issued();
        large.view.scroll(64);
        loop {
            large.view.frame(0);
            if large.view.rows().len() == 5
                && large.view.rows().iter().all(|(_, row)| {
                    matches!(
                        row,
                        Row::Agent {
                            status: Some(_),
                            ..
                        }
                    )
                })
            {
                break;
            }
            futures::future::poll_fn(|cx| large.poll_changed(0, cx)).await;
        }
        assert!(matches!(&large.view.rows()[0].0, Pos::Row { page: 1, .. }));
        assert!(
            large.view.reader_ref().issued() - before <= 16,
            "post-scroll requests: {}",
            large.view.reader_ref().issued() - before
        );
        golden.push_str("after one page\n");
        golden.push_str(&snapshot(&large.view)?);
        assert_eq!(golden, include_str!("ui/golden/tree-10000.txt"));
        let Row::Agent { path: selected, .. } = &large.view.rows()[0].1 else {
            return Err("selected child missing".into());
        };
        let selected = selected.clone();
        let before = large.view.reader_ref().issued();
        for (forward, expected) in [(false, 128u128), (true, 132)] {
            let mut navigation = large
                .view
                .sibling_navigation(&selected, forward)
                .ok_or("sibling cursor")?;
            let adjacent = loop {
                if let Poll::Ready(adjacent) = large.view.navigate_sibling(&mut navigation) {
                    break adjacent.ok_or("adjacent child")?;
                }
                large.view.frame(0);
                futures::future::poll_fn(|cx| large.poll_changed(0, cx)).await;
            };
            assert_eq!(adjacent.id().to_bytes(), expected.to_be_bytes());
            assert_eq!(adjacent.parent(), selected.parent());
        }
        let missing = selected
            .parent()
            .ok_or("parent")?
            .child(&AgentId::from_bytes(129u128.to_be_bytes()))?;
        let mut navigation = large
            .view
            .sibling_navigation(&missing, true)
            .ok_or("missing cursor")?;
        loop {
            if let Poll::Ready(adjacent) = large.view.navigate_sibling(&mut navigation) {
                assert!(
                    adjacent.is_none(),
                    "a gap identity is not an existing sibling"
                );
                break;
            }
            large.view.frame(0);
            futures::future::poll_fn(|cx| large.poll_changed(0, cx)).await;
        }
        assert!(large.view.reader_ref().issued() - before <= 16);
        let Row::Agent {
            path: parent,
            status: Some(status),
            ..
        } = &small.view.rows()[0].1
        else {
            return Err("visible parent".into());
        };
        let (parent, mut status) = (parent.clone(), status.clone());
        let nested = parent.child(&AgentId::from_bytes([7; 16]))?;
        small_provider
            .append(AppendRequest {
                path: StreamPath::new(nested.as_str())?,
                records: vec![b"nested".as_slice().into()],
                if_tail: None,
                idempotency_key: IdempotencyKey::new(b"nested".to_vec())?,
            })
            .await?;
        small_provider
            .append(AppendRequest {
                path: nested.status()?,
                records: vec![status.append_record()?],
                if_tail: None,
                idempotency_key: IdempotencyKey::new(b"nested-status".to_vec())?,
            })
            .await?;
        for (agent, key) in [
            (&parent, b"parent-created".as_slice()),
            (&nested, b"nested-self".as_slice()),
        ] {
            small_provider
                .append(AppendRequest {
                    path: agent.membership()?,
                    records: vec![key.into()],
                    if_tail: None,
                    idempotency_key: IdempotencyKey::new(key.to_vec())?,
                })
                .await?;
        }
        status.cursor += 1;
        small_provider
            .append(AppendRequest {
                path: parent.status()?,
                records: vec![status.append_record()?],
                if_tail: None,
                idempotency_key: IdempotencyKey::new(b"parent-changed".to_vec())?,
            })
            .await?;
        small.view.descend(true);
        loop {
            small.view.frame(0);
            if small.view.rows().iter().any(|(_, row)| matches!(row, Row::Agent { path, status: Some(_), .. } if path == &nested)) { break; }
            futures::future::poll_fn(|cx| small.poll_changed(0, cx)).await;
        }
        assert!(
            small.view.expanded.is_empty(),
            "search must not retain expansion intent for visited nodes"
        );
        small.view.descend(false);
        small.view.frame(0);
        assert!(
            small
                .view
                .rows()
                .iter()
                .all(|(_, row)| !matches!(row, Row::Agent { path, .. } if path == &nested)),
            "leaving search restores collapsed traversal"
        );
        Ok(())
    }

    #[test]
    fn reverse_navigation_reaches_deep_expanded_descendants() {
        futures::executor::block_on(async {
            let provider = MemoryStream::default();
            let root = AgentPath::root(&AgentId::from_bytes([1; 16]));
            let sibling = AgentPath::root(&AgentId::from_bytes([3; 16]));
            let mut agents = vec![root.clone()];
            for _ in 0..129 {
                agents.push(
                    agents
                        .last()
                        .unwrap()
                        .child(&AgentId::from_bytes([2; 16]))
                        .unwrap(),
                );
            }
            for (index, agent) in agents.iter().chain([&sibling]).enumerate() {
                provider
                    .append(AppendRequest {
                        path: agent.conversation().unwrap(),
                        records: vec![b"visible".to_vec().into()],
                        if_tail: None,
                        idempotency_key: IdempotencyKey::new(index.to_be_bytes().to_vec()).unwrap(),
                    })
                    .await
                    .unwrap();
            }
            let mut explorer = Explorer::new(ReaderPolicy::default()).unwrap();
            for parent in std::iter::once(explorer.root.clone())
                .chain(agents.iter().map(|agent| agent.children().unwrap()))
            {
                let page = provider
                    .children_page(ChildrenPageRequest {
                        parent: Some(parent.clone()),
                        after: None,
                        hierarchy_version: None,
                        limit: 64,
                        tail_stream: None,
                    })
                    .await
                    .unwrap();
                assert!(explorer.reader.cache_mut().insert_listing(
                    &parent,
                    0,
                    Listing {
                        after: None,
                        children: page.children.into_iter().map(|child| child.path).collect(),
                        next: page.next_after,
                        fresh: true,
                        projections: Default::default(),
                        version: None,
                    },
                    None
                ));
            }
            for agent in &agents {
                explorer.expand(agent);
            }
            let from_sibling = explorer.locate(&sibling).unwrap();
            let deepest = explorer.locate(agents.last().unwrap()).unwrap();
            assert_eq!(explorer.prev(&from_sibling), Some(deepest));
            // A collapsed branch is a real boundary, independent of its depth.
            explorer.collapse(&agents[64]);
            assert_eq!(explorer.prev(&from_sibling), explorer.locate(&agents[64]));
        });
    }

    #[test]
    fn history_seeks_preserve_exclusive_ends_at_the_sequence_boundary() {
        futures::executor::block_on(async {
            let provider = MemoryStream::default();
            let agent = AgentPath::root(&AgentId::try_from(format!("{:032x}", 1)).unwrap());
            let path = agent.conversation().unwrap();
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![b"original history".as_slice().into()],
                    if_tail: Some(0),
                    idempotency_key: IdempotencyKey::new(b"history-end".to_vec()).unwrap(),
                })
                .await
                .unwrap();
            let head = provider.head(path.clone()).await.unwrap();
            let original = head.last.unwrap();
            let mut explorer = Explorer::new(ReaderPolicy::default()).unwrap();
            explorer.reader.cache_mut().observe_tail(&path, head.tail);
            explorer
                .reader
                .cache_mut()
                .observe_origin(&path, head.origin);
            explorer.reader.cache_mut().insert_record(
                &path,
                original.sequence,
                original.value.clone(),
                Some(crate::reader::RecordEvidence {
                    origin: original.origin,
                    sequence: original.sequence,
                    commit: original.commit_id,
                    committed_at_micros: original.committed_at_micros,
                }),
                None,
            );
            explorer.open(agent, 4);
            assert_eq!(explorer.history().len(), 1);
            for start in [u64::MAX - 1, u64::MAX] {
                explorer.seek(start);
                let mut wants = Vec::new();
                explorer.history_wants(explorer.history.as_ref().unwrap(), &mut wants);
                assert!(matches!(
                    &wants[0].0,
                    Want::Range { stream, from, to }
                        if stream == &path && *from == start && *to == u64::MAX
                ));
                assert!(wants.iter().all(|(want, _)| match want {
                    Want::Range { from, to, .. } => from <= to,
                    _ => false,
                }));
                assert!(explorer.history().is_empty());
            }
            explorer.jump_end();
            let rows = explorer.history();
            let value = rows[0].value.as_ref().unwrap();
            assert_eq!(value.as_ref(), original.value.as_slice());
            assert_eq!(value.as_ptr(), original.value.as_slice().as_ptr());
            explorer.reader.cache_mut().forget(&path);
            assert!(explorer.history().is_empty());
        });
    }

    /// Directory data comes from the actual provider. Only the first and
    /// late pages remain cached, as after scrolling through a large tree.
    #[test]
    fn retained_late_pages_relocate_rows_and_resume_large_logical_offsets() {
        futures::executor::block_on(async {
            let provider = MemoryStream::default();
            for id in 1..=130u64 {
                let agent = AgentPath::root(&AgentId::try_from(format!("{id:032x}")).unwrap());
                provider
                    .append(AppendRequest {
                        path: StreamPath::new(agent.as_str()).unwrap(),
                        records: vec![b"directory entry".to_vec().into()],
                        if_tail: None,
                        idempotency_key: IdempotencyKey::new(id.to_be_bytes().to_vec()).unwrap(),
                    })
                    .await
                    .unwrap();
            }
            let mut explorer = Explorer::new(ReaderPolicy {
                listing_limit: 1,
                ..ReaderPolicy::default()
            })
            .unwrap();
            let parent = explorer.root.clone();
            let want = explorer.page(parent.clone(), 0);
            explorer.reader.plan([(want, Priority::Visible)]);
            let [first]: [Op; 1] = explorer.reader.poll(0).try_into().unwrap();
            let first_id = first.id();
            let mut after = None;
            let mut version = None;
            let mut target = None;
            let mut indexed = crate::reader::RecordCache::new(1024, 16);
            let protected = indexed.clock();
            for index in 0..=128 {
                let page = provider
                    .children_page(ChildrenPageRequest {
                        parent: Some(parent.clone()),
                        after: after.clone(),
                        hierarchy_version: version,
                        limit: 1,
                        tail_stream: None,
                    })
                    .await
                    .unwrap();
                version = Some(page.hierarchy_version);
                indexed.insert_listing(
                    &parent,
                    index,
                    Listing {
                        after: after.clone(),
                        children: page
                            .children
                            .iter()
                            .map(|child| child.path.clone())
                            .collect(),
                        next: page.next_after.clone(),
                        fresh: true,
                        projections: Default::default(),
                        version: None,
                    },
                    Some(protected),
                );
                if index == 0 {
                    explorer.reader.complete(
                        first_id,
                        Outcome::Children {
                            children: page
                                .children
                                .iter()
                                .map(|child| ListedChild {
                                    path: child.path.clone(),
                                    origin: child.origin.clone(),
                                    tail: None,
                                })
                                .collect(),
                            next: page.next_after.clone(),
                            version: page.hierarchy_version,
                        },
                        0,
                    );
                } else if index == 128 {
                    target = Some(
                        AgentPath::try_from(page.children[0].path.as_str().to_owned()).unwrap(),
                    );
                    explorer.reader.cache_mut().insert_listing(
                        &parent,
                        index,
                        Listing {
                            after: after.clone(),
                            children: page.children.into_iter().map(|child| child.path).collect(),
                            next: page.next_after.clone(),
                            fresh: true,
                            projections: Default::default(),
                            version: None,
                        },
                        None,
                    );
                }
                after = page.next_after;
            }
            let target = target.unwrap();
            explorer.reader.cache().take_listing_visits();
            assert!(matches!(
                explorer.locate(&target),
                Some(Pos::Row { page: 128, .. })
            ));
            assert_eq!(
                explorer.reader.cache().take_listing_visits(),
                1,
                "child location visited unrelated retained pages"
            );
            explorer.expand(&target);
            assert_eq!(explorer.expanded.get(&target), Some(&128));
            // Both residency maps contain actual pages from this same cut.
            // A bounded predecessor query cannot borrow a later page's cursor
            // or fall below its retained anchor, even on pass-through pages.
            assert!(!indexed.is_empty());
            assert!(indexed.passed_bytes() > 0);
            assert_eq!(indexed.listing_before(&parent, 64, 128).unwrap().0, 127);
            assert!(
                indexed.take_listing_visits() <= 2,
                "page predecessor visited unrelated retained pages"
            );
            assert!(indexed.listing_before(&parent, 129, u32::MAX).is_none());
            assert_eq!(indexed.take_listing_visits(), 0);
            assert!(indexed.listing_before(&parent, 128, 128).is_none());
            assert_eq!(indexed.indexed_children(), 129);
            let sparse = std::mem::replace(explorer.reader.cache_mut(), indexed);
            let hint = explorer.expanded.remove(&target);
            assert!(matches!(
                explorer.locate(&target),
                Some(Pos::Row { page: 128, .. })
            ));
            assert!(
                explorer.reader.cache().take_listing_visits() <= 1,
                "child location visited unrelated retained pages"
            );
            if let Some(hint) = hint {
                explorer.expanded.insert(target.clone(), hint);
            }
            // Keep the displayed pass-through pages in the logical plan while
            // seeking beyond them; unrelated wants cannot prolong residency.
            let wants: Vec<_> = std::iter::once(1_000_000)
                .chain(0..=128)
                .map(|index| (explorer.page(parent.clone(), index), Priority::Visible))
                .collect();
            explorer.reader.plan(wants);
            let requests = explorer.reader.poll(1);
            assert_eq!(requests.len(), explorer.reader.policy().requests);
            let resume = &requests[0];
            assert!(matches!(resume, Op::Children { index: 129, .. }));
            // Raw cached pages lack this projection's original admission, so
            // their own refreshes also use the ordinary request credit.
            assert!(requests[1..].iter().all(|op| matches!(op,
                Op::Children { index, .. } if *index < 129)));
            assert!(
                explorer.reader.cache().take_listing_visits() <= 2,
                "page predecessor visited unrelated retained pages"
            );
            let target_path = StreamPath::new(target.as_str()).unwrap();
            let record = provider
                .head(target_path.clone())
                .await
                .unwrap()
                .last
                .unwrap();
            explorer.reader.cache_mut().insert_record(
                &target_path,
                record.sequence,
                record.value,
                Some(crate::reader::RecordEvidence {
                    origin: record.origin,
                    sequence: record.sequence,
                    commit: record.commit_id,
                    committed_at_micros: record.committed_at_micros,
                }),
                Some(protected),
            );
            explorer.reader.complete(
                resume.id(),
                Outcome::Failed(StreamError::HierarchyChanged),
                2,
            );
            assert_eq!(
                explorer.reader.cache().take_listing_visits(),
                129,
                "hierarchy reset visited unrelated retained records"
            );
            assert!(explorer.reader.cache().listings(&parent).next().is_none());
            assert_eq!(
                explorer.reader.cache().indexed_children(),
                0,
                "hierarchy reset retained child locations"
            );
            assert!(
                explorer
                    .reader
                    .cache()
                    .listing_containing(&parent, &target_path)
                    .is_none()
            );
            assert!(explorer.reader.cache().record(&target_path, 0).is_some());
            *explorer.reader.cache_mut() = sparse;
            explorer.frame(2);
            assert!(
                explorer
                    .rows()
                    .iter()
                    .any(|(_, row)| matches!(row, Row::Agent { .. }))
            );
            // Two displayed children share their parent's original witness.
            // Admission and duplicate selection belong to the same Reader.
            for id in 1..=2u64 {
                let child = target
                    .child(&AgentId::try_from(format!("{id:032x}")).unwrap())
                    .unwrap();
                provider
                    .append(AppendRequest {
                        path: StreamPath::new(child.as_str()).unwrap(),
                        records: vec![b"child directory entry".as_slice().into()],
                        if_tail: Some(0),
                        idempotency_key: IdempotencyKey::new(child.as_str().as_bytes().to_vec())
                            .unwrap(),
                    })
                    .await
                    .unwrap();
            }
            let membership = target.membership().unwrap();
            provider
                .append(AppendRequest {
                    path: membership.clone(),
                    records: vec![b"parent witness".as_slice().into()],
                    if_tail: Some(0),
                    idempotency_key: IdempotencyKey::new(b"witness".to_vec()).unwrap(),
                })
                .await
                .unwrap();
            let head = provider.head(membership.clone()).await.unwrap();
            explorer
                .reader
                .cache_mut()
                .observe_tail(&membership, head.tail);
            explorer
                .reader
                .cache_mut()
                .observe_origin(&membership, head.origin);
            let children = target.children().unwrap();
            let page = provider
                .children_page(ChildrenPageRequest {
                    parent: Some(children.clone()),
                    after: None,
                    hierarchy_version: None,
                    limit: 2,
                    tail_stream: None,
                })
                .await
                .unwrap();
            assert_eq!(page.children.len(), 2);
            explorer.reader.cache_mut().insert_listing(
                &children,
                0,
                Listing {
                    after: None,
                    children: page.children.into_iter().map(|child| child.path).collect(),
                    next: page.next_after,
                    fresh: true,
                    projections: Default::default(),
                    version: None,
                },
                None,
            );
            explorer.anchor = Pos::Row {
                parent: children,
                page: 0,
                offset: 0,
            };
            explorer.set_viewport(2, 0, 16);
            explorer.reader.take_follow_selection_visits();
            explorer.frame(3);
            assert_eq!(explorer.rows().len(), 2);
            assert_eq!(
                explorer.reader.follows().collect::<Vec<_>>(),
                vec![(&membership, head.tail)]
            );
            assert_eq!(
                explorer.reader.take_follow_selection_visits(),
                5,
                "both visible statuses and both witnesses must use the shared admission, retaining one source"
            );
            let mut replacement =
                crate::reader::Driver::new(std::sync::Arc::new(MemoryStream::default()));
            let waker = futures::task::noop_waker();
            replacement.step(
                explorer.reader(),
                2,
                &mut std::task::Context::from_waker(&waker),
            );
            // Previously rendered clones belong to the original observation,
            // even though this new provider is readable and unqualified.
            assert!(explorer.reader_ref().readable());
            assert!(explorer.rows().is_empty());
            assert_eq!(
                explorer.reader.cache().indexed_children(),
                0,
                "source reset retained child locations"
            );
        });
    }
}

/// One rendered tree row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Row {
    /// A listed agent, rendered from its latest committed records.
    Agent {
        /// Agent address.
        path: AgentPath,
        /// Latest committed status, `None` until read.
        status: Option<StatusRecord>,
        /// Conversation tail, from the listing projection or a later read.
        conversation: Option<u64>,
        /// The latest committed conversation record, projected by the listing.
        latest: Option<RecordBytes>,
        /// Whether children are shown.
        expanded: bool,
    },
    /// Children not loaded yet; shown as a skeleton, never blank.
    Loading {
        /// Nesting depth of the missing rows.
        depth: usize,
    },
}

/// One rendered history row: a committed position and its record, if cached.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryRow {
    /// Conversation position.
    pub sequence: u64,
    /// Committed record, `None` while loading.
    pub value: Option<RecordBytes>,
}

struct History {
    stream: StreamPath,
    from: u64,
    rows: usize,
    end: bool,
}

/// The tree explorer and the open agent's history, over one reader.
pub struct Explorer {
    reader: Reader,
    root: StreamPath,
    projection: StreamPath,
    anchor: Pos,
    height: usize,
    overscan: usize,
    velocity: i64,
    frame_ms: u64,
    /// Logical expansion intent and the page each agent was seen on. Only
    /// the viewport requests data; cache pressure never collapses intent.
    expanded: BTreeMap<AgentPath, u32>,
    descend: bool,
    hover: Option<AgentPath>,
    focus: Option<AgentPath>,
    recent: VecDeque<AgentPath>,
    seen: BTreeMap<StreamPath, (AgentPath, u64)>,
    history: Option<History>,
    rows: Vec<(Pos, Row)>,
    rows_source: u64,
}

fn stream(text: &str) -> Option<StreamPath> {
    StreamPath::new(text).ok()
}

fn agent_of(child: &StreamPath) -> Option<AgentPath> {
    AgentPath::try_from(child.as_str().to_owned()).ok()
}

/// The listing parent that contains `agent`.
fn parent_listing(agent: &AgentPath) -> Option<StreamPath> {
    match agent.parent() {
        Some(parent) => parent.children().ok(),
        None => stream(AGENTS),
    }
}

/// The agent whose children `parent` lists, `None` for the top level.
fn owner(parent: &StreamPath) -> Option<AgentPath> {
    parent
        .as_str()
        .strip_suffix("/children")
        .and_then(|owner| AgentPath::try_from(owner.to_owned()).ok())
}

impl Explorer {
    /// An explorer at the top of the tree.
    ///
    /// # Errors
    /// Refuses an invalid policy.
    pub fn new(policy: ReaderPolicy) -> Result<Self, StreamError> {
        Self::listing(policy, StreamPath::new(AGENTS)?)
    }

    /// Explore only the selected agent's descendants. No global directory or
    /// sibling authority is required to open a sidebar for one served tree.
    pub fn children(policy: ReaderPolicy, agent: &AgentPath) -> Result<Self, StreamError> {
        let root = agent.children().map_err(|error| match error {
            acyclic_harness_core::agent::Error::Stream(error) => error,
            _ => StreamError::InvalidResponse,
        })?;
        Self::listing(policy, root)
    }

    fn listing(policy: ReaderPolicy, root: StreamPath) -> Result<Self, StreamError> {
        Ok(Self {
            reader: Reader::new(policy)?,
            anchor: Pos::Gap {
                parent: root.clone(),
                page: 0,
            },
            root,
            projection: StreamPath::new("conversation")?,
            height: 32,
            overscan: 16,
            velocity: 0,
            frame_ms: 16,
            expanded: BTreeMap::new(),
            descend: false,
            hover: None,
            focus: None,
            recent: VecDeque::new(),
            seen: BTreeMap::new(),
            history: None,
            rows: Vec::new(),
            rows_source: 0,
        })
    }

    /// The reader the platform driver executes.
    pub fn reader(&mut self) -> &mut Reader {
        &mut self.reader
    }

    /// Borrow the reader for diagnostics.
    #[must_use]
    pub fn reader_ref(&self) -> &Reader {
        &self.reader
    }

    /// Viewport rows, overscan rows and the frame interval. Zero overscan
    /// disables speculative row traversal, including velocity lookahead.
    pub fn set_viewport(&mut self, height: usize, overscan: usize, frame_ms: u64) {
        self.height = height.clamp(1, MAX_ROWS);
        self.overscan = overscan.min(MAX_ROWS);
        self.frame_ms = frame_ms.max(1);
    }

    /// Pointer intent: the hovered agent's children and history are likely next.
    pub fn hover(&mut self, agent: Option<AgentPath>) {
        self.hover = agent;
    }

    /// Keyboard intent, like [`Self::hover`].
    pub fn focus(&mut self, agent: Option<AgentPath>) {
        self.focus = agent;
    }

    /// Scroll by rows; the per-frame velocity steers lookahead.
    pub fn scroll(&mut self, rows: i64) {
        self.velocity = rows;
        let steps = rows.unsigned_abs().min(MAX_ROWS as u64 * 4);
        for _ in 0..steps {
            let next = if rows > 0 {
                self.next(&self.anchor)
            } else {
                self.prev(&self.anchor)
            };
            match next {
                Some(next) => self.anchor = next,
                None => break,
            }
        }
    }

    /// Traverse collapsed descendants while searching. This changes traversal
    /// intent, never expands or collects a subtree; the same reader admits only
    /// the visible windows and the ancestor pages needed to resume them.
    pub fn descend(&mut self, descend: bool) {
        if self.descend != descend {
            self.descend = descend;
            self.home();
        }
    }

    /// Return to the scope's first page without collecting its descendants.
    pub fn home(&mut self) {
        self.anchor = Pos::Gap {
            parent: self.root.clone(),
            page: 0,
        };
        self.velocity = 0;
    }

    /// Advance one visible window in the expanded tree. A missing next page
    /// remains a resumable gap, planned by the same reader on the next frame.
    pub fn next_page(&mut self) -> Poll<bool> {
        let Some((position, row)) = self.rows.last() else {
            return if self.reader.cache().listing(&self.root, 0).is_some() {
                Poll::Ready(false)
            } else {
                Poll::Pending
            };
        };
        if matches!(row, Row::Loading { .. }) {
            return Poll::Pending;
        }
        let Some(next) = self.next(position) else {
            return Poll::Ready(false);
        };
        self.anchor = next;
        self.velocity = 0;
        Poll::Ready(true)
    }

    /// Show an agent's children.
    pub fn expand(&mut self, agent: &AgentPath) {
        let page = match self.locate(agent) {
            Some(Pos::Row { page, .. }) => page,
            _ => 0,
        };
        self.expanded.insert(agent.clone(), page);
    }

    /// Hide an agent's children; the anchor moves out of them if inside.
    pub fn collapse(&mut self, agent: &AgentPath) {
        self.expanded.remove(agent);
        if self
            .anchor_agent()
            .is_some_and(|anchor| anchor.as_str().starts_with(&format!("{agent}/")))
            && let Some(at) = self.locate(agent)
        {
            self.anchor = at;
        }
    }

    /// Navigate adjacent direct children, including across unloaded pages.
    /// The original cached location is reused; an evicted location is sought
    /// lazily through the same listing pages, under the ordinary read credit.
    pub fn sibling_navigation(
        &self,
        agent: &AgentPath,
        forward: bool,
    ) -> Option<SiblingNavigation> {
        let parent = parent_listing(agent)?;
        if parent != self.root
            && !parent
                .as_str()
                .strip_prefix(self.root.as_str())
                .is_some_and(|suffix| suffix.starts_with('/'))
        {
            return None;
        }
        let page = match self.locate(agent) {
            Some(Pos::Row { page, .. }) => page,
            _ => self.expanded.get(agent).copied().unwrap_or(0),
        };
        Some(SiblingNavigation {
            agent: agent.clone(),
            parent,
            page,
            forward,
            neighbor: false,
            seek_forward: None,
        })
    }

    /// Consume resident pages, or anchor one missing page for `frame` to plan.
    /// Pending means await `LiveExplorer::poll_changed`, never a timer/probe.
    pub fn navigate_sibling(
        &mut self,
        navigation: &mut SiblingNavigation,
    ) -> Poll<Option<AgentPath>> {
        loop {
            let Some(listing) = self
                .reader
                .cache()
                .listing(&navigation.parent, navigation.page)
            else {
                self.anchor = Pos::Gap {
                    parent: navigation.parent.clone(),
                    page: navigation.page,
                };
                return Poll::Pending;
            };
            let target = if navigation.neighbor {
                if navigation.forward {
                    listing.children.first()
                } else {
                    listing.children.last()
                }
            } else {
                match listing
                    .children
                    .binary_search_by(|path| path.as_str().cmp(navigation.agent.as_str()))
                {
                    Ok(offset) => {
                        navigation.neighbor = true;
                        if navigation.forward {
                            listing.children.get(offset + 1)
                        } else {
                            offset
                                .checked_sub(1)
                                .and_then(|offset| listing.children.get(offset))
                        }
                    }
                    Err(offset) => {
                        if !listing.children.is_empty()
                            && offset < listing.children.len()
                            && offset != 0
                        {
                            return Poll::Ready(None);
                        }
                        // The target can move to an earlier page after a
                        // membership publication. Compare identities, never
                        // assume a formerly cached ordinal remains current.
                        if offset == 0 && !listing.children.is_empty() {
                            if navigation.seek_forward == Some(true) {
                                return Poll::Ready(None);
                            }
                            navigation.seek_forward = Some(false);
                            let Some(page) = navigation.page.checked_sub(1) else {
                                return Poll::Ready(None);
                            };
                            navigation.page = page;
                            continue;
                        }
                        if listing.next.is_none() {
                            return Poll::Ready(None);
                        }
                        if navigation.seek_forward == Some(false) {
                            return Poll::Ready(None);
                        }
                        navigation.seek_forward = Some(true);
                        let Some(page) = navigation.page.checked_add(1) else {
                            return Poll::Ready(None);
                        };
                        navigation.page = page;
                        continue;
                    }
                }
            };
            if let Some(path) = target {
                let agent = agent_of(path);
                if let Some(offset) = listing
                    .children
                    .binary_search(path)
                    .ok()
                    .and_then(|offset| u32::try_from(offset).ok())
                {
                    self.anchor = Pos::Row {
                        parent: navigation.parent.clone(),
                        page: navigation.page,
                        offset,
                    };
                }
                return Poll::Ready(agent);
            }
            navigation.page = if navigation.forward {
                if listing.next.is_none() {
                    return Poll::Ready(None);
                }
                let Some(page) = navigation.page.checked_add(1) else {
                    return Poll::Ready(None);
                };
                page
            } else {
                let Some(page) = navigation.page.checked_sub(1) else {
                    return Poll::Ready(None);
                };
                page
            };
        }
    }

    /// Open an agent's history at its end.
    pub fn open(&mut self, agent: AgentPath, rows: usize) {
        let Ok(stream) = agent.conversation() else {
            return;
        };
        self.push_recent(&agent);
        self.history = Some(History {
            stream,
            from: 0,
            rows: rows.clamp(1, MAX_HISTORY_ROWS),
            end: true,
        });
    }

    /// Close the history view.
    pub fn close(&mut self) {
        self.history = None;
    }

    /// Seek the history window to a position, O(1).
    pub fn seek(&mut self, sequence: u64) {
        if let Some(history) = &mut self.history {
            history.from = sequence;
            history.end = false;
        }
    }

    /// Jump the history window to the live end, O(1).
    pub fn jump_end(&mut self) {
        if let Some(history) = &mut self.history {
            history.end = true;
        }
    }

    fn push_recent(&mut self, agent: &AgentPath) {
        self.recent.retain(|recent| recent != agent);
        self.recent.push_front(agent.clone());
        self.recent.truncate(MAX_RECENT);
    }

    fn anchor_agent(&self) -> Option<AgentPath> {
        match &self.anchor {
            Pos::Row {
                parent,
                page,
                offset,
            } => self
                .child(parent, *page, *offset)
                .and_then(|c| agent_of(&c)),
            Pos::Gap { parent, .. } => owner(parent),
        }
    }

    fn child(&self, parent: &StreamPath, page: u32, offset: u32) -> Option<StreamPath> {
        self.reader
            .cache()
            .listing(parent, page)
            .and_then(|listing| listing.children.get(offset as usize))
            .cloned()
    }

    /// Where `agent` is listed, using retained page membership.
    fn locate(&self, agent: &AgentPath) -> Option<Pos> {
        let parent = parent_listing(agent)?;
        if parent != self.root
            && !parent
                .as_str()
                .strip_prefix(self.root.as_str())
                .is_some_and(|suffix| suffix.starts_with('/'))
        {
            return None;
        }
        let hint = self.expanded.get(agent).copied();
        // Keep an expanded row's exact page while present; otherwise seek its
        // retained membership without visiting unrelated pages.
        let cache = self.reader.cache();
        let hinted =
            hint.and_then(|page| cache.listing(&parent, page).map(|listing| (page, listing)));
        let position = |(page, listing): (u32, &crate::reader::Listing)| {
            let offset = listing
                .children
                .binary_search_by(|child| child.as_str().cmp(agent.as_str()))
                .ok()?;
            Some(Pos::Row {
                parent: parent.clone(),
                page,
                offset: u32::try_from(offset).ok()?,
            })
        };
        hinted.and_then(position).or_else(|| {
            let child = StreamPath::new(agent.as_str()).ok()?;
            cache.listing_containing(&parent, &child).and_then(position)
        })
    }

    /// The first row at or after `page`. Pages may be empty yet continue
    /// (children the credential cannot list are omitted), so empty pages
    /// with a cursor are skipped. A cache miss yields a resumable gap, so
    /// the walk is bounded by retained pages rather than a logical cutoff.
    fn first_of(&self, parent: StreamPath, mut page: u32) -> Option<Pos> {
        loop {
            match self.reader.cache().listing(&parent, page) {
                None => return Some(Pos::Gap { parent, page }),
                Some(listing) if !listing.children.is_empty() => {
                    return Some(Pos::Row {
                        parent,
                        page,
                        offset: 0,
                    });
                }
                Some(listing) if listing.next.is_none() => return None,
                Some(_) => {}
            }
            page = page.checked_add(1)?;
        }
    }

    /// Creation appends the parent's membership feed in the same commit.
    /// A spawn need not change its phase, so status cannot witness a listing.
    fn page(&self, parent: StreamPath, index: u32) -> Want {
        let witness = owner(&parent).and_then(|owner| owner.membership().ok());
        Want::Listing {
            parent,
            index,
            tail: Some(self.projection.clone()),
            witness,
        }
    }

    fn next(&self, pos: &Pos) -> Option<Pos> {
        match pos {
            Pos::Row {
                parent,
                page,
                offset,
            } => {
                if let Some(agent) = self
                    .child(parent, *page, *offset)
                    .and_then(|c| agent_of(&c))
                    && (self.descend || self.expanded.contains_key(&agent))
                    && let Some(first) = agent.children().ok().and_then(|c| self.first_of(c, 0))
                {
                    return Some(first);
                }
                self.sibling(parent, *page, *offset)
            }
            Pos::Gap { parent, .. } => self.up(parent),
        }
    }

    fn sibling(&self, parent: &StreamPath, page: u32, offset: u32) -> Option<Pos> {
        let (mut parent, mut page, mut offset) = (parent.clone(), page, offset);
        loop {
            let listing = self.reader.cache().listing(&parent, page)?;
            if (offset as usize) + 1 < listing.children.len() {
                return Some(Pos::Row {
                    parent,
                    page,
                    offset: offset + 1,
                });
            }
            if listing.next.is_some()
                && let Some(next) = self.first_of(parent.clone(), page.checked_add(1)?)
            {
                return Some(next);
            }
            if parent == self.root {
                return None;
            }
            let current = owner(&parent)?;
            let Pos::Row {
                parent: above,
                page: at,
                offset: index,
            } = self.locate(&current)?
            else {
                return None;
            };
            (parent, page, offset) = (above, at, index);
        }
    }

    fn up(&self, parent: &StreamPath) -> Option<Pos> {
        if parent == &self.root {
            return None;
        }
        let owner = owner(parent)?;
        match self.locate(&owner)? {
            Pos::Row {
                parent,
                page,
                offset,
            } => self.sibling(&parent, page, offset),
            Pos::Gap { .. } => None,
        }
    }

    fn prev(&self, pos: &Pos) -> Option<Pos> {
        let (parent, page, offset) = match pos {
            Pos::Row {
                parent,
                page,
                offset,
            } => (parent, *page, *offset),
            Pos::Gap { parent, page } => (parent, *page, 0),
        };
        if offset > 0 {
            return Some(self.last_descendant(Pos::Row {
                parent: parent.clone(),
                page,
                offset: offset - 1,
            }));
        }
        if page > 0 {
            return Some(match self.reader.cache().listing(parent, page - 1) {
                Some(listing) if !listing.children.is_empty() => self.last_descendant(Pos::Row {
                    parent: parent.clone(),
                    page: page - 1,
                    offset: u32::try_from(listing.children.len() - 1).unwrap_or(0),
                }),
                _ => Pos::Gap {
                    parent: parent.clone(),
                    page: page - 1,
                },
            });
        }
        (parent != &self.root)
            .then(|| owner(parent).and_then(|owner| self.locate(&owner)))
            .flatten()
    }

    /// The last row shown under `pos`, through expanded descendants.
    fn last_descendant(&self, mut pos: Pos) -> Pos {
        loop {
            let Pos::Row {
                parent,
                page,
                offset,
            } = &pos
            else {
                return pos;
            };
            let Some(agent) = self
                .child(parent, *page, *offset)
                .and_then(|c| agent_of(&c))
            else {
                return pos;
            };
            if !self.descend && !self.expanded.contains_key(&agent) {
                return pos;
            }
            let Ok(children) = agent.children() else {
                return pos;
            };
            let mut last = None;
            let mut page = 0;
            loop {
                match self.reader.cache().listing(&children, page) {
                    None => {
                        last = Some(Pos::Gap {
                            parent: children.clone(),
                            page,
                        });
                        break;
                    }
                    Some(listing) if listing.next.is_none() => {
                        last = listing
                            .children
                            .len()
                            .checked_sub(1)
                            .map(|offset| Pos::Row {
                                parent: children.clone(),
                                page,
                                offset: u32::try_from(offset).unwrap_or(0),
                            });
                        break;
                    }
                    Some(_) => {}
                }
                let Some(next) = page.checked_add(1) else {
                    break;
                };
                page = next;
            }
            match last {
                Some(next) => pos = next,
                None => return pos,
            }
        }
    }

    fn render(&self, pos: &Pos) -> Row {
        match pos {
            Pos::Gap { parent, .. } => Row::Loading {
                depth: owner(parent).map_or(0, |owner| owner.depth() + 1),
            },
            Pos::Row {
                parent,
                page,
                offset,
            } => {
                let Some(path) = self
                    .child(parent, *page, *offset)
                    .and_then(|c| agent_of(&c))
                else {
                    return Row::Loading { depth: 0 };
                };
                let cache = self.reader.cache();
                let status = path.status().ok().and_then(|stream| {
                    let tail = cache.tail(&stream)?.checked_sub(1)?;
                    StatusRecord::decode(cache.record(&stream, tail)?).ok()
                });
                let stream = path.conversation().ok();
                let conversation = stream.as_ref().and_then(|s| cache.tail(s));
                let latest = stream
                    .as_ref()
                    .zip(conversation)
                    .and_then(|(s, tail)| cache.record(s, tail.checked_sub(1)?).cloned());
                Row::Agent {
                    latest,
                    expanded: self.descend || self.expanded.contains_key(&path),
                    path,
                    status,
                    conversation,
                }
            }
        }
    }

    /// The wants that keep one position renderable.
    fn wants_for(&self, pos: &Pos, priority: Priority, out: &mut Vec<(Want, Priority)>) {
        let (parent, page) = match pos {
            Pos::Row { parent, page, .. } | Pos::Gap { parent, page } => (parent, *page),
        };
        out.push((self.page(parent.clone(), page), priority));
        if let Pos::Row { page, offset, .. } = pos
            && let Some(agent) = self
                .child(parent, *page, *offset)
                .and_then(|c| agent_of(&c))
            && let Ok(status) = agent.status()
        {
            out.push((
                Want::Last {
                    stream: status,
                    count: 1,
                },
                priority,
            ));
        }
    }

    /// An agent about to be opened: the end of its history. When `deep`
    /// (pointer or keyboard intent) it may also be expanded: its first
    /// children page and those children's statuses.
    fn intent_wants(&self, agent: &AgentPath, deep: bool, out: &mut Vec<(Want, Priority)>) {
        if let Ok(conversation) = agent.conversation() {
            out.push((
                Want::Last {
                    stream: conversation,
                    count: self.reader.policy().read_limit,
                },
                Priority::Prefetch,
            ));
        }
        let Some(children) = deep.then(|| agent.children().ok()).flatten() else {
            return;
        };
        out.push((self.page(children.clone(), 0), Priority::Prefetch));
        if let Some(listing) = self.reader.cache().listing(&children, 0) {
            for child in listing.children.iter().take(self.height) {
                if let Some(status) = agent_of(child).and_then(|c| c.status().ok()) {
                    out.push((
                        Want::Last {
                            stream: status,
                            count: 1,
                        },
                        Priority::Prefetch,
                    ));
                }
            }
        }
    }

    /// Lay out one frame and hand the reader its needs. Returns the rows.
    pub fn frame(&mut self, now: u64) -> &[(Pos, Row)] {
        self.reader.begin_frame(now);
        // A loaded gap becomes its first row.
        if let Pos::Gap { parent, page } = &self.anchor
            && let Some(listing) = self.reader.cache().listing(parent, *page)
        {
            self.anchor = if listing.children.is_empty() {
                self.prev(&self.anchor).unwrap_or(self.anchor.clone())
            } else {
                Pos::Row {
                    parent: parent.clone(),
                    page: *page,
                    offset: 0,
                }
            };
        }
        // Rows re-listed out from under the anchor: snap to the first row.
        if let Pos::Row {
            parent,
            page,
            offset,
        } = &self.anchor
            && self.child(parent, *page, *offset).is_none()
        {
            self.anchor = Pos::Gap {
                parent: parent.clone(),
                page: *page,
            };
        }
        let mut wants = Vec::new();
        let mut visible = Vec::with_capacity(self.height);
        let mut pos = Some(self.anchor.clone());
        while let Some(at) = pos {
            if visible.len() >= self.height {
                break;
            }
            pos = self.next(&at);
            visible.push(at);
        }
        for at in &visible {
            self.wants_for(at, Priority::Visible, &mut wants);
        }
        // Ancestors' pages carry traversal across page and level ends.
        let ancestors = self.ancestors(&visible);
        for (parent, page) in &ancestors {
            wants.push((self.page(parent.clone(), *page), Priority::Visible));
        }
        if let Some(history) = &self.history {
            self.history_wants(history, &mut wants);
        }
        // Likely next, most likely first: pointer and keyboard intent, then
        // recently active agents, then lookahead in the scroll direction.
        for agent in [&self.focus, &self.hover].into_iter().flatten() {
            self.intent_wants(agent, true, &mut wants);
        }
        for agent in &self.recent {
            self.intent_wants(agent, false, &mut wants);
        }
        // Lookahead in the scroll direction, overscan behind.
        let rtt_rows = (self.reader.rtt_ms() / self.frame_ms).max(1);
        let ahead = if self.overscan == 0 {
            0
        } else {
            (self.overscan as u64)
                .saturating_add(self.velocity.unsigned_abs().saturating_mul(rtt_rows))
                .min(MAX_LOOKAHEAD as u64) as usize
        };
        let (forward, backward) = if self.velocity < 0 {
            (self.overscan, ahead)
        } else {
            (ahead, self.overscan)
        };
        let mut below = visible.last().cloned();
        for _ in 0..forward {
            below = below.and_then(|at| self.next(&at));
            match &below {
                Some(at) => self.wants_for(at, Priority::Prefetch, &mut wants),
                None => break,
            }
        }
        let mut above = Some(self.anchor.clone());
        for _ in 0..backward {
            above = above.and_then(|at| self.prev(&at));
            match &above {
                Some(at) => self.wants_for(at, Priority::Prefetch, &mut wants),
                None => break,
            }
        }
        // Any visible row may be expanded next: its first children page.
        for at in &visible {
            if let Pos::Row {
                parent,
                page,
                offset,
            } = at
                && let Some(children) = self
                    .child(parent, *page, *offset)
                    .and_then(|c| agent_of(&c))
                    .and_then(|agent| agent.children().ok())
            {
                wants.push((self.page(children, 0), Priority::Prefetch));
            }
        }
        self.velocity /= 2;
        self.reader.plan(wants);
        // Follow only displayed live tails: visible statuses, the displayed
        // pages' witnesses and the open history.
        let mut follows = Vec::new();
        // The open history is followed first, wherever its window is: the
        // follow's `follow_checked` anchor is what proves cached (possibly
        // restored) records were not replaced, so a seek must not skip it,
        // and the follow budget is spent on tree rows only after it.
        if let Some(history) = &self.history {
            follows.push(history.stream.clone());
        }
        let mut seen = BTreeMap::new();
        for at in &visible {
            if let Pos::Row {
                parent,
                page,
                offset,
            } = at
                && let Some(agent) = self
                    .child(parent, *page, *offset)
                    .and_then(|c| agent_of(&c))
                && let Ok(status) = agent.status()
            {
                let tail = self.reader.cache().tail(&status).unwrap_or(0);
                seen.insert(status.clone(), (agent, tail));
                follows.push(status);
            }
        }
        // Each displayed page's witness, so new children appear live.
        let pages = visible
            .iter()
            .map(|at| match at {
                Pos::Row { parent, .. } | Pos::Gap { parent, .. } => parent,
            })
            .chain(ancestors.iter().map(|(parent, _)| parent));
        for parent in pages {
            if let Some(membership) = owner(parent).and_then(|owner| owner.membership().ok()) {
                follows.push(membership);
            }
        }
        // An advancing status marks its agent recently active.
        let mut active = Vec::new();
        for (status, (agent, tail)) in &seen {
            if self
                .seen
                .get(status)
                .is_some_and(|(_, before)| tail > before)
            {
                active.push(agent.clone());
            }
        }
        for agent in &active {
            self.push_recent(agent);
        }
        self.seen = seen;
        self.reader.set_follows(follows);
        // Mark what is displayed most recently used.
        for at in &visible {
            let (parent, page) = match at {
                Pos::Row { parent, page, .. } | Pos::Gap { parent, page } => (parent, *page),
            };
            self.reader.cache_mut().touch_listing(parent, page);
        }
        for (parent, page) in &ancestors {
            self.reader.cache_mut().touch_listing(parent, *page);
        }
        self.rows = visible
            .into_iter()
            .map(|at| {
                let row = self.render(&at);
                (at, row)
            })
            .collect();
        for (_, row) in &self.rows {
            let Row::Agent { path, .. } = row else {
                continue;
            };
            for stream in [path.status(), path.conversation()].into_iter().flatten() {
                if let Some(last) = self
                    .reader
                    .cache()
                    .tail(&stream)
                    .and_then(|t| t.checked_sub(1))
                {
                    self.reader.cache_mut().touch_record(&stream, last);
                }
            }
        }
        self.rows_source = self.reader.source_generation;
        self.rows()
    }

    /// Listing pages of every expanded ancestor of the visible rows.
    fn ancestors(&self, visible: &[Pos]) -> BTreeSet<(StreamPath, u32)> {
        let mut out = BTreeSet::new();
        let mut owners = BTreeSet::new();
        for at in visible {
            let (Pos::Row { parent, .. } | Pos::Gap { parent, .. }) = at;
            owners.insert(parent.clone());
        }
        for parent in owners {
            let mut agent = owner(&parent);
            while let Some(current) = agent {
                let Some(Pos::Row { page, .. }) = self.locate(&current) else {
                    break;
                };
                let Some(listing) = parent_listing(&current) else {
                    break;
                };
                if !out.insert((listing, page)) {
                    break;
                }
                agent = current.parent();
            }
        }
        out
    }

    fn history_wants(&self, history: &History, wants: &mut Vec<(Want, Priority)>) {
        let rows = history.rows as u32;
        if history.end {
            wants.push((
                Want::Last {
                    stream: history.stream.clone(),
                    count: rows,
                },
                Priority::Visible,
            ));
        } else {
            wants.push((
                Want::Range {
                    stream: history.stream.clone(),
                    from: history.from,
                    to: history.from.saturating_add(u64::from(rows)),
                },
                Priority::Visible,
            ));
        }
        // The page before the window is the likely next scroll.
        let start = self.history_start(history);
        if let Some(start) = start
            && start > 0
        {
            wants.push((
                Want::Range {
                    stream: history.stream.clone(),
                    from: start.saturating_sub(u64::from(rows)),
                    to: start,
                },
                Priority::Prefetch,
            ));
        }
    }

    fn history_start(&self, history: &History) -> Option<u64> {
        if history.end {
            let tail = self.reader.cache().tail(&history.stream)?;
            Some(tail.saturating_sub(history.rows as u64))
        } else {
            Some(history.from)
        }
    }

    /// The open history window: cached records, or `None` placeholders.
    #[must_use]
    pub fn history(&self) -> Vec<HistoryRow> {
        let Some(history) = &self.history else {
            return Vec::new();
        };
        let Some(start) = self.history_start(history) else {
            return Vec::new();
        };
        let end = start.saturating_add(history.rows as u64);
        let end = self
            .reader
            .cache()
            .tail(&history.stream)
            .map_or(end, |tail| tail.min(end));
        self.reader
            .cache()
            .records(&history.stream, start, end)
            .map(|(sequence, value)| HistoryRow {
                sequence,
                value: value.cloned(),
            })
            .collect()
    }

    /// Rows of the last frame.
    #[must_use]
    pub fn rows(&self) -> &[(Pos, Row)] {
        if self.reader.readable() && self.rows_source == self.reader.source_generation {
            &self.rows
        } else {
            &[]
        }
    }

    /// Parent stream of the top level.
    #[must_use]
    pub fn root(&self) -> &StreamPath {
        &self.root
    }

    /// Expanded nodes retained.
    #[must_use]
    pub fn expanded(&self) -> usize {
        self.expanded.len()
    }
}
