use crate::{
    Backend, Result, fixtures,
    observe::{Fault, Observed},
    vectors,
};
use acyclic_actors::{addressing::ActorReference, guest::*};
use acyclic_stream::{
    CommitCondition, CommitOutcome, CommitRequest, Record, StreamError, StreamPath,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Counts {
    opened: AtomicUsize,
    sealed: AtomicUsize,
    settled: AtomicUsize,
    conflicts: AtomicUsize,
    condition: Mutex<Option<StreamPath>>,
}

struct ComposerCounts(Arc<Counts>);
impl Composer for ComposerCounts {
    fn open(
        &self,
        _: &ActorReference,
        _: Option<&Checkpoint>,
        _: Option<&Record>,
        _: &Checkpoint,
        _: &Participant,
    ) -> std::result::Result<Box<dyn Composition>, GuestError> {
        self.0.opened.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(ParticipantCounts(self.0.clone())))
    }
}
struct ParticipantCounts(Arc<Counts>);
impl Composition for ParticipantCounts {
    fn seal<'a>(&'a mut self, mut request: CommitRequest) -> GuestFuture<'a, CommitRequest> {
        self.0.sealed.fetch_add(1, Ordering::SeqCst);
        if let Some(path) = self
            .0
            .condition
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            request.conditions.push(CommitCondition::Absent { path });
        }
        Box::pin(async { Ok(request) })
    }
    fn settle<'a>(
        self: Box<Self>,
        outcome: std::result::Result<&'a CommitOutcome, &'a StreamError>,
    ) -> GuestFuture<'a, ()> {
        Box::pin(async move {
            if matches!(
                outcome,
                Err(StreamError::Unavailable | StreamError::InvalidResponse)
            ) {
                return Err(GuestError::Handler("settled an unknown outcome".into()));
            }
            if matches!(outcome, Ok(CommitOutcome::Conflict(_))) {
                self.0.conflicts.fetch_add(1, Ordering::SeqCst);
            }
            self.0.settled.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

struct Participating(Arc<dyn Handler>);
impl Handler for Participating {
    fn manifest(&self) -> &Manifest {
        self.0.manifest()
    }
    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            let mut transition = self.0.on_record(delivery, checkpoint).await?;
            transition.participant = Some(Participant {
                kind: "conformance".into(),
                plan: bytes::Bytes::new(),
            });
            Ok(transition)
        })
    }
}

pub(crate) async fn verify(backend: &dyn Backend) -> Result {
    for fault in [Fault::Before, Fault::After, Fault::Refuse] {
        publication(backend, fault).await?;
    }
    conflict(backend).await
}

async fn conflict(backend: &dyn Backend) -> Result {
    let storage = backend.fresh().await?;
    let provider = Arc::new(Observed::new(storage.open().await?));
    let counts = Arc::new(Counts::default());
    let handler = backend.handler(Arc::new(Participating(vectors::handler(&provider)?)))?;
    let host = vectors::hosting(
        provider.clone(),
        handler,
        Some(Arc::new(ComposerCounts(counts.clone()))),
    )?;
    let root = host
        .create(&vectors::intent("create")?, fixtures::adopted())
        .await?;
    vectors::drain(&host).await?;
    host.send(&root, &fixtures::sender(), &fixtures::request("once")?)
        .await?;
    let state = host.state(&root).await?;
    let settled = counts.settled.load(Ordering::SeqCst);
    // This is a real provider conflict against an existing path, not a canned
    // response: the participant's condition must join the actor transaction.
    *counts.condition.lock().unwrap_or_else(|e| e.into_inner()) = Some(root.inbox()?);
    if host.runtime().drain().await.is_empty()
        || counts.settled.load(Ordering::SeqCst) != settled + 1
        || counts.conflicts.load(Ordering::SeqCst) != 1
        || host.state(&root).await? != state
        || !vectors::values(&*provider, root.conversation()?)
            .await?
            .is_empty()
    {
        return Err("conflicting publication did not settle without consuming input".into());
    }
    Ok(())
}

async fn publication(backend: &dyn Backend, fault: Fault) -> Result {
    let storage = backend.fresh().await?;
    let provider = Arc::new(Observed::new(storage.open().await?));
    let counts = Arc::new(Counts::default());
    let handler = backend.handler(Arc::new(Participating(vectors::handler(&provider)?)))?;
    let host = vectors::hosting(
        provider.clone(),
        handler,
        Some(Arc::new(ComposerCounts(counts.clone()))),
    )?;
    let root = host
        .create(&vectors::intent("create")?, fixtures::adopted())
        .await?;
    vectors::drain(&host).await?;
    host.send(&root, &fixtures::sender(), &fixtures::request("once")?)
        .await?;
    let before = provider.commits().len();
    let settled = counts.settled.load(Ordering::SeqCst);
    provider.arm(0, fault);
    if host.runtime().drain().await.is_empty() {
        return Err(format!("{fault:?} did not interrupt publication").into());
    }
    let terminal = matches!(fault, Fault::Refuse);
    if counts.settled.load(Ordering::SeqCst) != settled + usize::from(terminal) {
        return Err("participant settled before a terminal result".into());
    }
    let seals = counts.sealed.load(Ordering::SeqCst);
    let opens = counts.opened.load(Ordering::SeqCst);
    if !terminal {
        // A refusal of a retry cannot resolve an earlier lost acknowledgement.
        provider.arm(0, Fault::Refuse);
        host.host(&root)?;
        if host.runtime().drain().await.is_empty()
            || counts.settled.load(Ordering::SeqCst) != settled
        {
            return Err("a retry refusal settled an earlier unknown result".into());
        }
    }
    host.host(&root)?;
    vectors::drain(&host).await?;
    let commits = provider.commits();
    if !terminal {
        let Some([first, refused_retry, retry]) = commits.get(before..) else {
            return Err("unknown publication issued a second commit".into());
        };
        if first != retry || first != refused_retry {
            return Err("unknown publication retried different bytes".into());
        }
        if counts.sealed.load(Ordering::SeqCst) != seals
            || counts.opened.load(Ordering::SeqCst) != opens
        {
            return Err("unknown publication reopened or resealed its participant".into());
        }
    }
    if counts.settled.load(Ordering::SeqCst) != settled + 1 + usize::from(terminal) {
        return Err("terminal publication did not settle exactly once".into());
    }
    if vectors::values(&*provider, root.conversation()?).await? != [b"once".to_vec()] {
        return Err("publication duplicated or lost its turn effect".into());
    }
    Ok(())
}
