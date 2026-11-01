use super::*;
type PendingPage<N> = (
    ObjectId,
    Option<(N, ProofGuard)>,
    Option<(N, ProofGuard)>,
    bool,
);
type DirectoryProofs<'a> = ProofMap<'a, FileId, ProofVec<'a, (TreePage, ProofGuard)>>;
type FileTableProof<'a> = (
    ProofMap<'a, FileId, FileRecord>,
    Option<DirectoryProofs<'a>>,
);
mod bindings;
mod incremental;

struct BindingProof<'a> {
    root: ObjectId,
    leaves: ProofVec<'a, (crate::kernel::bindings::BindingPage, ProofGuard)>,
}

#[derive(Default)]
struct NamespaceFact {
    links: u64,
    reachable: bool,
    visited_directory: bool,
}

pub(crate) struct ProofContext<'a, S> {
    store: &'a S,
    limits: ClosureLimits,
    objects: ProofSet<'a>,
    publication: Option<PublicationOrder<'a>>,
    // ObjectId includes its kind: complete blob and extent proofs can share
    // one invocation-owned length table without a second allocation domain.
    proven_lengths: ProofMap<'a, ObjectId, u64, { ObjectId::ENCODED_LEN }>,
    binding_proofs: ProofMap<'a, FileId, BindingProof<'a>>,
    pub(super) meter: ProofMeter<'a>,
    object_bytes: u64,
}

struct PublicationOrder<'a> {
    objects: ProofVec<'a, ObjectId>,
    completed: crate::kernel::allocation::fixed_index::FixedIndex<bool, { ObjectId::ENCODED_LEN }>,
}

impl<'a, S: crate::AsyncObjectStore> ProofContext<'a, S> {
    pub(crate) fn new(store: &'a S, limits: ClosureLimits, meter: ProofMeter<'a>) -> Self {
        Self {
            store,
            limits,
            objects: ProofSet::new(&meter),
            publication: None,
            proven_lengths: ProofMap::new(&meter),
            binding_proofs: ProofMap::new(&meter),
            object_bytes: 0,
            meter,
        }
    }

    pub(crate) fn publication_order(mut self) -> Self {
        self.publication = Some(PublicationOrder {
            objects: ProofVec::new(&self.meter),
            completed: Default::default(),
        });
        self
    }

    fn complete(&mut self, object: ObjectId) -> Result<(), crate::FsError> {
        if let Some(order) = &mut self.publication {
            let completed = self.meter.with_accounting(|accounting| {
                order
                    .completed
                    .get_or_try_insert_with(object.index_key(), accounting, |_| Ok(false))
            })?;
            if !*completed {
                *completed = true;
                order.objects.push(object)?;
            }
        }
        Ok(())
    }

    pub(crate) async fn prove_record_closure(
        &mut self,
        records: &[FileRecord],
    ) -> Result<(Vec<ObjectId>, ProofGuard), crate::FsError> {
        for record in records {
            self.meter.charge(WorkCounters::UNCHARGED)?;
            self.prove_record(record).await?;
        }
        self.take_objects()
    }

    fn examine(&self, count: usize) -> Result<(), crate::FsError> {
        self.meter.charge(WorkCounters {
            items_examined: usize_to_u64(count),
            ..WorkCounters::default()
        })
    }

    pub(super) fn take_objects(&mut self) -> Result<(Vec<ObjectId>, ProofGuard), crate::FsError> {
        if let Some(order) = &mut self.publication {
            return std::mem::replace(&mut order.objects, ProofVec::new(&self.meter)).into_parts();
        }
        let objects = std::mem::replace(&mut self.objects, ProofSet::new(&self.meter));
