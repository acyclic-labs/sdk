//! Original local operations and their original physical fault controller.
use acyclic_fs::GenerationId;
use acyclic_native_runtime::testing::{DurableFault, DurableWrite, InjectedStorageFault, Schedule};
use acyclic_objects::{LocalMedium, Medium, MediumError, local::LocalObjectsLimits};
use acyclic_stream::OwnedBytes;
use std::{error::Error, path::Path};

#[derive(Clone, Copy, Debug)]
enum Mutation {
    Create,
    Replace,
    Remove,
}

/// Mutation and durability acknowledgement are separate original operations.
#[derive(Debug)]
pub enum Terminal {
    Mutation(Result<bool, MediumError>),
    Flush(Result<(), MediumError>),
}

/// Actual touched units and original outcome, retained for counterexamples.
#[derive(Debug)]
pub struct Report {
    pub generation: GenerationId,
    pub operation: &'static str,
    pub baseline: Vec<DurableWrite>,
    pub writes: Vec<DurableWrite>,
    pub boundaries: usize,
    pub fault: InjectedStorageFault,
    pub terminal: Terminal,
    pub visible: Result<Option<OwnedBytes>, MediumError>,
    pub recovered: Option<OwnedBytes>,
}

struct Case<'a> {
    generation: GenerationId,
    mutation: Mutation,
    flush: bool,
    fault: DurableFault,
    ordinal: u64,
    payload: &'a [u8],
}

impl<'a> Case<'a> {
    fn parse(bytes: &'a [u8]) -> Option<Self> {
        // This is generator configuration, not a second persisted decoder.
        let (config, rest) = bytes.split_at_checked(20)?;
        let (identity, payload) = rest.split_at_checked(usize::from(config[3]))?;
        let prefix = u64::from_le_bytes(config[12..20].try_into().ok()?);
        Some(Self {
            generation: GenerationId::from_bytes(identity)?,
            mutation: match config[0] % 3 {
                0 => Mutation::Create,
                1 => Mutation::Replace,
                _ => Mutation::Remove,
            },
            flush: config[1] & 1 != 0,
            fault: match config[2] % 5 {
                0 => DurableFault::Before,
                1 => DurableFault::After,
                2 => DurableFault::Failed,
                3 => DurableFault::DiskFull,
                _ => DurableFault::Torn(usize::try_from(prefix).unwrap_or(usize::MAX)),
            },
            ordinal: u64::from_le_bytes(config[4..12].try_into().ok()?),
            payload,
        })
    }

    fn value(&self, tag: u8) -> OwnedBytes {
        let mut value = Vec::with_capacity(self.payload.len() + 1);
        value.push(tag);
        value.extend_from_slice(self.payload);
        value.into()
    }

    fn before(&self) -> Option<OwnedBytes> {
        (!matches!(self.mutation, Mutation::Create)).then(|| self.value(0))
    }

    fn after(&self) -> Option<OwnedBytes> {
        (!matches!(self.mutation, Mutation::Remove)).then(|| self.value(1))
    }

    async fn mutate(&self, medium: &LocalMedium, key: &str) -> Result<bool, MediumError> {
        match self.mutation {
            Mutation::Create => medium.create(key, self.value(1)).await,
            Mutation::Replace => medium.replace(key, &self.value(0), self.value(1)).await,
            Mutation::Remove => medium.remove_if(key, &self.value(0)).await,
        }
    }

    async fn prepare(&self, root: &Path, key: &str) -> Result<LocalMedium, Box<dyn Error>> {
        let medium = LocalMedium::open(root, LocalObjectsLimits::default(), None).await?;
        if let Some(before) = self.before() {
            assert!(medium.create(key, before).await?, "prepare original value");
            medium.flush(&[key.to_owned()]).await?;
        }
        if self.flush {
            // The later flush must never replace this mutation's outcome.
            assert!(
                self.mutate(&medium, key).await?,
                "prepare unflushed mutation"
            );
        }
        Ok(medium)
    }

    async fn operation(&self, medium: &LocalMedium, key: &str) -> Terminal {
        if self.flush {
            Terminal::Flush(medium.flush(&[key.to_owned()]).await)
        } else {
            Terminal::Mutation(self.mutate(medium, key).await)
        }
    }
}

struct Execution {
    terminal: Terminal,
    writes: Vec<DurableWrite>,
    fault: Option<InjectedStorageFault>,
    boundaries: usize,
}

async fn execute(
    case: &Case<'_>,
    medium: &LocalMedium,
    key: &str,
    cut: Option<usize>,
) -> Result<Execution, Box<dyn Error>> {
    let (schedule, mut events) = Schedule::new();
    let driver = tokio::spawn(async move {
        let mut boundaries = 0;
        while let Some(boundary) = events.recv().await {
            boundaries += 1;
            boundary.release();
        }
        boundaries
    });
    if let Some(cut) = cut {
        schedule.cut_at(cut, case.fault);
    }
    let terminal = schedule.scope(case.operation(medium, key)).await;
    // Accepted original owners produce retirement; no timer or readiness probe.
    schedule.drained().await;
    assert_eq!(schedule.pending_native_work(), 0);
    let writes = schedule.durable_writes();
    let fault = schedule.storage_fault();
    // Dropping the last producer closes recv and completes the event driver.
    drop(schedule);
    let boundaries = driver.await?;
    Ok(Execution {
        terminal,
        writes,
        fault,
        boundaries,
    })
}

fn retained_cause(terminal: &Terminal) -> Option<&InjectedStorageFault> {
    let error = match terminal {
        Terminal::Mutation(Err(error)) | Terminal::Flush(Err(error)) => error,
        _ => return None,
    };
    let (MediumError::Failed(error) | MediumError::Ambiguous(error)) = error;
    error.get_ref()?.downcast_ref()
}

/// Select one actual physical site, inject one fault, and reopen the real log.
/// No returned outcome is supplied by a replacement store or replay model.
pub fn check(bytes: &[u8]) -> Result<Option<Report>, Box<dyn Error>> {
    let mut report = None;
    check_sites(bytes, false, |value| report = Some(value))?;
    Ok(report)
}

/// Exercise every site in one original successful trace without retaining a
/// history of reports or repeating the successful baseline operation.
pub fn check_each_site(bytes: &[u8], observe: impl FnMut(Report)) -> Result<bool, Box<dyn Error>> {
    check_sites(bytes, true, observe)
}

fn check_sites(
    bytes: &[u8],
    all_sites: bool,
    mut observe: impl FnMut(Report),
) -> Result<bool, Box<dyn Error>> {
    let Some(case) = Case::parse(bytes) else {
        return Ok(false);
    };
    acyclic_native_runtime::initialize_native_io_services()?;
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    runtime.block_on(async {
        let key = format!(
            "schedule/{}",
            hex::encode(case.generation.to_bytes().as_slice())
        );
        let baseline_root = tempfile::tempdir()?;
        let baseline_medium = case.prepare(baseline_root.path(), &key).await?;
        let mut baseline = execute(&case, &baseline_medium, &key, None).await?;
        assert!(
            matches!(
                baseline.terminal,
                Terminal::Mutation(Ok(true)) | Terminal::Flush(Ok(()))
            ),
            "original operation must succeed without a fault: {:?}",
            baseline.terminal
        );
        assert_eq!(baseline_medium.read(&key, None).await?, case.after());
        assert!(baseline.fault.is_none());
        assert!(
            !baseline.writes.is_empty(),
            "original physical operation escaped the fault controller"
        );
        drop(baseline_medium);

        let cut = (case.ordinal % baseline.writes.len() as u64) as usize;
        let cuts = if all_sites {
            0..baseline.writes.len()
        } else {
            cut..cut + 1
        };
        let last_cut = cuts.end - 1;
        for cut in cuts {
            let root = tempfile::tempdir()?;
            let medium = case.prepare(root.path(), &key).await?;
            let actual = execute(&case, &medium, &key, Some(cut)).await?;
            let fault = actual
                .fault
                .expect("selected original physical site was reached");
            assert_eq!(fault.ordinal, cut);
            assert_eq!(fault.fault, case.fault);
            assert_eq!(fault.operation, baseline.writes[cut].operation);
            // The controller latches the first fault: later physical calls cannot
            // escape it. Count actual touched calls rather than a wall-time budget.
            assert_eq!(actual.writes.len(), cut + 1);
            for (observed, original) in actual.writes.iter().zip(&baseline.writes) {
                assert_eq!(observed.operation, original.operation);
                assert_eq!(observed.requested, original.requested);
            }
            if matches!(
                actual.terminal,
                Terminal::Mutation(Err(_)) | Terminal::Flush(Err(_))
            ) {
                assert_eq!(
                    retained_cause(&actual.terminal),
                    Some(&fault),
                    "the original caller retains the first physical cause: {:?}",
                    actual.terminal
                );
            }
            let visible = medium.read(&key, None).await;
            if let Ok(value) = &visible {
                assert!(
                    *value == case.before() || *value == case.after(),
                    "torn bytes became visible: {value:?}"
                );
            }
            drop(medium);
            let reopened =
                LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await?;
            let recovered = reopened.read(&key, None).await?;
            assert!(
                recovered == case.before() || recovered == case.after(),
                "recovery exposed a torn mutation: {recovered:?}"
            );
            match &actual.terminal {
                Terminal::Mutation(Ok(true)) | Terminal::Flush(_) => {
                    assert_eq!(
                        recovered,
                        case.after(),
                        "a successful visible mutation was lost"
                    );
                }
                Terminal::Mutation(Ok(false) | Err(MediumError::Failed(_))) => {
                    assert_eq!(
                        recovered,
                        case.before(),
                        "no-effect result hid an original visible effect: {:?}; fault {fault:?}",
                        actual.terminal
                    );
                    if let Ok(value) = &visible {
                        assert_eq!(
                            *value,
                            case.before(),
                            "no-effect result hid a visible mutation"
                        );
                    }
                }
                Terminal::Mutation(Err(MediumError::Ambiguous(_))) => {}
            }
            observe(Report {
                generation: case.generation,
                operation: match case.mutation {
                    Mutation::Create => "create",
                    Mutation::Replace => "replace",
                    Mutation::Remove => "remove-if",
                },
                baseline: if cut == last_cut {
                    std::mem::take(&mut baseline.writes)
                } else {
                    baseline.writes.clone()
                },
                writes: actual.writes,
                boundaries: actual.boundaries,
                fault,
                terminal: actual.terminal,
                visible,
                recovered,
            });
        }
        Ok(true)
    })
}

/// Actual manifest and complete generation-reference writer outputs, preceded
/// only by the generator's mutation/stage/fault/ordinal configuration.
pub fn seeds() -> Vec<(String, Vec<u8>)> {
    use acyclic_fs::{Digest, FileId, ObjectId, ObjectKind, ObjectStorage};
    let payload = acyclic_objects::Manifest::default().encode();
    let mut seeds = Vec::new();
    for sealed in [false, true] {
        let generation = GenerationId::from_root(ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: Digest::from_bytes([11; 32]),
            storage: sealed.then_some(ObjectStorage {
                locator: Digest::from_bytes([12; 32]),
                root_file_id: FileId::from_bytes([13; 16]),
                origin_generation: Digest::from_bytes([14; 32]),
            }),
        });
        let identity = generation.to_bytes();
        for mutation in 0..3 {
            for flush in 0..2 {
                for fault in 0..5 {
                    let mut bytes = vec![mutation, flush, fault, identity.len() as u8];
                    bytes.extend_from_slice(&0_u64.to_le_bytes());
                    bytes.extend_from_slice(&1_u64.to_le_bytes());
                    bytes.extend_from_slice(identity.as_slice());
                    bytes.extend_from_slice(&payload);
                    seeds.push((format!("medium-{sealed}-{mutation}-{flush}-{fault}"), bytes));
                }
            }
        }
    }
    seeds
}
