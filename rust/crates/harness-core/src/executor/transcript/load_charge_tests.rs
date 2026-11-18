//! A loaded journal payload carries exactly one read charge: its own capacity,
//! released once when the loaded bytes drop. A second guard over the same bytes
//! would release that charge again.
use super::*;

const BUDGET: u64 = 16 * 1024 * 1024;

/// Live bytes on the standalone ledger that every custody of `io` shares.
fn ledger_live(io: &PreparationIo) -> Result<u64> {
    match io {
        PreparationIo::Owned { work, .. } => {
            let state = work
                .lock()
                .map_err(|_| Error::Invalid("poisoned test ledger".into()))?;
            state
                .1
                .as_ref()
                .map(|ledger| ledger.live_bytes())
                .map_err(|cause| acyclic_fs::FsError::from(*cause).into())
        }
        _ => Err(Error::Invalid("expected finite test ledger".into())),
    }
}

#[derive(Clone, Copy, Debug)]
enum Path {
    Json,
    Projection,
    Manifest,
    Request,
    Observation,
}

const PATHS: [Path; 5] = [
    Path::Json,
    Path::Projection,
    Path::Manifest,
    Path::Request,
    Path::Observation,
];

struct Payloads {
    record: FileRef,
    manifest: FileRef,
    request: FileRef,
    observation: ExecutionEvent,
}

impl Payloads {
    async fn stage(journal: &dyn ExecutionJournal) -> Result<Self> {
        let operation = OperationId::new();
        let prepared = crate::model::PreparedModelRequest::prepare(
            ModelRequest {
                summary: false,
                compose_tools: false,
                model: Model::new("test".into(), "charge".into(), "1".into())?,
                messages: vec![ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("payload".repeat(256)),
                }],
                tools: Vec::new(),
                sampling: Default::default(),
            },
            Limits::default(),
            None,
        )?;
        let manifest = stage_request(journal, operation, "manifest", &prepared, None, None).await?;
        let request = stage_bytes(
            journal,
            operation,
            "request",
            (prepared.bytes(), "application/json"),
            true,
            None,
        )
        .await?;
        let record = stage_json(
            journal,
            operation,
            "projection",
            &ProjectionRecord {
                version: 2,
                request: manifest.clone(),
                projection: ProjectionMetadata {
                    context: ContextMetadata {
                        metadata: std::collections::BTreeMap::new(),
                        current_input_index: None,
                    },
                    steering: None,
                    canonical: None,
                    checkpoint: None,
                    working: None,
                    accounting: None,
                },
            },
            None,
        )
        .await?;
        let model = ModelEvent::Content {
            delta: "observed ".repeat(512),
        };
        let observation = ExecutionEvent::Model {
            step: 1,
            purpose: ModelPurpose::Response,
            event: stage_json(journal, operation, "observation", &model, None).await?,
            usage: None,
        };
        Ok(Self {
            record,
            manifest,
            request,
            observation,
        })
    }

    fn file(&self, path: Path) -> Result<&FileRef> {
        Ok(match path {
            Path::Json | Path::Projection => &self.record,
            Path::Manifest => &self.manifest,
            Path::Request => &self.request,
            Path::Observation => match &self.observation {
                ExecutionEvent::Model { event, .. } => event,
                _ => return Err(Error::Invalid("observation is not staged".into())),
            },
        })
    }

    /// One loader path, dropping everything it returns.
    async fn load(
        &self,
        journal: &dyn ExecutionJournal,
        path: Path,
        io: &PreparationIo,
    ) -> Result<()> {
        match path {
            Path::Json => drop(
                super::super::payload::load_json::<ProjectionRecord>(
                    journal,
                    &self.record,
                    Some(io),
                )
                .await?,
            ),
            Path::Projection => {
                drop(load_projection_record(journal, &self.record, Some(io)).await?)
            }
            Path::Manifest => drop(load_manifest(journal, &self.manifest, Some(io)).await?),
            Path::Request => drop(
                super::super::payload::load_model_request(
                    journal,
                    &self.request,
                    Limits::default(),
                    Some(io),
                )
                .await?,
            ),
            Path::Observation => drop(
                super::super::model_observation::load(journal, &self.observation, Some(io)).await?,
            ),
        }
        Ok(())
    }

    /// The same path's decode over bytes the caller already holds.
    fn decode(&self, path: Path, bytes: &[u8], io: &PreparationIo) -> Result<()> {
        match path {
            Path::Json => drop(super::super::payload::decode_json::<ProjectionRecord>(
                bytes,
                Some(io),
            )?),
            Path::Projection => drop(retain_preparation(super::super::payload::decode_json::<
                ProjectionRecord,
            >(bytes, Some(io))?)?),
            Path::Manifest => drop(decode_manifest(&self.manifest, bytes, Some(io))?),
            Path::Request => drop(crate::model::PreparedModelRequest::decode(
                bytes,
                Limits::default(),
                Some(io),
            )?),
            Path::Observation => drop(super::super::model_observation::decode_payload(
                "application/json",
                bytes,
                Some(io),
            )?),
        }
        Ok(())
    }

    /// Ledger bytes left after one read held under its single charge, or, with
    /// `reguard`, under a second guard over the same bytes as well.
    async fn reference(
        &self,
        journal: &dyn ExecutionJournal,
        path: Path,
        reguard: bool,
    ) -> Result<u64> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let io = PreparationIo::from_budget(
            acyclic_fs_work::performance::work_budget(BUDGET, BUDGET),
            &cancellation,
        )?;
        let bytes = journal.load(self.file(path)?, Some(&io))?.await?;
        let guard = if reguard {
            io.guard_bytes(&bytes)?
        } else {
            None
        };
        self.decode(path, &bytes, &io)?;
        drop(guard);
        drop(bytes);
        ledger_live(&io)
    }
}

#[tokio::test]
async fn a_loaded_payload_releases_only_its_single_read_charge() -> Result<()> {
    let storage =
        crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 1 << 20).await?;
    let journal = storage.journal();
    let journal = journal.as_ref();
    let payloads = Payloads::stage(journal).await?;
    for path in PATHS {
        // The read charge is exactly the payload's capacity, given back when
        // the loaded bytes drop.
        let cancellation = acyclic_fs::CancellationToken::new();
        let io = PreparationIo::from_budget(
            acyclic_fs_work::performance::work_budget(BUDGET, BUDGET),
            &cancellation,
        )?;
        let loaded = journal.load(payloads.file(path)?, Some(&io))?.await?;
        let capacity = loaded
            .retained_capacity()
            .ok_or_else(|| Error::Invalid("loaded bytes expose no capacity".into()))?;
        let held = ledger_live(&io)?;
        drop(loaded);
        assert_eq!(held - ledger_live(&io)?, capacity as u64, "{path:?}");

        // Each loader leaves the ledger exactly where one charged read and
        // its decode leave it: no second guard releases the charge again.
        let expected = payloads.reference(journal, path, false).await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let io = PreparationIo::from_budget(
            acyclic_fs_work::performance::work_budget(BUDGET, BUDGET),
            &cancellation,
        )?;
        payloads.load(journal, path, &io).await?;
        assert_eq!(ledger_live(&io)?, expected, "{path:?}");

        // Negative control: a second guard over the loaded bytes is visible.
        assert_ne!(
            payloads.reference(journal, path, true).await.ok(),
            Some(expected),
            "{path:?}"
        );
    }
    Ok(())
}
