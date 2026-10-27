//! Shared private host setup; identity vectors enter the same production host.
use super::*;

pub(crate) struct Fixture {
    pub path: AgentPath,
    pub binding: VolumeBinding,
    pub provider: Arc<Observed>,
    scheduler: Arc<Observed>,
    filesystem:
        Arc<FilesystemHost<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>>,
    _storage: Box<dyn crate::Storage>,
}

impl Fixture {
    pub async fn new(backend: &dyn Backend) -> Result<Self> {
        let storage = backend.fresh().await?;
        let raw = storage.open().await?;
        let scheduler = Arc::new(Observed::new(raw.clone()));
        let provider = Arc::new(Observed::new(raw));
        let mut adopted = vectors::adopted();
        let path = AgentPath::root(&adopted.agent);
        let reference = ProviderRef::new(
            "conformance".into(),
            "filesystem".into(),
            "1".into(),
            &mut None,
        )?;
        let filesystem = FilesystemHost::new(acyclic_fs::Fs::memory(), &reference, None)?;
        let binding = VolumeBinding {
            name: PRIVATE_VOLUME.into(),
            volume: VolumeRef::new(
                reference,
                "private".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(acyclic_harness_core::AgentId::from_bytes(
                    path.id().to_bytes(),
                )),
                &mut None,
            )?,
        };
        filesystem.create_volume(&binding.volume).await?;
        adopted.volumes.push(binding.clone());
        let host = vectors::host(backend, scheduler.clone())?;
        host.create(&vectors::intent("create")?, adopted).await?;
        host.send(&path, &vectors::sender(), &vectors::request("history")?)
            .await?;
        vectors::drain(&host).await?;
        drop(host);
        Ok(Self {
            path,
            binding,
            provider,
            scheduler,
            filesystem,
            _storage: storage,
        })
    }

    pub fn no_turn_io(&self) -> bool {
        self.provider.reads.load(Ordering::SeqCst) == 0
            && self.provider.writes.load(Ordering::SeqCst) == 0
    }

    pub async fn check(
        self,
        backend: &dyn Backend,
        root: AuthorityRoot,
        admission: TurnAdmission,
        allowed: bool,
        label: &str,
    ) -> Result {
        let executors = Arc::new(Executors(AtomicUsize::new(0)));
        let driver = ExecutorDriver::new(FilesystemTurnHost::new(
            self.provider.clone(),
            self.filesystem.clone(),
            root,
            admission,
            executors.clone(),
            TurnLimits {
                maximum_bytes: 64 * 1024,
                max_steps: 4,
                history_turns: 8,
            },
        ));
        let handler = backend.handler(Arc::new(AgentHandler::new(
            actor_store(&self.scheduler)?,
            driver,
            vectors::limits()?,
        )?))?;
        let host = vectors::hosting(self.scheduler.clone(), handler, None)?;
        host.host(&self.path)?;
        host.send(&self.path, &vectors::sender(), &vectors::request("next")?)
            .await?;
        if host.runtime().drain().await.is_empty() {
            return Err(format!("{label} did not reach the admission/history sentinel").into());
        }
        if allowed && self.provider.reads.load(Ordering::SeqCst) == 0 {
            return Err(format!("{label}: valid admission did not read nonempty history").into());
        }
        if !allowed && (!self.no_turn_io() || executors.0.load(Ordering::SeqCst) != 0) {
            return Err(
                format!("{label}: refusal performed turn IO or invoked its executor").into(),
            );
        }
        Ok(())
    }
}
