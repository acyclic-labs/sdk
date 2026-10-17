//! Shared production filesystem fixture for client file projections.
use acyclic_fs::Fs;
use acyclic_harness::{
    AgentId, Capabilities, IdempotencyKey, Result,
    context::ContextRoot,
    conversation::{
        ContentGrant, ContentResidencyVerifier, VolumeClass, VolumeOperation, VolumeOwner,
        VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityIssuer},
    filesystem::{FilesystemContentVerifier, FilesystemHost},
    resources::ProviderRef,
};
use std::sync::Arc;

pub struct Fixture {
    pub(super) host:
        Arc<FilesystemHost<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>>,
    pub root: ContextRoot,
    writer: ContentGrant,
    pub reader: Arc<dyn ContentResidencyVerifier>,
}

impl Fixture {
    pub async fn new(files: &[(&str, &str)]) -> Result<Self> {
        let provider = ProviderRef::new(
            "commands".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?;
        let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
        let agent = AgentId::from_bytes([1; 16]);
        let volume = VolumeRef::new(
            provider,
            "sources".into(),
            VolumeClass::SessionShared,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        let issuer = AuthorityIssuer::new(
            "commands",
            [9; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "test".into(),
            },
        );
        let scope = issuer
            .issue(
                "owner",
                &Capabilities::new([
                    volume.capability(VolumeOperation::Read, &mut None)?,
                    volume.capability(VolumeOperation::Write, &mut None)?,
                    volume.directory_read_capability("", &mut None)?,
                ]),
                Some(agent),
                None,
                &mut None,
            )
            .expect("scope proof");
        host.create_volume(&volume).await?;
        let writer = ContentGrant::verify(
            &issuer.verifier(&mut None).expect("verifier"),
            &scope,
            &volume,
            VolumeOperation::Write,
            u64::MAX,
            &mut None,
        )?;
        let reader = FilesystemContentVerifier::new(
            host.clone(),
            &issuer.verifier(&mut None).expect("verifier"),
            &scope,
            32_768,
            None,
        )?;
        let fixture = Self {
            host,
            root: ContextRoot {
                volume,
                directory: String::new(),
            },
            writer,
            reader,
        };
        for (path, text) in files {
            fixture.put(path, text, path).await?;
        }
        Ok(fixture)
    }

    pub async fn put(&self, path: &str, text: &str, key: &str) -> Result<()> {
        self.put_file(path, text.as_bytes(), "text/plain", key)
            .await?;
        Ok(())
    }

    pub async fn put_file(
        &self,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        key: &str,
    ) -> Result<acyclic_harness::conversation::FileRef> {
        self.host
            .put_content(
                &self.root.volume,
                &self.writer,
                path,
                bytes,
                media_type,
                "source.md",
                1_000_000,
                &IdempotencyKey::new(key)?,
                None,
                None,
            )
            .await
    }
}
