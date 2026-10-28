//! Backend provisioning only: all host vectors and assertions stay in the suite.
use acyclic_agent_host_conformance::{Backend, Objects, Result, Storage};
use acyclic_objects::{
    Store,
    s3::{Capabilities as S3Capabilities, S3Medium},
};
use acyclic_stream::{
    BoxProviderFuture, LocalStream, LocalStreamLimits, StreamProvider, postgres::PostgresStream,
};
use std::{sync::Arc, time::Instant};

#[path = "endpoint.rs"]
mod endpoint;

struct Providers {
    database: Option<String>,
    objects: Option<Arc<dyn acyclic_objects::s3::Bucket>>,
}

struct Domain {
    providers: Providers,
    namespace: String,
    directory: tempfile::TempDir,
}

impl Providers {
    fn domain(&self) -> Result<Domain> {
        Ok(Domain {
            providers: Providers {
                database: self.database.clone(),
                objects: self.objects.clone(),
            },
            namespace: uuid::Uuid::new_v4().to_string(),
            directory: tempfile::tempdir()?,
        })
    }
}

impl Backend for Providers {
    fn serving(
        &self,
        policy: acyclic_harness_core::approvals::ApprovalPolicy,
    ) -> BoxProviderFuture<'_, Result<Box<dyn acyclic_agent_host_conformance::protocol::Endpoint>>>
    {
        Box::pin(acyclic_agent_host_conformance::protocol::embedded::open(
            self, policy,
        ))
    }

    fn fresh(&self) -> BoxProviderFuture<'_, Result<Box<dyn Storage>>> {
        Box::pin(async move { Ok(Box::new(self.domain()?) as Box<dyn Storage>) })
    }

    fn handler(
        &self,
        handler: Arc<dyn acyclic_actors::guest::Handler>,
    ) -> Result<Arc<dyn acyclic_actors::guest::Handler>> {
        acyclic_agent_host_conformance::remote(handler)
    }
}

impl Storage for Domain {
    fn staging_directory(&self) -> &std::path::Path {
        self.directory.path()
    }
    fn open(&self) -> BoxProviderFuture<'_, Result<Arc<dyn StreamProvider>>> {
        Box::pin(async move {
            match &self.providers.database {
                Some(database) => Ok(Arc::new(
                    PostgresStream::connect(database, &self.namespace).await?,
                ) as Arc<dyn StreamProvider>),
                None => Ok(Arc::new(
                    LocalStream::open(self.directory.path(), LocalStreamLimits::default()).await?,
                ) as Arc<dyn StreamProvider>),
            }
        })
    }

    fn objects(&self) -> BoxProviderFuture<'_, Result<Objects>> {
        Box::pin(async move {
            let Some(store) = &self.providers.objects else {
                return Ok(Objects::memory());
            };
            let medium = S3Medium::new(
                store.clone(),
                &format!("agent-host/{}", self.namespace),
                S3Capabilities::REQUIRED,
            )?;
            Ok(Objects {
                provider: Arc::new(Store::new(medium).account(b"host")?),
                capabilities: acyclic_fs::EmbeddedCapabilities { durable: true },
            })
        })
    }
}

pub async fn verify(
    database: &str,
    store: Arc<dyn acyclic_objects::s3::Bucket>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn acyclic_objects::s3::Bucket> =
        if std::env::var_os("STORAGE_IO_TRACE").is_some() {
            Arc::new(super::s3::observed_store::ObservedStore {
                inner: store,
                body_bytes: 0.into(),
                body_gets: 0.into(),
                metadata_bytes: 0.into(),
                envelope_creates: 0.into(),
                multipart_parts: 0.into(),
            })
        } else {
            store
        };
    for (name, database, objects) in [
        ("Postgres + memory objects", Some(database.to_owned()), None),
        ("local streams + MinIO", None, Some(store.clone())),
        ("Postgres + MinIO", Some(database.to_owned()), Some(store)),
    ] {
        let started = Instant::now();
        let providers = Providers { database, objects };
        acyclic_agent_host_conformance::verify(&providers)
            .await
            .map_err(|error| error as Box<dyn std::error::Error>)?;
        println!(
            "Agent-host unchanged vectors over {name}: {:?}",
            started.elapsed()
        );
        let domain = providers
            .domain()
            .map_err(|error| error as Box<dyn std::error::Error>)?;
        let started = Instant::now();
        match &domain.providers.database {
            Some(database) => {
                endpoint::verify(&domain, async || {
                    Ok(Arc::new(
                        PostgresStream::connect(database, &domain.namespace).await?,
                    ))
                })
                .await
            }
            None => {
                endpoint::verify(&domain, async || {
                    Ok(Arc::new(
                        LocalStream::open(
                            domain.directory.path().join("stream"),
                            LocalStreamLimits::default(),
                        )
                        .await?,
                    ))
                })
                .await
            }
        }
        .map_err(|error| error as Box<dyn std::error::Error>)?;
        println!(
            "OSS endpoint prompt/follow/reconnect/reopen over {name}: {:?}",
            started.elapsed()
        );
    }
    Ok(())
}
