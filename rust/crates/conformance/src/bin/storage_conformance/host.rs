//! Backend provisioning only: all host vectors and assertions stay in the suite.
use acyclic_agent_host_conformance::{Backend, Objects, Result, Storage};
use acyclic_objects::{
    ObjectsProvider as _,
    s3::{Capabilities, S3Objects},
    wire,
};
use acyclic_stream::{
    BoxProviderFuture, LocalStream, LocalStreamLimits, StreamProvider, postgres::PostgresStream,
};
use object_store::{ObjectStore, path::Path};
use std::{sync::Arc, time::Instant};

struct Providers {
    database: Option<String>,
    objects: Option<Arc<dyn ObjectStore>>,
    capabilities: Capabilities,
}

struct Domain {
    providers: Providers,
    namespace: String,
    directory: tempfile::TempDir,
}

impl Backend for Providers {
    fn fresh(&self) -> BoxProviderFuture<'_, Result<Box<dyn Storage>>> {
        Box::pin(async move {
            Ok(Box::new(Domain {
                providers: Providers {
                    database: self.database.clone(),
                    objects: self.objects.clone(),
                    capabilities: self.capabilities,
                },
                namespace: uuid::Uuid::new_v4().to_string(),
                directory: tempfile::tempdir()?,
            }) as Box<dyn Storage>)
        })
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
            let provider = Arc::new(
                S3Objects::open(
                    store.clone(),
                    Path::from(format!("agent-host/{}", self.namespace)),
                    self.providers.capabilities,
                )
                .await?,
            );
            let bucket = provider
                .create_bucket(wire::CreateBucketRequest {
                    name: "host".into(),
                    mutation: None,
                })
                .await?
                .bucket
                .ok_or("host bucket identity missing")?;
            Ok(Objects {
                provider,
                bucket,
                capabilities: acyclic_fs::EmbeddedCapabilities { durable: true },
            })
        })
    }
}

pub async fn verify(
    database: &str,
    store: Arc<dyn ObjectStore>,
    capabilities: Capabilities,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn ObjectStore> = if std::env::var_os("STORAGE_IO_TRACE").is_some() {
        Arc::new(super::s3::observed_store::ObservedStore {
            inner: store,
            body_bytes: 0.into(),
            body_gets: 0.into(),
            metadata_bytes: 0.into(),
            head_updates: 0.into(),
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
        acyclic_agent_host_conformance::verify(&Providers {
            database,
            objects,
            capabilities,
        })
        .await
        .map_err(|error| error as Box<dyn std::error::Error>)?;
        println!(
            "Agent-host unchanged vectors over {name}: {:?}",
            started.elapsed()
        );
    }
    Ok(())
}
