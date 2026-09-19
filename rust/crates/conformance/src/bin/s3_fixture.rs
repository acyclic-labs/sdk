//! Local signed S3 consumer over public filesystem APIs.

use acyclic_fs::{
    FilesystemS3Adapter, FilesystemS3Authentication, FilesystemS3Limits, FilesystemS3Principal,
    FilesystemS3Resolver, Fs, IdempotencyKey, LocalAuthorityBackend, LocalFs, LocalObjectBackend,
    LocalOptions, S3ListOptions, Workspace, WorkspaceDelete,
};
use async_trait::async_trait;
use axum::{error_handling::HandleError, http::StatusCode};
use s3s::{S3Result, auth::SecretKey, service::S3ServiceBuilder};
use std::{collections::BTreeMap, io::Write, path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

type Principal = FilesystemS3Principal<LocalAuthorityBackend, LocalObjectBackend>;
type LocalWorkspace = Workspace<LocalAuthorityBackend, LocalObjectBackend>;
type BucketMap = BTreeMap<String, (String, LocalWorkspace)>;

struct Resolver {
    filesystem: LocalFs,
    buckets: Mutex<BucketMap>,
}

fn secret(access_key: &str) -> Option<SecretKey> {
    match access_key {
        "access" => Some(SecretKey::from("secret")),
        "alt" => Some(SecretKey::from("alt-secret")),
        "tenant" => Some(SecretKey::from("tenant-secret")),
        _ => None,
    }
}

#[async_trait]
impl FilesystemS3Resolver<LocalAuthorityBackend, LocalObjectBackend> for Resolver {
    async fn resolve_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        secret(access_key).ok_or_else(|| s3s::s3_error!(InvalidAccessKeyId))
    }

    async fn resolve_principal(
        &self,
        access_key: &str,
        _session_token: Option<&str>,
        bucket: &str,
    ) -> S3Result<Principal> {
        let buckets = self.buckets.lock().await;
        let Some((owner, workspace)) = buckets.get(bucket) else {
            return Err(s3s::s3_error!(NoSuchBucket));
        };
        if owner != access_key {
            return Err(s3s::s3_error!(AccessDenied));
        }
        Ok(Principal {
            secret_key: secret(access_key).ok_or_else(|| s3s::s3_error!(InvalidAccessKeyId))?,
            bucket: bucket.to_owned(),
            workspace: workspace.clone(),
            generation: None,
            writable: true,
        })
    }

    async fn create_bucket(
        &self,
        access_key: &str,
        _session_token: Option<&str>,
        bucket: &str,
    ) -> S3Result<()> {
        let mut buckets = self.buckets.lock().await;
        if let Some((owner, _)) = buckets.get(bucket) {
            return if owner == access_key {
                Ok(())
            } else {
                Err(s3s::s3_error!(BucketAlreadyExists))
            };
        }
        let workspace = self
            .filesystem
            .create_workspace(format!(
                "s3-{}",
                hex::encode(IdempotencyKey::new().into_bytes())
            ))
            .await
            .map_err(|_| s3s::s3_error!(InternalError))?;
        buckets.insert(bucket.to_owned(), (access_key.to_owned(), workspace));
        Ok(())
    }

    async fn list_buckets(
        &self,
        access_key: &str,
        _session_token: Option<&str>,
    ) -> S3Result<Vec<String>> {
        Ok(self
            .buckets
            .lock()
            .await
            .iter()
            .filter(|(_, (owner, _))| owner == access_key)
            .map(|(name, _)| name.clone())
            .collect())
    }

    async fn delete_bucket(
        &self,
        access_key: &str,
        _session_token: Option<&str>,
        bucket: &str,
    ) -> S3Result<()> {
        let mut buckets = self.buckets.lock().await;
        let Some((owner, workspace)) = buckets.get(bucket) else {
            return Err(s3s::s3_error!(NoSuchBucket));
        };
        if owner != access_key {
            return Err(s3s::s3_error!(AccessDenied));
        }
        let contents = workspace
            .s3()
            .list_objects(S3ListOptions {
                maximum_keys: 1,
                ..S3ListOptions::default()
            })
            .await
            .map_err(|_| s3s::s3_error!(InternalError))?;
        if !contents.objects.is_empty() || !contents.common_prefixes.is_empty() {
            return Err(s3s::s3_error!(BucketNotEmpty));
        }
        match workspace
            .delete(IdempotencyKey::new())
            .await
            .map_err(|_| s3s::s3_error!(InternalError))?
        {
            WorkspaceDelete::Deleted | WorkspaceDelete::AlreadyDeleted => {
                buckets.remove(bucket);
                Ok(())
            }
            WorkspaceDelete::Conflict | WorkspaceDelete::IdempotencyConflict => {
                Err(s3s::s3_error!(OperationAborted))
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(std::env::args_os().nth(1).ok_or("usage: s3-fixture ROOT")?);
    let filesystem = Fs::local(LocalOptions::new(root)).await?;
    let workspace = filesystem.create_workspace("s3-fixture-initial").await?;
    let mut buckets = BTreeMap::new();
    buckets.insert("bucket".to_owned(), ("access".to_owned(), workspace));
    let resolver = Arc::new(Resolver {
        filesystem,
        buckets: Mutex::new(buckets),
    });
    let adapter = FilesystemS3Adapter::new(Arc::clone(&resolver), FilesystemS3Limits::default())?;
    let mut builder = S3ServiceBuilder::new(adapter);
    builder.set_auth(FilesystemS3Authentication::new(resolver));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    println!(
        "{{\"schema\":1,\"endpoint\":\"http://{address}\",\"bucket\":\"bucket\",\"access_key\":\"access\",\"secret_key\":\"secret\"}}"
    );
    std::io::stdout().flush()?;
    let service = HandleError::new(builder.build(), |error: s3s::HttpError| async move {
        eprintln!("S3 HTTP error: {error:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    });
    axum::serve(listener, axum::Router::new().fallback_service(service))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
