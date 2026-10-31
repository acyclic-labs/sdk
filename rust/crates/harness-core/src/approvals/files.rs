//! Storage contract over paths relative to one agent's private volume.
use crate::Result;
use acyclic_stream::BoxProviderFuture;

/// Result of a durable, atomic create-if-absent operation.
#[derive(Debug, PartialEq, Eq)]
pub enum ApprovalCreate {
    /// This caller created the file and its exact bytes are durable.
    Created,
    /// Another caller already created the file; these exact bytes are durable.
    Existing(Vec<u8>),
}

/// Agent-volume storage supplied by the authenticated journal/host owner.
/// Implementations enforce the bound before allocating or reading file contents.
/// Paths are relative to the same agent volume; no listing or polling is needed.
/// These operations confer no decision authority and never wake an agent.
pub trait ApprovalFiles: acyclic_stream::ProviderPlatform {
    /// Read durable bytes only, rejecting a larger file before loading it.
    /// An accepted but uncertified file cannot advance the policy chain.
    fn read<'a>(
        &'a self,
        path: &'a str,
        maximum_bytes: u64,
    ) -> BoxProviderFuture<'a, Result<Option<Vec<u8>>>>;

    /// Atomically create only if absent and wait for durability before returning
    /// either result. An existing file must never be replaced, even if identical.
    fn create<'a>(
        &'a self,
        path: &'a str,
        bytes: Vec<u8>,
        maximum_bytes: u64,
    ) -> BoxProviderFuture<'a, Result<ApprovalCreate>>;
    /// Reads through the same authenticated owner and original operation ledger.
    fn read_with_io<'a>(
        &'a self,
        path: &'a str,
        maximum_bytes: u64,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<acyclic_stream::OwnedBytes>>>
    {
        if io.is_some() {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "original-work approval reads are unavailable",
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            Ok(self.read(path, maximum_bytes).await?.map(Into::into))
        })
    }

    /// Publishes the same immutable file, without copying the caller's bytes.
    /// `None` means created; an existing file returns its original owned bytes.
    fn create_with_io<'a>(
        &'a self,
        path: &'a str,
        bytes: &'a [u8],
        maximum_bytes: u64,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<acyclic_stream::OwnedBytes>>>
    {
        if io.is_some() {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "original-work approval publication is unavailable",
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            Ok(
                match self.create(path, bytes.to_vec(), maximum_bytes).await? {
                    ApprovalCreate::Created => None,
                    ApprovalCreate::Existing(bytes) => Some(bytes.into()),
                },
            )
        })
    }
}

/// Borrowed original request bytes, with a canonical single-component identity.
/// The owner supplies this identity from its journal; it is not derived anew
/// from the request's operation id, so replay keeps the original request path.
#[derive(Clone, Copy)]
pub struct ApprovalRequest<'a> {
    id: &'a str,
    bytes: &'a [u8],
    maximum_bytes: u64,
}

impl<'a> ApprovalRequest<'a> {
    /// Checks the file bound and prevents aliases outside the request directory.
    pub fn new(id: &'a str, bytes: &'a [u8], maximum_bytes: u64) -> Result<Self> {
        Self::new_with_io(id, bytes, maximum_bytes, None)
    }

    pub(super) fn new_with_io(
        id: &'a str,
        bytes: &'a [u8],
        maximum_bytes: u64,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Self> {
        check_bound_with_io(bytes.len(), maximum_bytes, io)?;
        if id.is_empty()
            || id.len() > 255
            || id == "."
            || id == ".."
            || !id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
        {
            return Err(super::io::error(
                io,
                crate::wire::ErrorCode::Invalid,
                "approval request identity is not canonical",
            ));
        }
        Ok(Self {
            id,
            bytes,
            maximum_bytes,
        })
    }
    /// Identity already assigned by the request's journal owner.
    pub fn id(&self) -> &str {
        self.id
    }
    /// Exact immutable request bytes; fingerprints always cover these bytes.
    pub fn bytes(&self) -> &[u8] {
        self.bytes
    }
    /// Finite bound used for every review and dispatch file for this request.
    pub fn maximum_bytes(&self) -> u64 {
        self.maximum_bytes
    }
    /// Canonical path of a principal's retained review or model dispatch.
    pub fn review_path(&self, principal: &str) -> String {
        format!(
            "approvals/{}/reviews/{}.json",
            self.id,
            crate::interaction::ApprovalDecisionFile::principal_file(principal)
        )
    }

    pub(super) fn review_path_with_io(
        &self,
        principal: &str,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<String> {
        super::io::text(
            format_args!(
                "approvals/{}/reviews/{}.json",
                self.id,
                super::io::principal_file(principal, io)?
            ),
            io,
        )
    }
}

pub(super) fn check_bound(length: usize, maximum_bytes: u64) -> Result<()> {
    check_bound_with_io(length, maximum_bytes, None)
}

pub(super) fn check_bound_with_io(
    length: usize,
    maximum_bytes: u64,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    if maximum_bytes == 0 || maximum_bytes == u64::MAX || length as u64 > maximum_bytes {
        return Err(super::io::error(
            io,
            crate::wire::ErrorCode::Invalid,
            "approval file exceeds its finite byte bound",
        ));
    }
    Ok(())
}

pub(super) async fn create_exact(
    files: &dyn ApprovalFiles,
    path: &str,
    bytes: Vec<u8>,
    maximum_bytes: u64,
) -> Result<bool> {
    create_exact_with_io(files, path, &bytes, maximum_bytes, None).await
}

pub(super) async fn create_exact_with_io(
    files: &dyn ApprovalFiles,
    path: &str,
    bytes: &[u8],
    maximum_bytes: u64,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<bool> {
    check_bound_with_io(bytes.len(), maximum_bytes, io)?;
    match files
        .create_with_io(path, bytes, maximum_bytes, io)?
        .await?
    {
        None => Ok(true),
        Some(existing) => {
            check_bound_with_io(existing.len(), maximum_bytes, io)?;
            if existing.as_ref() != bytes {
                return Err(super::io::error(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "approval file binding changed",
                ));
            }
            Ok(false)
        }
    }
}
