//! Checked native checkout staging and cleanup paths.
use acyclic_fs::OperationId;
use base64::Engine as _;
use std::path::{Path, PathBuf};
/// A physical directory used for checked cleanup and checkout publication staging.
#[derive(Clone, Copy)]
pub(crate) struct NativeDirectory<'a>(pub &'a Path);

impl NativeDirectory<'_> {
    /// Constructs the staging path on the caller's original native work ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn publication_directory_with_native_work(
        &self,
        operation_id: OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> crate::Result<(PathBuf, acyclic_fs::NativeAllocation)> {
        let root = self.0;
        let borrowed = root.as_os_str().to_str();
        // A tuple drops formatted bytes before their original allocation custody,
        // including every early hash or path-construction refusal.
        let lossy = match borrowed {
            Some(_) => None,
            None => Some(work.display(&root.display())?),
        };
        let text = match borrowed {
            Some(text) => text,
            None => {
                &lossy
                    .as_ref()
                    .ok_or(crate::Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Storage,
                        reason: "root staging path has no admitted display image",
                    })?
                    .0
            }
        };
        let hashing = acyclic_fs::WorkCounters {
            bytes_hashed: text.len() as u64,
            ..acyclic_fs::WorkCounters::UNCHARGED
        };
        work.admit(hashing)?;
        let digest = blake3::hash(text.as_bytes());
        work.spent(hashing)?;
        drop(lossy);
        // `agents` is reserved by the mount table. A caller-chosen volume
        // alias must never select or conceal this recovery namespace.
        const PREFIX: &[u8] = crate::workspace::namespace::CHECKOUT_RECOVERY_PREFIX.as_bytes();
        let encoding = acyclic_fs::WorkCounters {
            bytes_encoded: 44,
            bytes_copied: (PREFIX.len() + 56) as u64,
            ..acyclic_fs::WorkCounters::UNCHARGED
        };
        work.admit(encoding)?;
        let leaf = root_materialization_leaf(&digest, operation_id).map_err(|_| {
            crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage,
                reason: "root staging identifier encoding exceeded its fixed width",
            }
        })?;
        let mut relative = [0; PREFIX.len() + 56];
        relative[..PREFIX.len()].copy_from_slice(PREFIX);
        relative[PREFIX.len()..].copy_from_slice(&leaf);
        work.spent(encoding)?;
        let relative =
            std::str::from_utf8(&relative).map_err(|_| crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage,
                reason: "root staging identifier encoding is not UTF-8",
            })?;
        Ok(work.path(root, Some(Path::new(relative)))?)
    }
}

fn root_materialization_leaf(
    digest: &blake3::Hash,
    operation_id: OperationId,
) -> Result<[u8; 56], base64::EncodeSliceError> {
    // Each operation retains its identity within the excluded staging namespace.
    let mut leaf = [std::path::MAIN_SEPARATOR as u8; 56];
    leaf[..10].copy_from_slice(b".acyclic-m");
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode_slice(&digest.as_bytes()[..16], &mut leaf[11..33])?;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode_slice(operation_id.into_bytes(), &mut leaf[34..])?;
    Ok(leaf)
}
