//! Reauthorization of bounded, confidential directory remainders.
use super::*;
use crate::conversation::PrivateDirectoryPage;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(in crate::filesystem) async fn authorize_directory_remainder(
        &self,
        request: DirectoryListing<'_>,
        page: &mut PrivateDirectoryPage,
        io: &PreparationIo,
    ) -> Result<()> {
        page.validate_with_io(Some(io))?;
        if request
            .generation
            .is_some_and(|expected| expected != &page.generation)
        {
            return Err(io.invalid("directory remainder differs from selected generation"));
        }
        let selection = self
            .select_directory(
                &DirectoryListing {
                    generation: Some(&page.generation),
                    ..request
                },
                io,
                &mut || std::future::ready(Ok(())),
            )
            .await?;
        let now = super::super::super::access::access_now()?;
        let mut failure = None;
        page.entries.retain(|entry| {
            if failure.is_some() {
                return false;
            }
            let allowed = (|| {
                let path = crate::executor::with_history_io(Some(io), |work| {
                    crate::executor::preparation_format(
                        format_args!(
                            "{}{}{}",
                            request.path,
                            if request.path.is_empty() { "" } else { "/" },
                            entry.name
                        ),
                        work,
                    )
                })?;
                let _hold = io.guard_owned(path.capacity() as u64)?;
                Ok::<_, Error>(selection.lists(&path, now))
            })();
            match allowed {
                Ok(allowed) => allowed,
                Err(error) => {
                    failure = Some(error);
                    false
                }
            }
        });
        if let Some(error) = failure {
            return Err(error);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(cut) = &selection.snapshot {
            self.validate_private_snapshot(cut, io).await?;
        }
        selection.require_current(io).await?;
        request.grant.recheck(Some(io))
    }
}
