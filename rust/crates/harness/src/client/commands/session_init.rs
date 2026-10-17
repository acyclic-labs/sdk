use super::*;

impl<P: StreamProvider + ?Sized> CommandSession<P> {
    // Only a followed generation advance permits retry. An existing seed is
    // authoritative; neither another writer's settings nor inbox are overwritten.
    // RETRY=false is the model's original fail-on-contention negative control.
    pub(super) async fn initialize_file<const RETRY: bool>(
        &mut self,
        key: files::FileKey,
        bytes: Vec<u8>,
    ) -> Result<()> {
        loop {
            if self.files.contains_key(&key) {
                return Ok(());
            }
            let before = self.generation;
            match self
                .publish_rebased::<RETRY, true>(key.clone(), bytes.clone(), true)
                .await
            {
                Err(Error::Conflict(_)) if RETRY && self.generation != before => {}
                result => return result,
            }
        }
    }
}
