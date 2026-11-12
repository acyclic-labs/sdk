//! Restore current actors before following the captured activation cut.
use super::*;
use acyclic_stream::ChildrenPageRequest;

impl<P: StreamProvider + ?Sized> Runtime<P> {
    pub(super) async fn restore(&self) -> Result<(), Error> {
        let mut after = None;
        loop {
            let page = self
                .store
                .provider()
                .children_page(ChildrenPageRequest {
                    parent: Some(self.store.root().clone()),
                    after: after.clone(),
                    // This is live discovery, not a hierarchy snapshot. The
                    // captured activation follow covers concurrent creations,
                    // including names inserted before this page's cursor.
                    hierarchy_version: None,
                    limit: 256,
                    tail_stream: None,
                })
                .await?;
            for child in page.children {
                if let Ok(actor) = self.store.actor(&child.path)
                    && self.claims(&actor)
                {
                    self.track(actor)?;
                }
            }
            match page.next_after {
                Some(next) => {
                    after = Some(next);
                }
                None => return Ok(()),
            }
        }
    }
}
