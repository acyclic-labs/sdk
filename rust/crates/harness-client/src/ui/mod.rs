//! Client adapters over the shared pure UI projection.
mod attention;
mod cut;
mod reply;
pub use acyclic_harness_core::ui::{
    Affected, Children, Cut, Error, Node, Page, Result, UiPath, View, blocks,
};
pub use attention::{attention, attention_wants};
pub use cut::ReaderCut;
pub use reply::{Reply, ReplyPayload, approval_reply};

#[cfg(test)]
mod tests;
