//! Native connection ownership around the shared client projection.
use super::Conversation;
use crate::{Error, Result, executor::ModelPrices};
use acyclic_harness_client::UsageQueryError;
pub(super) use acyclic_harness_client::usage::purpose_slot;
use std::{ops::Deref, sync::Arc};

/// One selected conversation's original authenticated usage projection.
pub struct UsageQuery {
    pub(super) conversation: Arc<Conversation>,
    query: acyclic_harness_client::UsageQuery,
}

impl Conversation {
    /// Start an unpriced query on this exact connection.
    pub fn usage_query(self: &Arc<Self>) -> UsageQuery {
        self.usage_query_with_prices(Arc::default())
    }

    /// Start a fresh projection against one admitted immutable price file.
    pub fn usage_query_with_prices(self: &Arc<Self>, prices: Arc<ModelPrices>) -> UsageQuery {
        UsageQuery {
            conversation: Arc::clone(self),
            query: acyclic_harness_client::UsageQuery::new(self.agent().clone(), prices),
        }
    }
}

impl Deref for UsageQuery {
    type Target = acyclic_harness_client::UsageQuery;
    fn deref(&self) -> &Self::Target {
        &self.query
    }
}

impl UsageQuery {
    /// Admit one bounded page, waiting on the original continuation when quiet.
    pub async fn advance(&mut self) -> Result<bool> {
        let connection = self.conversation.connection();
        self.query
            .advance(&connection.controls)
            .await
            .map_err(|error| match error {
                UsageQueryError::Projection(error) => error,
                UsageQueryError::Exchange(error) if error.code() == "unavailable" => {
                    connection.stream_failure(error)
                }
                UsageQueryError::Exchange(error) => {
                    Error::Storage(format!("usage command: {error}"))
                }
            })
    }
}
