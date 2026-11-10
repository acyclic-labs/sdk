//! Incremental usage on the original authenticated input/journal query.
use super::{Conversation, failed, unexpected};
use crate::{
    Error, Result,
    executor::{
        ContextUsage, ModelCostEstimate, ModelPrices, ModelPurpose, ModelUsageProjection,
        ModelUsageRecord, ModelUsageTotals,
    },
};
use acyclic_harness_client::{AgentCommand, AgentReply};
use std::{borrow::Cow, sync::Arc};

/// One selected conversation's live usage frontier. Storage is independent of
/// history length: the shared fixed-size totals and at most one pending call.
/// No work occurs on redraw, and no transcript or child tree is traversed.
pub struct UsageQuery {
    conversation: Arc<Conversation>,
    input: u64,
    end: u64,
    pending: Option<ModelUsageProjection<'static>>,
    totals: ModelUsageTotals,
    synchronized: bool,
    closed: bool,
    context: Option<ContextUsage>,
    model: Option<(String, String)>,
    open_children: Option<u64>,
    prices: Arc<ModelPrices>,
}

/// Where the current page's last uncompleted contribution came from. Keeping
/// its row index moves the original row only after the whole page is admitted.
enum Pending {
    Existing,
    Row(usize),
    None,
}

impl Conversation {
    /// Start an unpriced query on this exact connection. No currency or price
    /// is inferred from a model, catalog, account or incoming row.
    #[must_use]
    pub fn usage_query(self: &Arc<Self>) -> UsageQuery {
        self.usage_projection(Arc::default(), ModelUsageTotals::unpriced())
    }

    /// Start a fresh projection against one admitted immutable price file.
    /// Repricing is an explicit new query; no price lookup or timer is added
    /// to the original journal follow, and original usage remains unchanged.
    pub fn usage_query_with_prices(self: &Arc<Self>, prices: Arc<ModelPrices>) -> UsageQuery {
        let totals = prices.totals();
        self.usage_projection(prices, totals)
    }

    fn usage_projection(
        self: &Arc<Self>,
        prices: Arc<ModelPrices>,
        totals: ModelUsageTotals,
    ) -> UsageQuery {
        UsageQuery {
            conversation: Arc::clone(self),
            input: 0,
            end: 0,
            pending: None,
            totals,
            synchronized: false,
            closed: false,
            context: None,
            model: None,
            open_children: None,
            prices,
        }
    }
}

impl UsageQuery {
    /// Shared totals through the retained query frontier.
    #[must_use]
    pub const fn totals(&self) -> &ModelUsageTotals {
        &self.totals
    }

    /// Latest original prepared-context accounting at this query's frontier.
    #[must_use]
    pub const fn context(&self) -> Option<ContextUsage> {
        self.context
    }

    /// Direct open-child count from the same actor cut that fences this page.
    #[must_use]
    pub const fn open_children(&self) -> Option<u64> {
        self.open_children
    }

    /// Latest response-model admission, independent of a provider response label.
    #[must_use]
    pub fn model(&self) -> Option<(&str, &str)> {
        self.model
            .as_ref()
            .map(|(provider, model)| (provider.as_str(), model.as_str()))
    }

    /// The initial bounded replay has reached a quiet live input or inbox tail.
    /// A footer must distinguish loading from the selected observed total.
    #[must_use]
    pub const fn synchronized(&self) -> bool {
        self.synchronized
    }

    /// Admit one bounded page. Once caught up, the original Usage command waits
    /// on its journal/inbox/actor continuation when quiet; no probe or timer is
    /// scheduled here. Returns whether the visible projection changed.
    ///
    /// # Errors
    /// Original session/source failure, invalid contribution or exhausted input
    /// cursor. A failure preserves the prior totals, pending call and frontier.
    pub async fn advance(&mut self) -> Result<bool> {
        if self.closed {
            return futures::future::pending().await;
        }
        let reply = self
            .conversation
            .connection
            .execute(&AgentCommand::Usage {
                agent: self.conversation.agent().clone(),
                input: self.input,
                after: self.end,
                wait: self.synchronized,
                observed_children: self.open_children,
            })
            .await?;
        let AgentReply::Usage {
            input,
            page,
            settled,
            open_children,
        } = reply
        else {
            return Err(unexpected());
        };
        let Some(mut page) = page else {
            self.closed = settled;
            // An idle tail still carries the original actor's metadata. The
            // next request parks on both inbox and actor continuations.
            let changed = !self.synchronized || self.open_children != Some(open_children);
            self.input = input;
            self.open_children = Some(open_children);
            self.synchronized = true;
            return Ok(changed);
        };
        let finished = settled && page.next.is_none();
        let next_input = if finished {
            input
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("usage input continuation overflows".into()))?
        } else {
            input
        };
        let synchronized = self.synchronized || (!settled && page.next.is_none());
        let changed = !page.items.is_empty()
            || page.context.is_some()
            || self.open_children != Some(open_children)
            || synchronized != self.synchronized;
        let model = page
            .items
            .iter()
            .rev()
            .find(|row| row.purpose == ModelPurpose::Response)
            .map(|row| (row.provider.clone(), row.model.clone()));
        if !page.items.is_empty() {
            // One fixed-size copy makes contribution admission atomic. Empty
            // pages advance only their frontier, without copying any totals.
            let mut totals = self.totals.clone();
            let mut pending = Pending::Existing;
            for (index, row) in page.items.iter().enumerate() {
                let projection = row.estimate(self.prices.price(row)).map_err(failed)?;
                if row.completed {
                    match pending {
                        Pending::Existing => totals.replace(
                            self.pending.as_ref().ok_or_else(missing_admission)?,
                            &projection,
                        ),
                        Pending::Row(previous) => totals.replace(
                            &page.items[previous]
                                .estimate(self.prices.price(&page.items[previous]))
                                .map_err(failed)?,
                            &projection,
                        ),
                        Pending::None => return Err(missing_admission()),
                    }
                    .map_err(failed)?;
                    pending = Pending::None;
                } else {
                    totals.push(&projection).map_err(failed)?;
                    pending = Pending::Row(index);
                }
            }
            if !finished {
                match pending {
                    Pending::Existing => {}
                    Pending::None => self.pending = None,
                    Pending::Row(index) => {
                        self.pending = Some(owned(page.items.swap_remove(index), &self.prices)?);
                    }
                }
            }
            self.totals = totals;
        }
        if finished {
            self.pending = None;
        }
        self.open_children = Some(open_children);
        if let Some(context) = page.context {
            self.context = Some(context);
        }
        if let Some(model) = model {
            self.model = Some(model);
        }
        self.input = next_input;
        self.end = if finished { 0 } else { page.end };
        self.synchronized = synchronized;
        Ok(changed)
    }
}

fn missing_admission() -> Error {
    Error::Storage("usage completion has no original admitted contribution".into())
}

/// Transfer the original row into its shared projection without cloning the
/// row. Only the shared estimator defines values and provenance. With no price
/// selected, neither optional provenance string allocates.
fn owned(row: ModelUsageRecord, prices: &ModelPrices) -> Result<ModelUsageProjection<'static>> {
    let ModelUsageProjection {
        version,
        estimated_cost,
        ..
    } = row.estimate(prices.price(&row)).map_err(failed)?;
    let ModelCostEstimate {
        price_revision,
        effective_at,
        currency,
        token_nano_units,
    } = estimated_cost;
    let estimated_cost = ModelCostEstimate {
        price_revision: price_revision.map(|value| Cow::Owned(value.into_owned())),
        effective_at,
        currency: currency.map(|value| Cow::Owned(value.into_owned())),
        token_nano_units,
    };
    Ok(ModelUsageProjection {
        version,
        usage: Cow::Owned(row),
        estimated_cost,
    })
}
