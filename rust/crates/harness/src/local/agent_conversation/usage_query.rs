//! Incremental usage on the original authenticated input/journal query.
use super::{Conversation, failed, unexpected};
use crate::{
    Error, Result,
    executor::{
        ContextUsage, ModelCostEstimate, ModelPrices, ModelPurpose, ModelUsageProjection,
        ModelUsageRecord, ModelUsageTotals, ToolUsage, UsageTotal,
    },
};
use acyclic_harness_client::{AgentCommand, AgentReply};
use std::{borrow::Cow, collections::BTreeMap, sync::Arc};

/// One selected conversation's live usage frontier. Storage is independent of
/// history length: shared totals, one summary per admitted provider and one
/// pending call per model purpose.
/// No work occurs on redraw, and no transcript or child tree is traversed.
pub struct UsageQuery {
    pub(super) conversation: Arc<Conversation>,
    input: u64,
    end: u64,
    pending: [Option<ModelUsageProjection<'static>>; 2],
    totals: ModelUsageTotals,
    providers: BTreeMap<String, ModelUsageTotals>,
    model_time_ns: UsageTotal,
    tools: ToolUsage,
    context: Option<ContextUsage>,
    model: Option<(String, String)>,
    open_children: Option<u64>,
    prices: Arc<ModelPrices>,
    synchronized: bool,
    closed: bool,
}

/// Where the current page's last uncompleted contribution came from. Keeping
/// its row index moves the original row only after the whole page is admitted.
#[derive(Clone, Copy)]
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
            pending: [None, None],
            totals,
            providers: BTreeMap::new(),
            model_time_ns: UsageTotal::default(),
            tools: ToolUsage::default(),
            context: None,
            synchronized: false,
            closed: false,
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

    /// Same original contributions grouped by admitted provider, without a
    /// second source read or retained call history. Counters keep their original
    /// provider conventions; no cache fraction is inferred from unknown units.
    pub fn providers(&self) -> &BTreeMap<String, ModelUsageTotals> {
        &self.providers
    }

    /// Sum of original monotonic completed-call durations. Unknown terminal
    /// durations remain unknown; unfinished calls are not elapsed-time samples.
    pub const fn model_time_ns(&self) -> UsageTotal {
        self.model_time_ns
    }

    /// Original tool facts from the same bounded source pages as model usage.
    pub fn tools(&self) -> &ToolUsage {
        &self.tools
    }

    /// Latest original verified response occupancy, independent of cumulative usage.
    #[must_use]
    pub const fn context(&self) -> Option<ContextUsage> {
        self.context
    }

    /// Direct open-child count from the same actor cut that fences this page.
    pub const fn open_children(&self) -> Option<u64> {
        self.open_children
    }

    /// Latest response-model admission.
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
        let Some(page) = page else {
            // A settled inbox tail is the producer's terminal frontier.
            self.closed = settled;
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
            || !page.tools.is_empty()
            || page.context.is_some()
            || self.open_children != Some(open_children)
            || synchronized != self.synchronized;
        let model = page
            .items
            .iter()
            .rev()
            .find(|row| row.purpose == ModelPurpose::Response)
            .map(|row| (row.provider.clone(), row.model.clone()));
        let tools = if page.tools.is_empty() {
            None
        } else {
            Some(self.tools.merged(&page.tools)?)
        };
        if !page.items.is_empty() {
            // Stage the summaries before installing this page's contributions.
            // Empty pages advance only the frontier, without copying summaries.
            let mut totals = self.totals.clone();
            let mut providers = self.providers.clone();
            let mut model_time_ns = self.model_time_ns;
            let mut pending = [Pending::Existing; 2];
            for (index, row) in page.items.iter().enumerate() {
                let purpose = purpose_slot(row.purpose);
                let projection = row.estimate(self.prices.price(row)).map_err(failed)?;
                let provider = providers
                    .entry(row.provider.clone())
                    .or_insert_with(|| self.prices.totals());
                if row.completed {
                    let previous = match pending[purpose] {
                        Pending::Existing => self.pending[purpose]
                            .as_ref()
                            .ok_or_else(missing_admission)?,
                        Pending::Row(previous) => &page.items[previous]
                            .estimate(self.prices.price(&page.items[previous]))
                            .map_err(failed)?,
                        Pending::None => return Err(missing_admission()),
                    };
                    totals.replace(previous, &projection).map_err(failed)?;
                    provider.replace(previous, &projection).map_err(failed)?;
                    model_time_ns.push(row.latency_ns).map_err(failed)?;
                    pending[purpose] = Pending::None;
                } else {
                    // A stopped producer has no terminal usage contribution.
                    // Its unknown counters already remain in the aggregate;
                    // the next admission takes this role's live slot.
                    totals.push(&projection).map_err(failed)?;
                    provider.push(&projection).map_err(failed)?;
                    pending[purpose] = Pending::Row(index);
                }
            }
            if !finished {
                let mut admitted = [None, None];
                for (index, row) in page.items.into_iter().enumerate() {
                    let purpose = purpose_slot(row.purpose);
                    if matches!(pending[purpose], Pending::Row(selected) if selected == index) {
                        admitted[purpose] = Some(owned(row, &self.prices)?);
                    }
                }
                for (purpose, state) in pending.into_iter().enumerate() {
                    match state {
                        Pending::Existing => {}
                        Pending::None => self.pending[purpose] = None,
                        Pending::Row(_) => self.pending[purpose] = admitted[purpose].take(),
                    }
                }
            }
            self.totals = totals;
            self.providers = providers;
            self.model_time_ns = model_time_ns;
        }
        if finished {
            self.pending = [None, None];
        }
        if let Some(tools) = tools {
            self.tools = tools;
        }
        if let Some(context) = page.context {
            self.context = Some(context);
        }
        self.open_children = Some(open_children);
        if let Some(model) = model {
            self.model = Some(model);
        }
        self.input = next_input;
        self.end = if finished { 0 } else { page.end };
        self.synchronized = synchronized;
        Ok(changed)
    }
}

pub(super) fn purpose_slot(purpose: ModelPurpose) -> usize {
    match purpose {
        ModelPurpose::Response => 0,
        ModelPurpose::Summary => 1,
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
