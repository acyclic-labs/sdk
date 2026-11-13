//! Incremental usage on the original authenticated input/journal query.
use crate::{
    AgentCommand, AgentControls, AgentExchangeError, AgentPath, AgentReply, PreparedCommand,
};
use acyclic_harness_core::{
    Error, Result,
    executor::{
        ContextUsage, ModelCostEstimate, ModelPrices, ModelPurpose, ModelUsageProjection,
        ModelUsageRecord, ModelUsageTotals, ToolUsage, UsageTotal, WaitUsage,
    },
};
use std::{borrow::Cow, collections::BTreeMap, sync::Arc};

/// One selected conversation's live usage frontier. Storage is independent of
/// history length: shared totals, one summary per admitted provider and one
/// pending call per model purpose.
/// No work occurs on redraw, and no transcript or child tree is traversed.
pub struct UsageQuery {
    agent: AgentPath,
    input: u64,
    end: u64,
    pending: [Option<ModelUsageProjection<'static>>; 2],
    totals: ModelUsageTotals,
    providers: BTreeMap<String, ModelUsageTotals>,
    model_time_ns: UsageTotal,
    tools: ToolUsage,
    waits: WaitUsage,
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

/// An authenticated command failure or an invalid original contribution.
#[derive(Debug, thiserror::Error)]
pub enum UsageQueryError {
    /// The original session or command failure.
    #[error(transparent)]
    Exchange(#[from] AgentExchangeError),
    /// The original projection failure; the previous frontier remains intact.
    #[error(transparent)]
    Projection(#[from] Error),
}

impl UsageQuery {
    /// Start a bounded live projection for one selected agent and price file.
    pub fn new(agent: AgentPath, prices: Arc<ModelPrices>) -> Self {
        Self {
            agent,
            input: 0,
            end: 0,
            pending: [None, None],
            totals: prices.totals(),
            providers: BTreeMap::new(),
            model_time_ns: UsageTotal::default(),
            tools: ToolUsage::default(),
            waits: WaitUsage::default(),
            context: None,
            synchronized: false,
            closed: false,
            model: None,
            open_children: None,
            prices,
        }
    }

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

    /// Original wait starts, finishes and measured elapsed time at this frontier.
    pub const fn waits(&self) -> WaitUsage {
        self.waits
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
    pub async fn advance(
        &mut self,
        controls: &AgentControls,
    ) -> std::result::Result<bool, UsageQueryError> {
        if self.closed {
            return futures::future::pending().await;
        }
        let command = PreparedCommand::new(&AgentCommand::Usage {
            agent: self.agent.clone(),
            input: self.input,
            after: self.end,
            wait: self.synchronized,
            observed_children: self.open_children,
        })
        .map_err(AgentExchangeError::from)?;
        let invocation = controls
            .prepare(command)
            .map_err(AgentExchangeError::from)?;
        let reply = controls.execute(&invocation).await?;
        let AgentReply::Usage {
            input,
            page,
            settled,
            open_children,
        } = reply
        else {
            return Err(Error::Conflict("the endpoint answered another command".into()).into());
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
            || !page.waits.is_empty()
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
        let waits = if page.waits.is_empty() {
            None
        } else {
            Some(self.waits.merged(page.waits)?)
        };
        if !page.items.is_empty() {
            // Stage the summaries before installing this page's contributions.
            // Empty pages advance only the frontier, without copying summaries.
            let mut totals = self.totals.clone();
            let mut providers = self.providers.clone();
            let mut model_time_ns = self.model_time_ns;
            let mut pending = [Pending::Existing; 2];
            for (index, row) in page.items.iter().enumerate() {
                let purpose = purpose_slot(row.purpose)?;
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
                        Pending::None => return Err(missing_admission().into()),
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
                    let purpose = purpose_slot(row.purpose)?;
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
        if let Some(waits) = waits {
            self.waits = waits;
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

/// Stable slot for the two original model purposes.
pub fn purpose_slot(purpose: ModelPurpose) -> Result<usize> {
    match purpose {
        ModelPurpose::Response => Ok(0),
        ModelPurpose::Summary | ModelPurpose::SummaryPart(_) => Ok(1),
        ModelPurpose::Count => Err(Error::Storage(
            "token admission cannot carry generation usage".into(),
        )),
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

fn failed(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
