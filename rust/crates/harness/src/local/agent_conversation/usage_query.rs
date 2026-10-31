//! Incremental usage on the original authenticated input/journal query.
use super::{Conversation, failed, unexpected};
use crate::{
    Error, Result,
    executor::{ModelCostEstimate, ModelUsageProjection, ModelUsageRecord, ModelUsageTotals},
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
        UsageQuery {
            conversation: Arc::clone(self),
            input: 0,
            end: 0,
            pending: None,
            totals: ModelUsageTotals::unpriced(),
            synchronized: false,
        }
    }
}

impl UsageQuery {
    /// Shared totals through the retained query frontier.
    #[must_use]
    pub const fn totals(&self) -> &ModelUsageTotals {
        &self.totals
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
        let reply = self
            .conversation
            .connection
            .execute(&AgentCommand::Usage {
                agent: self.conversation.agent().clone(),
                input: self.input,
                after: self.end,
                wait: self.synchronized,
            })
            .await?;
        let AgentReply::Usage {
            input,
            page,
            settled,
        } = reply
        else {
            return Err(unexpected());
        };
        let Some(mut page) = page else {
            // The shared boundary admits None only for a non-waiting inbox-tail
            // snapshot. The next request parks at this same original input.
            let changed = !self.synchronized;
            self.input = input;
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
        let changed = !page.items.is_empty() || synchronized != self.synchronized;
        // One fixed-size aggregate copy makes page admission atomic. All row
        // validation and checked arithmetic precede installation; no row or
        // original pending contribution is cloned for rollback.
        let mut totals = self.totals.clone();
        let mut pending = Pending::Existing;
        for (index, row) in page.items.iter().enumerate() {
            let projection = row.estimate(None).map_err(failed)?;
            if row.completed {
                match pending {
                    Pending::Existing => totals.replace(
                        self.pending.as_ref().ok_or_else(missing_admission)?,
                        &projection,
                    ),
                    Pending::Row(previous) => totals.replace(
                        &page.items[previous].estimate(None).map_err(failed)?,
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
        if finished {
            self.pending = None;
        } else {
            match pending {
                Pending::Existing => {}
                Pending::None => self.pending = None,
                Pending::Row(index) => {
                    self.pending = Some(unpriced(page.items.swap_remove(index))?);
                }
            }
        }
        self.totals = totals;
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
fn unpriced(row: ModelUsageRecord) -> Result<ModelUsageProjection<'static>> {
    let ModelUsageProjection {
        version,
        estimated_cost,
        ..
    } = row.estimate(None).map_err(failed)?;
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
