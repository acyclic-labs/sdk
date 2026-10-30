use crate::{Metrics, Query, QueryError, Slo, SloError, Snapshot, Verdict};

/// A selected SLO file and its live operation evidence. The caller retains the
/// authorized file reference and follows replacements through its volume query.
/// This projection has no persistence, background task or transport of its own.
pub struct SloQuery {
    slo: Slo,
    query: Query,
}

/// Serializable live view of the selected SLO file.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct SloView {
    /// Validated contents of the ordinary volume file.
    pub slo: Slo,
    /// Bounded cumulative evidence from the selected operation.
    pub evidence: Snapshot,
    /// Unknown when the evidence cannot decide the objective.
    pub verdict: Verdict,
}

impl Metrics {
    /// Project a decoded SLO file onto one live series. Read the file through the
    /// existing authorized volume reader; no separate registration is required.
    pub fn query_slo(&self, slo: Slo) -> Result<SloQuery, SloError> {
        slo.validate()?;
        let query = match &slo.rpc {
            Some(rpc) => self.query_rpc(&slo.operation, rpc),
            None => self.query(&slo.operation),
        };
        Ok(SloQuery { slo, query })
    }
}

impl SloQuery {
    /// Current file projection. On a volume-query replacement, construct the
    /// replacement query from those exact file bytes; invalid files are errors.
    pub fn snapshot(&self) -> SloView {
        self.view(self.query.snapshot())
    }

    /// Wait for this file's selected operation, or the producer's close event.
    /// The owner selects this with its existing volume-query change event to
    /// replace or remove the file, without a timer or a per-file worker.
    pub async fn changed(&mut self) -> Result<SloView, QueryError> {
        let evidence = self.query.changed().await?;
        Ok(self.view(evidence))
    }

    fn view(&self, evidence: Snapshot) -> SloView {
        // The private fields are constructed together after validation. The
        // selected series always names this exact operation and RPC method.
        let verdict = self.slo.evaluate_selected(&evidence);
        SloView {
            slo: self.slo.clone(),
            evidence,
            verdict,
        }
    }
}
