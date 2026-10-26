//! Scenario hosting substrates: each runs expectation-free fixture inputs.
use crate::{CaseObservation, TestResult};
use acyclic_fs::Fs;
use acyclic_stream::{BoxProviderFuture, ProviderPlatform};
use serde_json::Value;
use std::{collections::BTreeSet, sync::Arc};

/// Explicit host capabilities. Native fixture drivers require `local-driver:<name>`.
pub trait Substrate: ProviderPlatform {
    /// Returns explicitly implemented fixture capabilities.
    fn capabilities(&self) -> BTreeSet<String>;
    /// Runs expectation-free fixture input and returns raw observations.
    fn execute(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, TestResult<CaseObservation>>;
}

/// Direct executor host, with fresh providers per case.
pub struct Local;
impl Substrate for Local {
    fn capabilities(&self) -> BTreeSet<String> {
        BTreeSet::from([
            "executor".into(),
            "composer".into(),
            "filesystem-policy".into(),
        ])
    }
    fn execute(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, TestResult<CaseObservation>> {
        Box::pin(crate::collect_on(
            recipe,
            paths,
            Fs::memory(),
            Arc::new(acyclic_stream::MemoryStream::default()),
        ))
    }
}
