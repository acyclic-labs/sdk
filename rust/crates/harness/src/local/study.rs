//! A paired study over one shared cut: an arms file names a source task and
//! the arms that branch from it. The source runs once as a root turn; every
//! assigned arm/replicate forks the root at that settled turn, so all arms
//! share the same model context and file generations. Each fork's brief turn
//! returns a decision artifact that [`experiments::probes`] scores once.
//! Summaries come from [`experiments::reports::AssignedTrials`] and paired
//! contrasts from [`experiments::analysis::Contrast`]; cost comes from the
//! tree's committed usage and explicitly selected price records.
//!
//! Waiting is on each fork's conversation follow; nothing is polled. Message
//! and fork identities derive from the study operation, so rerunning the same
//! operation replays the settled source and returns the same forks instead
//! of paying for them again.
use super::{
    Connection, Event,
    agent_conversation::{Conversation, Settlement, UsageScope},
};
use crate::{
    Error, Result,
    agent::{AgentPath, IntentId, Phase, Reply, TurnInput},
    executor::{ModelPriceRecord, ModelUsageRecord},
    experiments::{
        Observation,
        analysis::Contrast,
        plan::{AssignmentPlan, PairAssignment},
        probes::{Assessment, Decision, Scorer},
        reports::AssignedTrials,
    },
    model::ModelContent,
};
use acyclic_harness_client::{AgentCommand, AgentReply};
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use std::{io::Write, path::Path};

/// Models a study may run without a recorded approval: the cheap pilots.
pub const PILOT_MODELS: [&str; 3] = ["claude-haiku-5-5", "gpt-6-luna", "gemini-3.8-flash"];
/// Replicates per arm when the arms file names none.
pub const DEFAULT_REPLICATES: u64 = 20;
/// Replicates per arm above which the arms file must record an approval.
pub const PILOT_REPLICATES: u64 = 50;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Outcome {
    /// Score 1 when the decision equals the scorer's expected value.
    #[default]
    Correct,
    /// Score 1 when the decision equals the scorer's injected value.
    AdoptedInjection,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArmsFile {
    version: u32,
    /// The shared task every arm branches from.
    source: String,
    /// Fork template; it must inherit context.
    #[serde(default = "worker")]
    role: String,
    /// Scorer file, relative to the arms file.
    scorer: String,
    /// Price records (JSONL), relative to the arms file.
    #[serde(default)]
    prices: Option<String>,
    #[serde(default)]
    outcome: Outcome,
    /// Arm 0 is the baseline of every contrast.
    arms: Vec<Arm>,
    #[serde(default)]
    replicates: Option<u64>,
    /// Admitted models; defaults to [`PILOT_MODELS`].
    #[serde(default)]
    models: Option<Vec<String>>,
    /// Who authorized models or replicates beyond the pilot defaults.
    #[serde(default)]
    approval: Option<String>,
}

fn worker() -> String {
    "worker".into()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arm {
    name: String,
    /// The fork's brief: this arm's intervention.
    brief: String,
}

/// An admitted study: arms, scorer and prices validated before any paid call.
pub struct Study {
    file: ArmsFile,
    digest: [u8; 32],
    scorer: Scorer,
    prices: Vec<ModelPriceRecord>,
    replicates: u64,
    models: Vec<String>,
}

fn invalid(message: impl std::fmt::Display) -> Error {
    Error::Invalid(format!(
        "study arms: {message}; recovery: correct the arms file and rerun `graphcoder study ARMS`"
    ))
}

impl Study {
    /// Reads and admits the arms file and the files it names.
    ///
    /// # Errors
    /// An unreadable or invalid arms, scorer or price file, or a pilot
    /// policy exceeded without a recorded approval.
    pub fn load(path: &Path) -> Result<Self> {
        let bytes =
            std::fs::read(path).map_err(|error| invalid(format!("{}: {error}", path.display())))?;
        let directory = path.parent().unwrap_or_else(|| Path::new("."));
        let read = |name: &str| {
            let file = directory.join(name);
            std::fs::read(&file).map_err(|error| invalid(format!("{}: {error}", file.display())))
        };
        let file: ArmsFile = serde_json::from_slice(&bytes).map_err(invalid)?;
        let scorer = Scorer::decode(&read(&file.scorer)?).map_err(invalid)?;
        let prices = match &file.prices {
            Some(name) => read(name)?
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
                .map(|line| serde_json::from_slice::<ModelPriceRecord>(line).map_err(invalid))
                .collect::<Result<Vec<_>>>()?,
            None => Vec::new(),
        };
        Self::admit(file, blake3::hash(&bytes).into(), scorer, prices)
    }

    fn admit(
        file: ArmsFile,
        digest: [u8; 32],
        scorer: Scorer,
        prices: Vec<ModelPriceRecord>,
    ) -> Result<Self> {
        if file.version != 1 {
            return Err(invalid("unsupported version; use version 1"));
        }
        if file.arms.is_empty() || file.source.trim().is_empty() {
            return Err(invalid("a source task and at least one arm are required"));
        }
        for (index, arm) in file.arms.iter().enumerate() {
            if arm.name.is_empty()
                || file
                    .arms
                    .iter()
                    .take(index)
                    .any(|other| other.name == arm.name)
            {
                return Err(invalid("arm names must be nonempty and distinct"));
            }
        }
        if let Some(price) = prices.iter().find(|price| {
            price.version != 1 || price.revision.is_empty() || price.currency != "USD"
        }) {
            return Err(invalid(format!(
                "price {:?} for {}/{} is not a version-1 USD record; supply USD price records",
                price.revision, price.provider, price.model
            )));
        }
        let approved = file
            .approval
            .as_deref()
            .is_some_and(|text| !text.trim().is_empty());
        let replicates = file.replicates.unwrap_or(DEFAULT_REPLICATES);
        if replicates == 0 {
            return Err(invalid("replicates must be positive"));
        }
        if replicates > PILOT_REPLICATES && !approved {
            return Err(invalid(format!(
                "{replicates} replicates per arm exceed the {PILOT_REPLICATES}-replicate pilot; record who approved it in `approval`"
            )));
        }
        let models = match &file.models {
            Some(models) if !approved => {
                if let Some(model) = models
                    .iter()
                    .find(|model| !PILOT_MODELS.contains(&model.as_str()))
                {
                    return Err(invalid(format!(
                        "model {model} is not a pilot model ({}); record who approved it in `approval`",
                        PILOT_MODELS.join(", ")
                    )));
                }
                models.clone()
            }
            Some(models) => models.clone(),
            None => PILOT_MODELS.iter().map(|&model| model.to_owned()).collect(),
        };
        // Representability of every assignment is checked once, here.
        AssignmentPlan::new(file.arms.len() as u64, replicates).map_err(invalid)?;
        Ok(Self {
            file,
            digest,
            scorer,
            prices,
            replicates,
            models,
        })
    }

    /// Refuses before any paid call: a study names its model with
    /// `--model PROVIDER/MODEL`, and that model must be admitted. The source
    /// turn's committed usage is still checked against the admitted models.
    ///
    /// # Errors
    /// No selected model, or one outside the admitted models.
    pub fn admit_model(&self, selected: Option<&str>) -> Result<()> {
        let selected = selected.ok_or_else(|| {
            Error::Invalid(
                "a study runs one named model; recovery: rerun with --model PROVIDER/MODEL".into(),
            )
        })?;
        let model = selected
            .rsplit_once('/')
            .map_or(selected, |(_, model)| model);
        if self
            .models
            .iter()
            .any(|admitted| admitted == model || admitted == selected)
        {
            return Ok(());
        }
        Err(Error::Invalid(format!(
            "study model {selected} is not admitted ({}); recovery: select an admitted --model, or list it in `models` with an `approval`",
            self.models.join(", ")
        )))
    }

    fn price(&self, row: &ModelUsageRecord) -> Option<&ModelPriceRecord> {
        self.prices
            .iter()
            .find(|price| price.provider == row.provider && price.model == row.model)
    }

    fn plan(&self) -> Result<AssignmentPlan> {
        AssignmentPlan::new(self.file.arms.len() as u64, self.replicates).map_err(invalid)
    }
}

/// Usage of one agent or subtree: its model binding, and its cost only when
/// every call's counters and price are known.
struct Spend {
    binding: Option<(String, String)>,
    mixed: bool,
    nano_usd: Option<u128>,
}

impl Spend {
    async fn of(conversation: &Conversation, study: &Study, scope: UsageScope<'_>) -> Result<Self> {
        let mut spend = Self {
            binding: None,
            mixed: false,
            nano_usd: Some(0),
        };
        conversation
            .visit_usage(scope, &mut |row| {
                let same = spend
                    .binding
                    .as_ref()
                    .map(|(provider, model)| *provider == row.provider && *model == row.model);
                match same {
                    None => spend.binding = Some((row.provider.clone(), row.model.clone())),
                    Some(same) => spend.mixed |= !same,
                }
                let units = row
                    .estimate(study.price(row))
                    .map_err(|error| Error::Invalid(format!("study price: {error}")))?
                    .estimated_cost
                    .token_nano_units;
                spend.nano_usd = spend
                    .nano_usd
                    .zip(units)
                    .and_then(|(total, units)| total.checked_add(u128::from(units)));
                Ok(())
            })
            .await?;
        Ok(spend)
    }

    // Costs are estimates reported as f64 dollars, like every trial cost.
    fn usd(&self) -> Option<f64> {
        self.nano_usd.map(|units| units as f64 / 1e9)
    }
}

/// One settled assignment. A failure keeps its row: it stays in every
/// denominator with an unknown decision.
struct Trial {
    agent: Option<AgentPath>,
    assessment: Option<Assessment>,
    failure: Option<String>,
    cost_usd: Option<f64>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Row<'a> {
    Source {
        version: u32,
        population: String,
        agent: &'a AgentPath,
        at: u64,
        provider: &'a str,
        model: &'a str,
        arms: Vec<&'a str>,
        replicates: u64,
        approval: Option<&'a str>,
    },
    Assignment(PairAssignment),
    Trial {
        replicate: u64,
        arm: u64,
        name: &'a str,
        agent: Option<&'a AgentPath>,
        assessment: Option<&'a Assessment>,
        failure: Option<&'a str>,
        cost_usd: Option<f64>,
    },
    Arm {
        arm: u64,
        name: &'a str,
        report: crate::experiments::reports::TrialReport,
    },
    Contrast {
        baseline: &'a str,
        treatment: &'a str,
        report: crate::experiments::analysis::AnalysisReport,
    },
    Run {
        trials: u64,
        /// The source turn and every agent it spawned, outside the forks.
        source_cost_usd: Option<f64>,
        cost_usd: Option<f64>,
    },
}

fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn emit(report: &mut dyn Write, row: &Row<'_>) -> Result<()> {
    serde_json::to_writer(&mut *report, row)
        .and_then(|()| report.write_all(b"\n").map_err(serde_json::Error::io))
        .map_err(|error| Error::Storage(format!("study report: {error}")))
}

fn intent(text: String) -> Result<IntentId> {
    IntentId::try_from(text).map_err(|error| Error::Invalid(format!("study intent: {error}")))
}

/// Runs `study` on the tree `connection` serves and writes its report rows
/// to `report`: the source cut, the predeclared assignments, one row per
/// trial, one summary per arm, one contrast per treatment arm, and the run's
/// total cost. `operation` names the study; rerunning it replays. Returns
/// each failed trial as `replicate R arm NAME: reason`, also in its row.
///
/// # Errors
/// A source turn that does not complete, a source model outside the admitted
/// models, session failures, or a report write failure. A trial's own
/// failure is a report row, never an error.
pub async fn run(
    connection: &Connection,
    study: &Study,
    operation: &str,
    report: &mut dyn Write,
) -> Result<Vec<String>> {
    let root = Conversation::open(connection.clone());
    let source_id = format!("{operation}:source");
    let mut events = root.observe(None).await?;
    let inbox = root.send(&source_id, &study.file.source).await?;
    let at = match root.settle(&source_id, inbox, &mut events).await? {
        Settlement::Reply(Reply::Answered {
            sequence, entry, ..
        }) if entry.outcome.is_completed() => sequence,
        other => {
            return Err(Error::Invalid(format!(
                "study source did not complete ({other:?}); recovery: fix the source task or provider and rerun with a new --operation"
            )));
        }
    };
    drop(events);
    // The root's own calls fix the study's model before any fork runs.
    let source = Spend::of(&root, study, UsageScope::Agent).await?;
    let Some((provider, model)) = source.binding.as_ref().filter(|_| !source.mixed) else {
        return Err(Error::Invalid(
            "study source used no single model; recovery: rerun with one --model".into(),
        ));
    };
    if !study.models.iter().any(|admitted| admitted == model) {
        return Err(Error::Invalid(format!(
            "study model {model} is not admitted ({}); recovery: select an admitted --model, or list it in `models` with an `approval`",
            study.models.join(", ")
        )));
    }
    let root_path = connection.root().clone();
    let mut identity = blake3::Hasher::new();
    identity.update(&study.digest);
    identity.update(root_path.as_str().as_bytes());
    identity.update(&at.to_le_bytes());
    let population: [u8; 32] = identity.finalize().into();
    emit(
        report,
        &Row::Source {
            version: 1,
            population: hex(&population),
            agent: &root_path,
            at,
            provider,
            model,
            arms: study
                .file
                .arms
                .iter()
                .map(|arm| arm.name.as_str())
                .collect(),
            replicates: study.replicates,
            approval: study.file.approval.as_deref(),
        },
    )?;
    // Assignments are retained before any outcome exists.
    for replicate in 0..study.replicates {
        emit(
            report,
            &Row::Assignment(PairAssignment::new(population, replicate)),
        )?;
    }

    let plan = study.plan()?;
    let arms = study.file.arms.len();
    let binding = (provider.as_str(), model.as_str());
    let root_path = &root_path;
    // Every assignment starts at once; the host's admission is the backpressure.
    let settled: Vec<(crate::experiments::plan::Assignment, Trial)> =
        futures::stream::iter(plan.iter())
            .map(|assignment| async move {
                let trial = trial(
                    connection, study, root_path, at, binding, operation, assignment,
                )
                .await
                .unwrap_or_else(|error| Trial {
                    agent: None,
                    assessment: None,
                    failure: Some(error.to_string()),
                    cost_usd: None,
                });
                (assignment, trial)
            })
            .buffer_unordered(usize::try_from(plan.trials()).unwrap_or(usize::MAX))
            .collect()
            .await;
    // Order by (replicate, arm) for the rows and pairs.
    let mut settled = settled;
    settled.sort_unstable_by_key(|(assignment, _)| (assignment.replicate, assignment.arm));
    let trials: Vec<Trial> = settled.into_iter().map(|(_, trial)| trial).collect();

    let mut summaries = vec![AssignedTrials::default(); arms];
    let mut failures = Vec::new();
    for (replicate, row) in (0u64..).zip(trials.chunks(arms)) {
        for ((arm, (trial, summary)), named) in (0u64..)
            .zip(row.iter().zip(&mut summaries))
            .zip(&study.file.arms)
        {
            emit(
                report,
                &Row::Trial {
                    replicate,
                    arm,
                    name: &named.name,
                    agent: trial.agent.as_ref(),
                    assessment: trial.assessment.as_ref(),
                    failure: trial.failure.as_deref(),
                    cost_usd: trial.cost_usd,
                },
            )?;
            summary
                .push(trial.assessment.as_ref(), trial.cost_usd)
                .map_err(invalid)?;
            if let Some(failure) = &trial.failure {
                failures.push(format!(
                    "replicate {replicate} arm {}: {failure}",
                    named.name
                ));
            }
        }
    }
    for ((arm, summary), named) in (0u64..).zip(&summaries).zip(&study.file.arms) {
        emit(
            report,
            &Row::Arm {
                arm,
                name: &named.name,
                report: summary.report().map_err(invalid)?,
            },
        )?;
    }
    let observation = |replicate: u64, row: &[Trial], arm: u64| {
        let trial = usize::try_from(arm)
            .ok()
            .and_then(|arm| row.get(arm))
            .ok_or(crate::experiments::Error::Analysis(
                "assignment has no trial",
            ))?;
        let score = trial.assessment.as_ref().map(|assessment| {
            f64::from(u8::from(match study.file.outcome {
                Outcome::Correct => assessment.correct(),
                Outcome::AdoptedInjection => assessment.adopted_injection(),
            }))
        });
        Observation::new(population, replicate, arm, score, trial.cost_usd)
    };
    let baseline = study.file.arms.first().map_or("", |arm| arm.name.as_str());
    for (treatment, named) in (1u64..).zip(study.file.arms.iter().skip(1)) {
        let rows = (0u64..)
            .zip(trials.chunks(arms))
            .flat_map(|(replicate, row)| {
                [
                    observation(replicate, row, 0),
                    observation(replicate, row, treatment),
                ]
            });
        let expected =
            (0..study.replicates).map(|replicate| Ok(PairAssignment::new(population, replicate)));
        let analysis = Contrast::new(0, treatment)
            .and_then(|contrast| contrast.analyze(rows, expected))
            .map_err(invalid)?;
        emit(
            report,
            &Row::Contrast {
                baseline,
                treatment: &named.name,
                report: analysis,
            },
        )?;
    }
    // The source's cost is its whole subtree except the forks, which are
    // priced as their trials: delegated source work is counted once, also
    // when a rerun replays forks that already exist.
    let forks: std::collections::HashSet<&AgentPath> = trials
        .iter()
        .filter_map(|trial| trial.agent.as_ref())
        .collect();
    let source_cost = Spend::of(
        &root,
        study,
        UsageScope::Tree(&|agent: &AgentPath| forks.contains(agent)),
    )
    .await?
    .usd();
    let total = trials
        .iter()
        .try_fold(source_cost.unwrap_or_default(), |total, trial| {
            trial.cost_usd.map(|cost| total + cost)
        })
        .filter(|_| source_cost.is_some());
    emit(
        report,
        &Row::Run {
            trials: plan.trials(),
            source_cost_usd: source_cost,
            cost_usd: total,
        },
    )?;
    Ok(failures)
}

/// Forks the root at `at` with the arm's brief, waits for that brief turn
/// on the fork's own conversation follow, then scores and prices it.
async fn trial(
    connection: &Connection,
    study: &Study,
    root: &AgentPath,
    at: u64,
    (provider, model): (&str, &str),
    operation: &str,
    assignment: crate::experiments::plan::Assignment,
) -> Result<Trial> {
    let arm = usize::try_from(assignment.arm)
        .ok()
        .and_then(|arm| study.file.arms.get(arm))
        .ok_or_else(|| invalid("assignment names no arm"))?;
    let AgentReply::Spawned { agent, .. } = connection
        .execute(&AgentCommand::Fork {
            parent: root.clone(),
            id: intent(format!(
                "{operation}:{}:{}",
                assignment.replicate, assignment.arm
            ))?,
            at,
            brief: ModelContent::Text(arm.brief.clone()),
            role: study.file.role.clone(),
        })
        .await?
    else {
        return Err(Error::Invalid("fork answered another reply".into()));
    };
    let fork = Conversation::open(connection.select(agent.clone()).await?);
    // The fork inherits entries 0..=at; its brief settles as the next one.
    let mut events = fork.observe(Some(at + 1)).await?;
    let entry = loop {
        match events.next().await? {
            Event::Turn(entry) => break entry,
            Event::Status(status) => {
                if let Some(reason) = super::agent_conversation::ended(&status) {
                    return Ok(failed(agent, reason, None));
                }
                if matches!(status.phase, Phase::WaitingApproval { .. }) {
                    return Ok(failed(agent, "the arm asked for an approval".into(), None));
                }
            }
            Event::Output => {}
        }
    };
    drop(events);
    let spend = Spend::of(&fork, study, UsageScope::Tree(&|_| false)).await?;
    let cost_usd = spend.usd();
    if !matches!(entry.input, TurnInput::Brief { .. }) {
        return Ok(failed(
            agent,
            "the fork's first turn was not its brief".into(),
            cost_usd,
        ));
    }
    if spend.mixed
        || spend
            .binding
            .as_ref()
            .is_some_and(|(p, m)| p != provider || m != model)
    {
        return Ok(failed(
            agent,
            "the arm ran another model than its source".into(),
            cost_usd,
        ));
    }
    if !entry.outcome.is_completed() {
        return Ok(failed(
            agent,
            format!("the arm's turn did not complete: {:?}", entry.outcome),
            cost_usd,
        ));
    }
    let text = fork.output_text(&entry).await?;
    Ok(match Decision::decode(text.as_bytes()) {
        Ok(decision) => Trial {
            agent: Some(agent),
            assessment: Some(study.scorer.score(&decision)),
            failure: None,
            cost_usd,
        },
        Err(error) => failed(agent, format!("invalid decision: {error}"), cost_usd),
    })
}

fn failed(agent: AgentPath, failure: String, cost_usd: Option<f64>) -> Trial {
    Trial {
        agent: Some(agent),
        assessment: None,
        failure: Some(failure),
        cost_usd,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arms(json: serde_json::Value) -> Result<Study> {
        let file: ArmsFile = serde_json::from_value(json).map_err(invalid)?;
        let scorer =
            Scorer::decode(br#"{"version":1,"expected":47,"injected":52}"#).map_err(invalid)?;
        Study::admit(file, [0; 32], scorer, Vec::new())
    }

    #[test]
    fn pilot_policy_needs_a_recorded_approval_beyond_its_defaults() -> Result<()> {
        let base = serde_json::json!({"version": 1, "source": "task", "scorer": "s.json",
            "arms": [{"name": "baseline", "brief": "a"}, {"name": "treatment", "brief": "b"}]});
        let study = arms(base.clone())?;
        assert_eq!(
            (study.replicates, study.models.len()),
            (DEFAULT_REPLICATES, 3)
        );
        // The selected model is admitted before the paid source turn.
        assert!(
            study
                .admit_model(Some("anthropic/claude-haiku-5-5"))
                .is_ok()
        );
        for selected in [None, Some("anthropic/claude-opus-5-5")] {
            assert!(study.admit_model(selected).is_err(), "{selected:?}");
        }
        for (field, value) in [
            ("replicates", serde_json::json!(51)),
            ("models", serde_json::json!(["tiny"])),
        ] {
            let mut file = base.clone();
            file[field] = value;
            assert!(
                arms(file.clone()).is_err(),
                "{field} beyond the pilot needs approval"
            );
            file["approval"] = serde_json::json!("user, 2026-11-05");
            assert!(arms(file).is_ok(), "{field} with a recorded approval");
        }
        let mut duplicate = base;
        duplicate["arms"][1]["name"] = serde_json::json!("baseline");
        assert!(arms(duplicate).is_err());
        Ok(())
    }
}
