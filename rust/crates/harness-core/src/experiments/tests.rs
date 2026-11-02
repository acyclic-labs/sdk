use super::*;
use crate::{
    agent::AgentId,
    profile::{Profiles, RoleFile},
};

fn profiles(worker_model: &str) -> Result<Profiles, crate::Error> {
    // These are configured role inputs, not captured provider responses.
    let root = "---\nversion: 1\nname: orchestrator\nmodel: openai/gpt-6-luna\ncontext: top_level\nmay_spawn: [worker]\n---\nCoordinate.\n";
    let worker = format!(
        "---\nversion: 1\nname: worker\nmodel: {worker_model}\ncontext: fork\nmay_spawn: []\n---\nAnswer the task.\n"
    );
    Profiles::new([RoleFile::parse(root)?, RoleFile::parse(&worker)?])
}

fn arms() -> ArmsFile {
    ArmsFile {
        version: 1,
        experiment: "paired-task".into(),
        cut: Cut {
            agent: AgentPath::root(&AgentId::from_bytes([7; 16])),
            turn: 2,
        },
        replicates: NonZeroU64::MAX,
        arms: ["baseline", "treatment"]
            .map(|name| Arm {
                name: name.into(),
                topology: Topology::ForkJoin,
                role: "worker".into(),
                model: CheapModel::Luna,
                prompt: format!("{name}: solve the task"),
            })
            .to_vec(),
    }
}

#[test]
fn enormous_population_has_lazy_unique_reproducible_forks_from_one_cut()
-> Result<(), Box<dyn std::error::Error>> {
    let profiles = profiles(CheapModel::Luna.key())?;
    let population = Population::admit(arms(), &profiles, &profiles.root()?)?;
    // N cannot be enumerated in a test or represented by N times the arm count.
    // Taking three replicates demonstrates no eager multiplication/allocation.
    let trials: Vec<_> = population.trials().take(6).collect();
    assert_eq!(
        trials
            .iter()
            .map(|trial| trial.arm.name.as_str())
            .collect::<Vec<_>>(),
        [
            "baseline",
            "treatment",
            "treatment",
            "baseline",
            "baseline",
            "treatment"
        ]
    );
    let mut intents = HashSet::new();
    for (trial, replay) in trials.iter().zip(population.trials()) {
        assert_eq!(trial.file.cut, arms().cut);
        assert_eq!(trial.intent()?, replay.intent()?);
        assert!(intents.insert(trial.intent()?));
    }
    let mut changed = arms();
    changed.arms[1].prompt.push_str(" changed");
    let changed = Population::admit(changed, &profiles, &profiles.root()?)?;
    assert_ne!(population.digest(), changed.digest());
    assert_ne!(
        population
            .trials()
            .next()
            .ok_or("missing trial")?
            .intent()?,
        changed.trials().next().ok_or("missing trial")?.intent()?
    );
    Ok(())
}

#[test]
fn unsafe_admission_and_mismatched_pairs_are_refused_before_execution()
-> Result<(), Box<dyn std::error::Error>> {
    let expensive = profiles("anthropic/claude-sonnet-5-5")?;
    assert!(Population::admit(arms(), &expensive, &expensive.root()?).is_err());
    let profiles = profiles(CheapModel::Haiku.key())?;
    assert!(Population::admit(arms(), &profiles, &profiles.root()?).is_err());
    let mut invalid = arms();
    invalid.version = 2;
    assert!(ArmsFile::decode(&serde_json::to_vec(&invalid)?).is_err());
    invalid = arms();
    invalid.arms[1].name = invalid.arms[0].name.clone();
    assert!(invalid.validate().is_err());
    let baseline = Observation {
        version: 1,
        population: "a".into(),
        replicate: 0,
        arm: "baseline".into(),
        score: 1.0,
        cost_usd: Some(0.02),
    };
    let mut treatment = Observation {
        arm: "treatment".into(),
        score: 3.0,
        ..baseline.clone()
    };
    assert_eq!(treatment.difference(&baseline)?, 2.0);
    treatment.replicate = 1;
    assert!(treatment.difference(&baseline).is_err());
    treatment.replicate = 0;
    treatment.population = "another cut".into();
    assert!(treatment.difference(&baseline).is_err());
    Ok(())
}
