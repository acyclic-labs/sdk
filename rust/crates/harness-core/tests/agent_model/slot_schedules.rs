//! Deterministic actor schedules: the real step, paused tool effects, client
//! arrivals and replay after losing the step response. No second reducer.
use super::*;

#[tokio::test]
async fn an_invalid_host_wait_fails_before_publishing_status_or_effects() -> Result {
    for host in [String::new(), "\0".into()] {
        let (root, mut state) = populated_parent().await?;
        state.turn = Some(Progress {
            inbox: 0,
            checkpoint: Vec::new(),
        });
        state.phase = Phase::Running;
        let driver = script(vec![TurnStep {
            checkpoint: Some(vec![]),

            outs: vec![Out::Spawn {
                intent: intent("spawn:invalid-host"),
                brief: None,
                volumes: vec![],
            }],
            wait: Wait::Host(host),
            drops: None,
        }]);
        let result = step(
            &driver,
            &root,
            Some(state),
            1,
            Wake::Alarm,
            acyclic_actors::guest::CancellationToken::new(),
        )
        .await?;
        assert!(
            matches!(
                result.state.phase,
                Phase::Done {
                    outcome: AgentOutcome::Failed { .. }
                }
            ),
            "invalid host wait must settle as a bounded failure"
        );
        assert!(
            spawns(&result.outs).is_empty(),
            "invalid host wait published unchecked effects"
        );
        StatusRecord {
            phase: result.state.phase,
            cursor: u64::MAX,
            conversation: u64::MAX,
        }
        .encode()?;
    }
    Ok(())
}
