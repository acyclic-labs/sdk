//! On-demand AgentHost workloads shared by native, guest and hosted adapters.
use super::{Backend, Result, fixtures, observe::Observed, vectors};
use acyclic_harness_core::agent::{AgentHost, AgentId, Delivered};
use acyclic_stream::{UnixMillisClock as _, testing::Requests};
use std::{sync::Arc, time::Duration};

const DELIVERIES: usize = 64;

/// Checked steady-state turn workload, excluding provisioning and warmup.
#[derive(Debug)]
pub struct Measurement {
    /// Retained agents without pending work during measurement.
    pub idle_agents: usize,
    /// Inputs consumed exactly once with their expected conversation effects.
    pub deliveries: usize,
    /// Actual runtime registrations, exactly one for each provisioned agent.
    pub registered_agents: usize,
    /// Ready registrations after the final global drain, required to be zero.
    pub ready_agents: usize,
    /// Elapsed wall-clock time at millisecond resolution. A native performance
    /// runner can additionally time the invocation with a monotonic clock.
    pub elapsed: Duration,
    /// Calls at the public Stream dispatch boundary, including scheduler work.
    pub requests: Requests,
}

/// Fresh populations used by the on-demand scale vector.
#[derive(Clone, Copy, Debug)]
pub struct Scale {
    /// Smaller idle population. Must be positive.
    pub small: usize,
    /// Larger idle population. Must be strictly greater than `small`.
    pub large: usize,
}

impl Default for Scale {
    fn default() -> Self {
        Self {
            small: 1_000,
            large: 10_000,
        }
    }
}

/// Both independently provisioned scale samples. This measures dispatch work
/// and actual runtime registration counts; it makes no claim about physical
/// memory or a backend's internal allocations.
#[derive(Debug)]
pub struct ScaleMeasurement {
    /// The smaller fresh population.
    pub small: Measurement,
    /// The larger fresh population.
    pub large: Measurement,
}

/// Run the shared checked turn workload. No CI or merge gate invokes this.
/// Request counts are reported separately from timing, so control requests
/// cannot disappear into a single throughput number.
pub async fn bench(backend: &dyn Backend) -> Result<Measurement> {
    measure(backend, 0).await
}

/// Send the same work through the global runtime drain with two idle populations.
/// At most twice as many data or control calls may be issued at the larger size;
/// a zero control baseline must remain zero. Population creation, scheduler
/// warmup and final observation are excluded from both request deltas.
/// Every settled turn must retain exactly one registration per provisioned
/// agent and leave no ready registration after the global drain.
pub async fn scale(backend: &dyn Backend, sizes: Scale) -> Result<ScaleMeasurement> {
    if sizes.small == 0 || sizes.large <= sizes.small {
        return Err(format!(
            "STEP agent-scale setup failed: require 0 < small < large, got {sizes:?}; \
             recovery: call conformance::scale(backend, Scale::default())"
        )
        .into());
    }
    let small = measure(backend, sizes.small).await?;
    let large = measure(backend, sizes.large).await?;
    if large.requests.data() > small.requests.data().saturating_mul(2)
        || large.requests.control() > small.requests.control().saturating_mul(2)
    {
        return Err(format!(
            "STEP agent-scale dispatch failed: small={small:?}, large={large:?}; \
             recovery: rerun conformance::scale with the same backend and Scale {sizes:?}"
        )
        .into());
    }
    Ok(ScaleMeasurement { small, large })
}

async fn measure(backend: &dyn Backend, idle_agents: usize) -> Result<Measurement> {
    let expected_agents = idle_agents.checked_add(1).ok_or(
        "STEP agent-scale population failed: agent count overflow; recovery: call conformance::scale(backend, Scale::default())",
    )?;
    // `storage` owns the durable domain until the final observation completes.
    let storage = backend.fresh().await?;
    let provider = Arc::new(Observed::new(storage.open().await?));
    let host = vectors::host(backend, provider.clone())?;
    let active = host
        .create(&vectors::intent("bench-active")?, fixtures::adopted())
        .await?;
    let mut last_idle = None;
    for index in 0..idle_agents {
        let mut adopted = fixtures::adopted();
        adopted.agent = AgentId::from_bytes((index as u128 + 2).to_le_bytes());
        last_idle = Some(
            host.create(&vectors::intent(&format!("bench-idle-{index}"))?, adopted)
                .await?,
        );
    }
    vectors::drain(&host).await?;
    host.send(&active, &fixtures::sender(), &fixtures::request("warmup")?)
        .await?;
    vectors::drain(&host).await?;

    registrations(&host, expected_agents)?;
    let ledger = provider.requests.clone();
    let before = ledger.requests();
    let clock = acyclic_stream::SystemUnixMillisClock;
    let started = clock.now_unix_millis();
    let mut expected = vec![b"warmup".to_vec()];
    for index in 0..DELIVERIES {
        let id = format!("bench-turn-{index}");
        if !matches!(
            host.send(&active, &fixtures::sender(), &fixtures::request(&id)?)
                .await?,
            Delivered::Committed { .. }
        ) {
            return Err("agent benchmark refused an input to a live agent".into());
        }
        // Global drain is intentional: a target-only drain would hide idle
        // population scans in the scheduler under test.
        vectors::drain(&host).await?;
        registrations(&host, expected_agents)?;
        expected.push(id.into_bytes());
    }
    let elapsed = Duration::from_millis(clock.now_unix_millis().saturating_sub(started));
    let requests = ledger.requests().since(before);
    if vectors::values(&*provider, active.conversation()?).await? != expected {
        return Err("agent benchmark lost, changed, reordered or duplicated a turn".into());
    }
    if let Some(idle) = last_idle {
        if !vectors::values(&*provider, idle.conversation()?)
            .await?
            .is_empty()
        {
            return Err("idle agent acquired an unsolicited conversation effect".into());
        }
    }
    let (registered_agents, ready_agents) = registrations(&host, expected_agents)?;
    Ok(Measurement {
        idle_agents,
        deliveries: DELIVERIES,
        registered_agents,
        ready_agents,
        elapsed,
        requests,
    })
}

fn registrations(host: &AgentHost<Observed>, expected: usize) -> Result<(usize, usize)> {
    let (registered, ready) = host.runtime().counts();
    if registered != expected || ready != 0 {
        return Err(format!(
            "STEP agent-scale registrations failed: expected={expected}, registered={registered}, ready={ready}; \
             recovery: rerun conformance::scale with the same backend and Scale::default()"
        )
        .into());
    }
    Ok((registered, ready))
}
