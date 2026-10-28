//! Public client qualification against a disposable managed or self-hosted tree.
use acyclic_conformance::client::endpoint::{Scenario, connect, verify};
use acyclic_harness_client::ConnectionOptions;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut endpoint = None;
    let mut scenario = None;
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--endpoint" => endpoint = Some(arguments.next().ok_or("CLIENT_INPUT endpoint missing; recovery: client-conformance --endpoint URL --scenario FILE")?),
            "--scenario" => scenario = Some(arguments.next().ok_or("CLIENT_INPUT scenario missing; recovery: client-conformance --scenario FILE")?),
            _ => return Err("CLIENT_INPUT unknown option; recovery: client-conformance [--endpoint URL] --scenario FILE".into()),
        }
    }
    let scenario = scenario
        .ok_or("CLIENT_INPUT missing scenario; recovery: client-conformance --scenario FILE")?;
    let scenario: Scenario = serde_json::from_slice(&std::fs::read(scenario)?)?;
    let options = ConnectionOptions::from_env(endpoint.as_deref(), None)?;
    let report = verify(connect(&options).await?, &scenario).await?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}
