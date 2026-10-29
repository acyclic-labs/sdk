//! Real endpoint runner; the host provisions the fixture through AgentHost.
use acyclic_conformance::client::{Fixture, verify};
use acyclic_harness_client::{AgentId, AgentPath};
use acyclic_stream::grpc::Client;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("ACYCLIC_CONFORMANCE_TOOL_FIXTURE").is_some()
        && std::env::var_os("ACYCLIC_CONFORMANCE_CONTROL_FIXTURE").is_some()
    {
        return Err("CLIENT_INPUT choose one tool or control fixture".into());
    }
    let endpoint = std::env::var("ACYCLIC_CONFORMANCE_ENDPOINT")?;
    let token = std::env::var("ACYCLIC_CONFORMANCE_TOKEN")?;
    let credential = std::env::var("ACYCLIC_CONFORMANCE_SESSION_CREDENTIAL")?;
    let credential = hex::decode(credential)?;
    let provider = match std::env::var("ACYCLIC_CONFORMANCE_CA") {
        Ok(path) => {
            Client::connect_eager_with_ca_certificate(
                &endpoint,
                &token,
                std::fs::read(&path)?,
            )
            .await?
        }
        Err(std::env::VarError::NotPresent) => Client::connect_eager(&endpoint, &token).await?,
        Err(error) => return Err(error.into()),
    };
    let session = provider.session_client()?;
    session.register_credential(0, credential.into())?;
    match std::env::var("ACYCLIC_CONFORMANCE_TOOL_FIXTURE") {
        Ok(path) => {
            let fixture = serde_json::from_slice(&std::fs::read(&path)?)?;
            let report =
                acyclic_conformance::client::tool_output::verify(&provider, session, 0, &fixture)
                    .await?;
            println!("{}", serde_json::to_string(&report)?);
            return Ok(());
        }
        Err(std::env::VarError::NotPresent) => {}
        Err(error) => return Err(error.into()),
    }
    match std::env::var("ACYCLIC_CONFORMANCE_CONTROL_FIXTURE") {
        Ok(path) => {
            let fixture = serde_json::from_slice(&std::fs::read(&path)?)?;
            let report =
                acyclic_conformance::client::controls::verify(&provider, &session, 0, &fixture)
                    .await?;
            println!("{}", serde_json::to_string(&report)?);
            return Ok(());
        }
        Err(std::env::VarError::NotPresent) => {}
        Err(error) => return Err(error.into()),
    }
    let namespace = AgentPath::root(&AgentId::try_from(std::env::var(
        "ACYCLIC_CONFORMANCE_NAMESPACE",
    )?)?);
    let path = std::env::var("ACYCLIC_CONFORMANCE_AGENT_FIXTURE")?;
    let fixture: Fixture = serde_json::from_slice(&std::fs::read(&path)?)?;
    if fixture.agent != namespace
        && !fixture
            .agent
            .as_str()
            .starts_with(&format!("{}/children/", namespace.as_str()))
    {
        return Err("CLIENT_INPUT fixture is outside the authorized namespace".into());
    }
    let report = verify(&provider, session, 0, &fixture).await?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}
