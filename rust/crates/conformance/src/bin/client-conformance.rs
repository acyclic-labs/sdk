//! Real endpoint runner; the host provisions the fixture through AgentHost.
use acyclic_conformance::client::{Fixture, verify};
use acyclic_harness_client::{AgentId, AgentPath};
use acyclic_stream::grpc::Client;
use std::io::Read as _;

fn input_file(path: &str, maximum: usize) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("CLIENT_INPUT file exceeds {maximum} bytes: {path}"),
        ));
    }
    Ok(bytes)
}

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
    if credential.len() > 131_072 {
        return Err("CLIENT_INPUT credential exceeds byte bound".into());
    }
    let credential = hex::decode(credential)?;
    let provider = match std::env::var("ACYCLIC_CONFORMANCE_CA") {
        Ok(path) => {
            Client::connect_eager_with_ca_certificate(
                &endpoint,
                &token,
                input_file(&path, acyclic_stream::MAX_CA_CERTIFICATE_BYTES)?,
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
            let fixture = serde_json::from_slice(&input_file(&path, 8 * 1024 * 1024)?)?;
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
            let fixture = serde_json::from_slice(&input_file(&path, 32 * 1024 * 1024)?)?;
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
    let fixture: Fixture = serde_json::from_slice(&input_file(&path, 65_536)?)?;
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
