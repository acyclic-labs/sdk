//! Guest shell vectors: shared verbatim by all machine transports.

use acyclic_machines::{
    ExecCommand, ExecOutput, ExecStop, ExecStopKind, ExecStream, MachineObservation,
    MachinesProvider, ProviderAssurance, ProviderError,
};
use futures::StreamExt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn command(source: &str) -> Result<ExecCommand, String> {
    ExecCommand::new(vec!["/bin/sh".into(), "-c".into(), source.into()], 5_000)
        .map_err(|error| error.to_string())
}

async fn next(output: &mut ExecStream) -> Result<ExecOutput, String> {
    tokio::time::timeout(Duration::from_secs(3), output.next())
        .await
        .map_err(|_| "guest output failed to make bounded progress".to_owned())?
        .ok_or_else(|| "guest output ended without its exit code".to_owned())?
        .map_err(|error| error.to_string())
}

async fn completed(
    provider: &dyn MachinesProvider,
    machine: &MachineObservation,
    source: &str,
) -> Result<(Vec<u8>, Vec<u8>, i32), String> {
    let mut output = provider
        .exec(machine.id, command(source)?)
        .await
        .map_err(|error| error.to_string())?;
    let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
    loop {
        match next(&mut output).await? {
            ExecOutput::Stdout(data) => stdout.extend(data),
            ExecOutput::Stderr(data) => stderr.extend(data),
            ExecOutput::ExitCode(code) => {
                if output.next().await.is_some() {
                    return Err("guest emitted output after its exit code".into());
                }
                return Ok((stdout, stderr, code));
            }
            ExecOutput::Stopped(stop) => return Err(format!("guest capture stopped: {stop:?}")),
        }
    }
}

pub(super) async fn verify(
    provider: &dyn MachinesProvider,
    machine: &MachineObservation,
) -> Result<(), String> {
    if provider.assurance() == ProviderAssurance::ProcessLocalSimulation {
        return if matches!(
            provider.exec(machine.id, command("true")?).await,
            Err(ProviderError::Unsupported(_))
        ) {
            Ok(())
        } else {
            Err("simulation claimed an OS guest execution boundary".into())
        };
    }
    streaming(provider, machine)
        .await
        .map_err(|error| format!("guest streaming vector: {error}"))?;
    cancel_on_drop(provider, machine)
        .await
        .map_err(|error| format!("guest cancel-on-drop vector: {error}"))?;
    absolute_expiry(provider, machine)
        .await
        .map_err(|error| format!("guest absolute-expiry vector: {error}"))?;
    output_limit(provider, machine)
        .await
        .map_err(|error| format!("guest output-limit vector: {error}"))?;
    exit_descendants(provider, machine)
        .await
        .map_err(|error| format!("guest exit cleanup vector: {error}"))?;
    Ok(())
}

async fn streaming(
    provider: &dyn MachinesProvider,
    machine: &MachineObservation,
) -> Result<(), String> {
    let barrier = format!("/tmp/acyclic-stream-{}", machine.id);
    let source = format!(
        "printf '\\000\\377\\001\\001\\001\\002\\002\\002'; printf 'err\\000\\377' >&2; while [ ! -f {barrier} ]; do sleep 0.01; done; exit 7"
    );
    let mut output = provider
        .exec(machine.id, command(&source)?)
        .await
        .map_err(|error| error.to_string())?;
    let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
    while stdout.len() < 8 || stderr.len() < 5 {
        match next(&mut output).await? {
            ExecOutput::Stdout(data) => stdout.extend(data),
            ExecOutput::Stderr(data) => stderr.extend(data),
            ExecOutput::ExitCode(_) | ExecOutput::Stopped(_) => {
                return Err("guest exited before its stream release barrier".into());
            }
        }
    }
    if stdout != [0, 255, 1, 1, 1, 2, 2, 2] || stderr != b"err\0\xff" {
        return Err("guest altered binary stdout or stderr".into());
    }
    if completed(provider, machine, &format!("touch {barrier}"))
        .await?
        .2
        != 0
    {
        return Err("stream release failed".into());
    }
    if next(&mut output).await? != ExecOutput::ExitCode(7) || output.next().await.is_some() {
        return Err("guest did not emit exactly its nonzero exit code".into());
    }
    let retained = provider
        .read_output(machine.id, 0, 65_536)
        .await
        .map_err(|error| error.to_string())?;
    if retained.data.len() < 13 {
        return Err("streamed console output was not retained".into());
    }
    Ok(())
}

async fn cancel_on_drop(
    provider: &dyn MachinesProvider,
    machine: &MachineObservation,
) -> Result<(), String> {
    let leaked = format!("/tmp/acyclic-drop-{}", machine.id);
    let source = format!("printf ready; (sleep 1; touch {leaked}) & wait");
    let mut output = provider
        .exec(machine.id, command(&source)?)
        .await
        .map_err(|error| error.to_string())?;
    let mut ready = Vec::new();
    while ready.len() < 5 {
        match next(&mut output).await? {
            ExecOutput::Stdout(data) => ready.extend(data),
            _ => return Err("drop vector did not reach its dispatch barrier".into()),
        }
    }
    if ready != b"ready" {
        return Err("drop vector altered its dispatch barrier".into());
    }
    drop(output);
    if completed(provider, machine, &format!("sleep 1.1; test ! -e {leaked}"))
        .await?
        .2
        != 0
    {
        return Err("dropping guest output left a process descendant running".into());
    }
    Ok(())
}

async fn absolute_expiry(
    provider: &dyn MachinesProvider,
    machine: &MachineObservation,
) -> Result<(), String> {
    let leaked = format!("/tmp/acyclic-expiry-{}", machine.id);
    let now = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_millis(),
    )
    .map_err(|error| error.to_string())?;
    let command = command(&format!("(sleep 1; touch {leaked}) & wait"))?.with_expiry(now + 300);
    let mut output = provider
        .exec(machine.id, command)
        .await
        .map_err(|error| error.to_string())?;
    // Keep the quiet stream alive and unpolled. Expiry must wake its owner.
    if completed(provider, machine, &format!("sleep 1.1; test ! -e {leaked}"))
        .await?
        .2
        != 0
    {
        return Err("quiet guest survived its absolute authority expiry".into());
    }
    if !matches!(
        next(&mut output).await?,
        ExecOutput::Stopped(ExecStop {
            kind: ExecStopKind::Expired,
            cleanup_completed: true,
            ..
        })
    ) || output.next().await.is_some()
    {
        return Err("absolute expiry did not report its stop and confirmed cleanup".into());
    }
    Ok(())
}

async fn output_limit(
    provider: &dyn MachinesProvider,
    machine: &MachineObservation,
) -> Result<(), String> {
    let leaked = format!("/tmp/acyclic-output-limit-{}", machine.id);
    let source = format!("(sleep 1; touch {leaked}) & printf '\\000\\377'; printf 'err' >&2; wait");
    let mut output = provider
        .exec(machine.id, command(&source)?.with_output_limit(3))
        .await
        .map_err(|error| error.to_string())?;
    let mut bytes = 0;
    loop {
        match next(&mut output).await? {
            ExecOutput::Stdout(data) | ExecOutput::Stderr(data) => bytes += data.len(),
            ExecOutput::Stopped(ExecStop {
                kind: ExecStopKind::OutputLimit,
                cleanup_completed: true,
                ..
            }) => break,
            other => return Err(format!("output overflow returned {other:?}")),
        }
    }
    if bytes != 3 || output.next().await.is_some() {
        return Err("output allowance did not retain exactly its combined prefix".into());
    }
    if completed(provider, machine, &format!("sleep 1.1; test ! -e {leaked}"))
        .await?
        .2
        != 0
    {
        return Err("output overflow left a process descendant running".into());
    }
    Ok(())
}

async fn exit_descendants(
    provider: &dyn MachinesProvider,
    machine: &MachineObservation,
) -> Result<(), String> {
    let leaked = format!("/tmp/acyclic-exit-{}", machine.id);
    let source = format!("(sleep 1; touch {leaked}) & exit 7");
    if completed(provider, machine, &source).await?.2 != 7 {
        return Err("normal exit code changed during descendant cleanup".into());
    }
    if completed(provider, machine, &format!("sleep 1.1; test ! -e {leaked}"))
        .await?
        .2
        != 0
    {
        return Err("normal exit left a process descendant running".into());
    }
    Ok(())
}
