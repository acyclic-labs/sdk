use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) fn work()
-> std::result::Result<acyclic_fs::NativeMaterializationWork, Box<dyn std::error::Error>> {
    let mut allowances = serde_json::to_value(WorkBudget::UNBOUNDED)?;
    if let Some(fields) = allowances.as_object_mut() {
        for value in fields.values_mut() {
            *value = json!(1_000_000);
        }
    }
    let budget = serde_json::from_value(allowances)?;
    acyclic_fs::NativeMaterializationWork::new(budget, &acyclic_fs::CancellationToken::new())
        .map_err(|(cause, work)| acyclic_harness::Error::NativeConstruction { cause, work }.into())
}

#[derive(Default)]
pub(super) struct Output {
    stdout: AtomicUsize,
    stderr: AtomicUsize,
}

impl Output {
    pub(super) fn observer(
        self: &Arc<Self>,
    ) -> Arc<acyclic_harness::filesystem::NativeProcessObserver> {
        let output = self.clone();
        Arc::new(move |chunk| {
            let (count, bytes) = match chunk {
                acyclic_native_runtime::ProcessOutput::Stdout(bytes) => (&output.stdout, bytes),
                acyclic_native_runtime::ProcessOutput::Stderr(bytes) => (&output.stderr, bytes),
            };
            count.fetch_add(bytes.len(), Ordering::Relaxed);
            Ok(())
        })
    }

    pub(super) async fn verify(
        &self,
        content: &dyn ContentResidencyVerifier,
        status: Option<&EffectStatus>,
        applied: bool,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let stdout = self.stdout.load(Ordering::Relaxed);
        let stderr = self.stderr.load(Ordering::Relaxed);
        if let Some(EffectStatus::Succeeded { result }) = status {
            let retained: NativeProcessResult =
                serde_json::from_slice(&content.read(result, 1_000_000, None, None)?.await?)?;
            if (stdout, stderr) != (retained.stdout.len(), retained.stderr.len()) {
                return Err("streamed and retained native output differ".into());
            }
        }
        if applied != (stdout > 0) {
            return Err("output observer disagrees with the launch receipt".into());
        }
        Ok(())
    }
}

pub(super) fn child() -> std::result::Result<(), Box<dyn std::error::Error>> {
    use std::io::Read as _;
    let mut input = Vec::new();
    std::io::stdin().read_to_end(&mut input)?;
    if input != b"exact native input" {
        return Err("native process did not receive its exact literal input and EOF".into());
    }
    std::fs::write("destination/output.txt", std::fs::read("source/input.txt")?)?;
    println!("{}", std::env::var("APPROVED_TOKEN")?);
    Ok(())
}
