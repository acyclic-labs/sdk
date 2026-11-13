//! CLI composition of shared provider, terminal and local-host preflights.
use super::*;
use acyclic_harness::tui::onboarding;
use acyclic_harness::tui::term::Tty;
use std::io::{IsTerminal as _, Write as _};

type Failure = Box<dyn std::error::Error>;

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser as _;

    #[tokio::test]
    async fn home_selection_checks_credentials_for_the_effective_model() -> Result<(), Failure> {
        let directory = tempfile::tempdir()?;
        let workspace = directory.path().join("project");
        std::fs::create_dir(&workspace)?;
        let catalog = directory.path().join("catalog.json");
        std::fs::write(
            &catalog,
            serde_json::to_vec(&serde_json::json!({
                "available": {
                    "adapter": "responses", "authentication": "optional",
                    "base_url": "http://127.0.0.1:9",
                    "models": {"ready": {"capacity": {"context_tokens": 8192, "output_tokens": 1024}}}
                }
            }))?,
        )?;
        let agent = directory.path().join("agent.ts");
        std::fs::write(
            &agent,
            "import { model } from './choice.ts'; export default base => ({...base, model});",
        )?;
        let choice = directory.path().join("choice.ts");
        std::fs::write(&choice, "export const model = 'available/ready';")?;
        let args = Args::try_parse_from([
            "graphcoder".to_owned(),
            "--default-config".into(),
            "--workspace".into(),
            workspace.display().to_string(),
            "--host-state-directory".into(),
            directory.path().join("state").display().to_string(),
            "--workspace-mounting".into(),
            "physical".into(),
            "--catalog".into(),
            catalog.display().to_string(),
            "--home-file".into(),
            format!("agent.ts={}", agent.display()),
            "--home-file".into(),
            format!("choice.ts={}", choice.display()),
        ])?;
        let registry = registry(&args)?;
        assert!(registry.needs_credentials(registry.default_model())?);
        let prepared = LocalHarness::prepare(configure(&args, super::registry(&args)?)?).await?;
        let selected = prepared.model_selection(None).await?;
        assert_eq!(selected, "available/ready");
        assert!(!registry.needs_credentials(&selected)?);
        Ok(())
    }
}

fn credentials(args: &Args) -> Result<Credentials, Failure> {
    if args.environment_credentials {
        return Ok(Credentials::environment());
    }
    if args.default_config {
        let configured = args
            .credentials
            .as_deref()
            .map(|path| -> Result<Credentials, Failure> {
                let bytes = acyclic_harness::local::read_regular_file(path)?;
                serde_json::from_slice(&bytes)
                    .map_err(|_| "invalid provider credential file".into())
            })
            .transpose()?;
        return Ok(Credentials::explicit(
            configured
                .map(|credentials| credentials.api_keys)
                .unwrap_or_default(),
        ));
    }
    let credentials = args
        .credentials
        .as_deref()
        .map(Credentials::from_file)
        .transpose()?
        .unwrap_or_default();
    Ok(credentials)
}

pub(super) fn registry(args: &Args) -> Result<Registry, Failure> {
    let mut registry = Registry::new(credentials(args)?)?;
    if let Some(path) = &args.catalog {
        registry.load_catalog(path)?;
    }
    Ok(registry)
}

fn settings(args: &Args) -> Result<Option<SettingsConfig>, Failure> {
    if args.default_config {
        return Ok(None);
    }
    Ok(Some(match &args.settings_root {
        Some(root) => SettingsConfig::new(&args.workspace, root).codex_home(root.join(".codex")),
        None => SettingsConfig::new(
            &args.workspace,
            acyclic_harness::native_environment::home().ok_or(
                "user settings directory is unavailable; recovery: configure --settings-root",
            )?,
        ),
    }))
}

fn base_selection(
    args: &Args,
    registry: &Registry,
    model: Option<&str>,
) -> Result<String, Failure> {
    let role = acyclic_harness::templates::root()?.model;
    let configured = match model {
        Some(model) => Some(model.to_owned()),
        None => settings(args)?
            .map(|settings| settings.configured_model())
            .transpose()?
            .flatten(),
    };
    Ok(
        acyclic_harness::templates::resolve_model(role.as_deref(), configured.as_deref())
            .unwrap_or_else(|| registry.default_model())
            .to_owned(),
    )
}

async fn selection_from(
    args: &Args,
    registry: &Registry,
    model: Option<&str>,
) -> Result<String, Failure> {
    let base = base_selection(args, registry, model)?;
    let home = args
        .home_file
        .iter()
        .map(|(path, source)| {
            Ok((
                path.clone(),
                acyclic_harness::local::read_regular_file(source)?.into(),
            ))
        })
        .collect::<Result<std::collections::BTreeMap<_, _>, Failure>>()?;
    Ok(acyclic_harness::config::agent_from_home(&home, &base)
        .await?
        .model)
}

const KEYS: &str = "provider key unavailable; recovery: run graphcoder in a terminal to save a key in OS keychain service acyclic-harness, set MORPH_API_KEY / ANTHROPIC_API_KEY / OPENAI_API_KEY / OPENROUTER_API_KEY / GEMINI_API_KEY (or GOOGLE_API_KEY), or pass --credentials FILE with an api_keys provider map";

pub(super) async fn first_run(
    args: &mut Args,
    registry: &mut Registry,
) -> Result<Option<Tty>, Failure> {
    if args.environment_credentials
        || args.default_config
        || !onboarding::interactive()
        || args.input_format != InputFormat::Text
        || args.output_format != OutputFormat::Text
        || !matches!(
            args.command,
            None | Some(Command::Run { .. } | Command::Exec { .. })
        )
    {
        return Ok(None);
    }
    let base = base_selection(args, registry, args.model.as_deref())?;
    let prepared = LocalHarness::prepare(configure(args, self::registry(args)?)?).await?;
    let selected = prepared.model_selection(Some(&base)).await?;
    args.model = Some(base);
    if !registry.needs_credentials(&selected)? {
        return Ok(None);
    }
    let mut terminal = Tty::open()?;
    let mut input = onboarding::Input::new(&mut terminal)?;
    let role = acyclic_harness::templates::root()?.model;
    let selected =
        if let Some(role) = acyclic_harness::templates::resolve_model(role.as_deref(), None) {
            print!("GraphCoder first run: credentials for role model {role}\r\n");
            role.to_owned()
        } else {
            print!("GraphCoder first run: choose a provider\r\n");
            let providers: Vec<String> = registry
                .catalog()
                .iter()
                .filter(|(_, spec)| !spec.models.is_empty())
                .map(|(name, _)| name.clone())
                .collect();
            let index = input.choose("Provider", &providers)?;
            let provider = providers
                .get(index)
                .ok_or("provider selection unavailable")?;
            let spec = registry
                .catalog()
                .get(provider)
                .ok_or("provider selection unavailable")?;
            let models: Vec<String> = spec
                .models
                .keys()
                .map(|model| format!("{provider}/{model}"))
                .collect();
            let index = input.choose("Model", &models)?;
            let selected = models
                .get(index)
                .ok_or("model selection unavailable")?
                .clone();
            selected
        };
    let effective = prepared.model_selection(Some(&selected)).await?;
    let provider = effective
        .split_once('/')
        .ok_or("selected model has no provider")?
        .0;
    if registry.needs_credentials(&effective)? {
        if !Credentials::keychain_available() {
            return Err("OS keychain unavailable; recovery: unlock/start the OS credential service, or configure an environment key or --credentials FILE before starting graphcoder".into());
        }
        let key = input.read_secret("API key (hidden)")?;
        credentials(args)?.store_api_key_in_service(provider, &key)?;
        print!("Key saved in OS keychain.\r\n");
    }
    drop(input);
    // Discard cached missing-source observations after publishing a credential,
    // and admit the exact selection before making it the next launch's default.
    let refreshed = self::registry(args)?;
    refreshed.resolve(&effective)?;
    if let Some(settings) = settings(args)? {
        settings.set_model(&selected)?;
    }
    args.model = Some(selected);
    *registry = refreshed;
    Ok(Some(terminal))
}

pub(super) async fn doctor(args: &Args) -> Result<(), Failure> {
    let headless = !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal();
    let mut refusals = Vec::new();
    let mut notes = Vec::new();
    let registry = doctor_registry(args, &mut refusals)?;
    let selected = selection_from(args, &registry, args.model.as_deref()).await;
    let needs_key =
        doctor_provider(&registry, selected.as_deref(), &mut notes, &mut refusals).await;
    doctor_facilities(args, needs_key, headless, &mut notes, &mut refusals);
    let local = match configure(args, registry) {
        Ok(config) => LocalHarness::doctor(config).await,
        Err(_) => {
            refusals.push("local configuration: unavailable; recovery: repair workspace, approval policy and settings flags".into());
            Registry::new(Credentials::explicit(Default::default()))?
                .graphcoder(&args.workspace, args.model.clone())?
                .preflight()
        }
    };
    for error in local {
        let message = error.to_string();
        // Credential refusal already includes all three credential remedies.
        if needs_key && message.contains("API key is unavailable") {
            continue;
        }
        refusals.push(format!("{message}; recovery: correct the reported startup prerequisite and rerun graphcoder doctor"));
    }
    notes.push("filesystem mode: --workspace-mounting selects native mounts or physical directories; auto probes native availability".into());
    notes.push("shell and state directory: checked by shared native host preflight".into());
    let mut output = std::io::stdout().lock();
    if headless {
        serde_json::to_writer(
            &mut output,
            &serde_json::json!({
                "version": 1,
                "kind": "doctor",
                "mode": "headless",
                "ready": refusals.is_empty(),
                "checks": &notes,
                "refusals": &refusals,
            }),
        )?;
        writeln!(output)?;
    } else {
        for note in notes {
            writeln!(output, "check: {note}")?;
        }
        for refusal in &refusals {
            writeln!(output, "refused: {refusal}")?;
        }
        if refusals.is_empty() {
            writeln!(
                output,
                "ok: local prerequisites checked; state and signing authority unopened"
            )?;
        }
    }
    output.flush()?;
    if refusals.is_empty() {
        Ok(())
    } else {
        Err(Box::new(Exit::failed(format!(
            "{} startup refusal(s); a run with these flags does not start",
            refusals.len()
        ))))
    }
}

fn doctor_registry(args: &Args, refusals: &mut Vec<String>) -> Result<Registry, Failure> {
    let credentials = match credentials(args) {
        Ok(credentials) => credentials,
        Err(_) => {
            refusals.push("credentials file: cannot read or decode provider credentials; recovery: repair --credentials FILE (JSON api_keys provider map)".to_owned());
            Credentials::explicit(Default::default())
        }
    };
    let mut registry = Registry::new(credentials)?;
    if let Some(path) = &args.catalog
        && registry.load_catalog(path).is_err()
    {
        refusals
            .push("provider catalog: cannot load catalog; recovery: repair --catalog FILE".into());
    }
    Ok(registry)
}

async fn doctor_provider(
    registry: &Registry,
    selected: Result<&str, &Failure>,
    notes: &mut Vec<String>,
    refusals: &mut Vec<String>,
) -> bool {
    let mut needs_key = false;
    match selected {
        Ok(selected) => {
            match registry.needs_credentials(selected) {
                Ok(true) => { needs_key = true; refusals.push(KEYS.into()); }
                Ok(false) => notes.push("provider credentials: available or optional".into()),
                Err(_) => refusals.push("provider/model: unavailable; recovery: select a catalogued --model provider/model".into()),
            }
            match registry.check_reachability(selected).await {
                Ok(()) => notes.push("provider network: reachable (HTTP; no key sent)".into()),
                Err(error) => refusals.push(format!("{error}; recovery: check the configured provider endpoint and network")),
            }
        }
        Err(_) => refusals.push("model settings: unavailable; recovery: repair user config.toml or select --model provider/model".into()),
    }
    needs_key
}

fn doctor_facilities(
    args: &Args,
    needs_key: bool,
    headless: bool,
    notes: &mut Vec<String>,
    refusals: &mut Vec<String>,
) {
    if args.default_config {
        notes.push("keychain: native credential discovery disabled by --default-config".into());
    } else if Credentials::keychain_available() {
        notes.push("keychain: available (service acyclic-harness)".into());
    } else {
        let message = "keychain: unavailable; recovery: unlock/start the OS credential service, or use an environment key or --credentials FILE";
        if needs_key {
            refusals.push(message.into());
        } else {
            notes.push(format!("{message} (optional for this run)"));
        }
    }
    let terminal_required = !headless
        && args.prompt.is_none()
        && args.input_format == InputFormat::Text
        && args.output_format == OutputFormat::Text;
    if terminal_required
        && (!onboarding::interactive() || acyclic_harness::tui::term::Tty::open().is_err())
    {
        refusals.push("terminal: interactive terminal unavailable; recovery: use a VT capable terminal, or select -p TASK / JSONL for headless execution".into());
    } else {
        notes.push("terminal: ready for selected interactive/headless mode".into());
    }
}
