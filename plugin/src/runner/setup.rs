//! CLI composition of shared provider, terminal and local-host preflights.
use super::*;
use acyclic_harness::tui::onboarding;
use acyclic_harness::tui::term::Tty;
use std::io::Write as _;

type Failure = Box<dyn std::error::Error>;

fn credentials(args: &Args) -> Result<Credentials, Failure> {
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

fn selection(args: &Args, registry: &Registry) -> Result<String, Failure> {
    if let Some(model) = &args.model {
        return Ok(model.clone());
    }
    Ok(settings(args)?
        .map(|settings| settings.configured_model())
        .transpose()?
        .flatten()
        .unwrap_or_else(|| registry.default_model().into()))
}

const KEYS: &str = "provider key unavailable; recovery: run graphcoder in a terminal to save a key in OS keychain service acyclic-harness, set ANTHROPIC_API_KEY / OPENAI_API_KEY / OPENROUTER_API_KEY / GEMINI_API_KEY (or GOOGLE_API_KEY), or pass --credentials FILE with an api_keys provider map";

pub(super) fn first_run(args: &mut Args, registry: &mut Registry) -> Result<Option<Tty>, Failure> {
    let selected = selection(args, registry)?;
    args.model = Some(selected.clone());
    if !registry.needs_credentials(&selected)? {
        return Ok(None);
    }
    if args.default_config
        || !onboarding::interactive()
        || args.input_format != InputFormat::Text
        || args.output_format != OutputFormat::Text
        || !matches!(
            args.command,
            None | Some(Command::Run | Command::Exec { .. })
        )
    {
        return Err(KEYS.into());
    }
    let mut terminal = Tty::open()?;
    print!("GraphCoder first run: choose a provider\r\n");
    let providers: Vec<String> = registry
        .catalog()
        .iter()
        .filter(|(_, spec)| !spec.models.is_empty())
        .map(|(name, _)| name.clone())
        .collect();
    let index = onboarding::choose(&mut terminal, "Provider", &providers)?;
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
    let requires_key =
        registry.needs_credentials(models.first().ok_or("provider has no catalogued model")?)?;
    if requires_key {
        if !Credentials::keychain_available() {
            return Err("OS keychain unavailable; recovery: unlock/start the OS credential service, or configure an environment key or --credentials FILE before starting graphcoder".into());
        }
        let key = onboarding::read(&mut terminal, "API key (hidden)", true)?;
        Credentials::store_api_key(provider, &key)?;
        print!("Key saved in OS keychain service acyclic-harness.\r\n");
    }
    let index = onboarding::choose(&mut terminal, "Model", &models)?;
    let selected = models
        .get(index)
        .ok_or("model selection unavailable")?
        .clone();
    if let Some(settings) = settings(args)? {
        settings.set_model(&selected)?;
    }
    args.model = Some(selected);
    // Rebuild the source snapshot only after publishing the new credential.
    *registry = self::registry(args)?;
    Ok(Some(terminal))
}

pub(super) async fn doctor(args: &Args) -> Result<(), Failure> {
    let mut refusals = Vec::new();
    let mut notes = Vec::new();
    let registry = doctor_registry(args, &mut refusals)?;
    let selected = selection(args, &registry);
    let needs_key =
        doctor_provider(&registry, selected.as_deref(), &mut notes, &mut refusals).await;
    doctor_facilities(args, needs_key, &mut notes, &mut refusals);
    let local = match configure(args, registry) {
        Ok(config) => LocalHarness::doctor(config).await,
        Err(_) => {
            refusals.push("local configuration: unavailable; recovery: repair workspace, approval policy and settings flags".into());
            Registry::new(Credentials::explicit(Default::default()))?
                .graphcoder(&args.workspace, args.model.clone())
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
    notes.push("filesystem mode: native mounts (FUSE on Linux); no-FUSE plain-directory execution is not available in this build".into());
    notes.push("shell and state directory: checked by shared native host preflight".into());
    let mut output = std::io::stdout().lock();
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
    let terminal_required = args.prompt.is_none()
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
