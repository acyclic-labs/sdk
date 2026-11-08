//! Generated plugin inputs reach the production loader and existing evaluator.
use super::*;
use crate::plugins::{self, Enabled};
use std::sync::atomic::AtomicUsize;

const MANIFEST: &str = "plugins/p/plugin.ts";
const INPUT: &str = "sys/plugin.json";
const SHELL: &str = "plugins/p/hooks/before-tool.sh";
const POWERSHELL: &str = "plugins/p/hooks/before-tool.ps1";
const TYPESCRIPT: &str = "plugins/p/hooks/before-tool.ts";

struct Observed {
    files: BTreeMap<&'static str, Vec<u8>>,
    reads: AtomicUsize,
    failure: Option<(usize, bool)>,
}

impl Tree for Observed {
    fn read<'a>(
        &'a self,
        path: &'a str,
    ) -> BoxFuture<'a, Result<Option<acyclic_stream::OwnedBytes>>> {
        Box::pin(async move {
            let read = self.reads.fetch_add(1, Ordering::Relaxed);
            if let Some((failed, invalid)) = self.failure
                && read == failed
            {
                return Err(if invalid {
                    Error::Invalid("original plugin read refusal".into())
                } else {
                    Error::Storage("original plugin read failure".into())
                });
            }
            Ok(self
                .files
                .get(path)
                .cloned()
                .map(acyclic_stream::OwnedBytes::from))
        })
    }
}

fn tree(files: BTreeMap<&'static str, Vec<u8>>) -> Observed {
    Observed {
        files,
        reads: AtomicUsize::new(0),
        failure: None,
    }
}

fn rules(enabled: &Enabled) -> std::result::Result<Value, String> {
    serde_json::to_value(&enabled.rules).map_err(|error| error.to_string())
}

pub(super) async fn check(bytes: &[u8]) -> std::result::Result<(), String> {
    let layers = [Layer {
        name: "home",
        generation: "original",
        bytes: Some(b"export default {p: {enabled: true}};"),
    }];
    let manifest = b"import defaults from 'sys/plugin.json'; export default {name:'p', description:'generated', defaults};";
    let settings = json!({"original": bytes});
    let input = serde_json::to_vec(&settings).map_err(|error| error.to_string())?;
    let script = format!("echo {}", blake3::hash(bytes));
    let valid = BTreeMap::from([(MANIFEST, manifest.to_vec()), (INPUT, input)]);
    let mut identities = Vec::new();
    for (files, expected_reads, admitted) in [
        (BTreeMap::new(), 1, false),
        (BTreeMap::from([(MANIFEST, Vec::new())]), 1, false),
        (BTreeMap::from([(MANIFEST, manifest.to_vec())]), 2, false),
        (
            BTreeMap::from([(MANIFEST, manifest.to_vec()), (INPUT, Vec::new())]),
            2,
            false,
        ),
        (valid.clone(), 20, true),
        (
            valid
                .clone()
                .into_iter()
                .chain([(SHELL, script.as_bytes().to_vec())])
                .collect(),
            20,
            true,
        ),
        (
            valid
                .clone()
                .into_iter()
                .chain([(POWERSHELL, script.as_bytes().to_vec())])
                .collect(),
            20,
            true,
        ),
        (
            valid
                .clone()
                .into_iter()
                .chain([(TYPESCRIPT, Vec::new())])
                .collect(),
            5,
            false,
        ),
        (
            valid
                .clone()
                .into_iter()
                .chain([(
                    "plugins/p/hooks/before-model.sh",
                    script.as_bytes().to_vec(),
                )])
                .collect(),
            8,
            false,
        ),
        (
            valid
                .clone()
                .into_iter()
                .chain([(SHELL, [vec![255], bytes.to_vec()].concat())])
                .collect(),
            4,
            false,
        ),
        (
            valid
                .clone()
                .into_iter()
                .chain([(SHELL, [vec![254], bytes.to_vec()].concat())])
                .collect(),
            4,
            false,
        ),
    ] {
        let observed = tree(files);
        let reads = RETAINED_READS.get();
        let roots = RETAINED_ROOTS.get();
        let plain = plugins::load(&observed, &layers, "original").await;
        if RETAINED_READS.get() != reads || RETAINED_ROOTS.get() != roots {
            return Err("plain plugin load retained discarded module evidence".into());
        }
        if observed.reads.swap(0, Ordering::Relaxed) != expected_reads {
            return Err("plain plugin load repeated an original file read".into());
        }
        let decision = plugins::load_decision(&observed, &layers, "original")
            .await
            .map_err(|error| error.to_string())?;
        let identity = decision.identity();
        if identities.contains(&identity) {
            return Err("plugin decision lost an original manifest or hook observation".into());
        }
        identities.push(identity);
        if observed.reads.load(Ordering::Relaxed) != expected_reads {
            return Err("plugin decision repeated an original file read".into());
        }
        let decision = decision.into_result();
        match (&plain, &decision) {
            (Ok(plain), Ok(retained))
                if plain.names == retained.names && rules(plain)? == rules(retained)? => {}
            (Err(plain), Err(retained)) if plain.to_string() == retained.to_string() => {}
            _ => return Err("plain and retained plugin decisions diverged".into()),
        }
        match decision {
            Ok(enabled) if admitted => {
                if enabled.names != ["p"] {
                    return Err("plugin selection changed its original names".into());
                }
                let rules = rules(&enabled)?;
                if let Some((form, _)) = [(SHELL, "sh"), (POWERSHELL, "ps1")]
                    .into_iter()
                    .find(|(path, _)| observed.files.contains_key(path))
                {
                    let hook = &rules[0]["hooks"][0]["run"];
                    if hook["instance"] != "original:plugins.ts#p:hooks/before-tool"
                        || hook[if form == SHELL { "sh" } else { "ps1" }] != script
                        || hook["config"]
                            .as_str()
                            .and_then(|text| serde_json::from_str::<Value>(text).ok())
                            != Some(settings.clone())
                    {
                        return Err("plugin decision changed its original hook or settings".into());
                    }
                } else if rules != json!([]) {
                    return Err("absent plugin hooks created a rule".into());
                }
            }
            Err(Error::Invalid(_)) if !admitted => {}
            other => return Err(format!("plugin semantic decision changed: {other:?}")),
        }
    }
    let observed = tree(valid.clone());
    let original = plugins::load_decision(&observed, &layers, "original")
        .await
        .map_err(|error| error.to_string())?;
    let repeat = plugins::load_decision(&observed, &layers, "original")
        .await
        .map_err(|error| error.to_string())?;
    if original.identity() != repeat.identity() {
        return Err("unchanged plugin inputs changed their decision identity".into());
    }
    let changed = plugins::load_decision(&observed, &layers, "changed")
        .await
        .map_err(|error| error.to_string())?;
    if original.identity() == changed.identity() {
        return Err("plugin decision discarded its original enabler".into());
    }
    let original = original.into_result().map_err(|error| error.to_string())?;
    let legacy = plugins::load(&observed, &layers, "original")
        .await
        .map_err(|error| error.to_string())?;
    if original.names != legacy.names || rules(&original)? != rules(&legacy)? {
        return Err("plugin load diverged from its original decision".into());
    }
    // Fail the actual read producer, including at an originally absent hook.
    // Error::Invalid from that producer is still operational, never cached.
    for invalid in [false, true] {
        for failed in 0..20 {
            let observed = Observed {
                failure: Some((failed, invalid)),
                ..tree(valid.clone())
            };
            match plugins::load_decision(&observed, &layers, "original").await {
                Err(Error::Invalid(reason))
                    if invalid && reason == "original plugin read refusal" => {}
                Err(Error::Storage(reason))
                    if !invalid && reason == "original plugin read failure" => {}
                _ => return Err("plugin decision cached an operational read failure".into()),
            }
        }
    }
    // The shared layered fold has the same producer/error boundary before
    // the enabled-plugin loader is entered, with no repeated dependency read.
    let layers = [Layer {
        name: "home",
        generation: "original",
        bytes: Some(b"import input from 'sys/plugin.json'; export default input.enabled ?? {};"),
    }];
    for failure in [None, Some((0, false)), Some((0, true))] {
        let observed = Observed {
            failure,
            ..tree(valid.clone())
        };
        let result = plugins::load_decision(&observed, &layers, "original").await;
        if observed.reads.load(Ordering::Relaxed) != 1 {
            return Err("plugin composition repeated its original dependency read".into());
        }
        match (failure, result) {
            (None, Ok(decision)) => {
                let enabled = decision.into_result().map_err(|error| error.to_string())?;
                if !enabled.names.is_empty() || rules(&enabled)? != json!([]) {
                    return Err("empty plugin selection created a plugin".into());
                }
            }
            (Some((_, false)), Err(Error::Storage(reason)))
                if reason == "original plugin read failure" => {}
            (Some((_, true)), Err(Error::Invalid(reason)))
                if reason == "original plugin read refusal" => {}
            _ => return Err("plugin composition cached an operational read failure".into()),
        }
    }
    Ok(())
}
