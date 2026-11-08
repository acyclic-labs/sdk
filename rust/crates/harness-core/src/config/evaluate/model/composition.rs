//! The existing oracle checks the original fold's refused and disabled cuts.
use super::*;

pub(super) async fn check(bytes: &[u8]) -> std::result::Result<(), String> {
    let base = json!({"base": bytes});
    let source = format!(
        "export default {};",
        serde_json::to_string(&base).map_err(|e| e.to_string())?
    );
    let missing = b"import input from 'sys/input.json'; export default input;";
    let tree = Files {
        input: None,
        cycle: false,
    };
    let mut layers = [
        Layer {
            name: "seed",
            generation: "seed-1",
            bytes: Some(source.as_bytes()),
        },
        Layer {
            name: "scope",
            generation: "scope-1",
            bytes: None,
        },
        Layer {
            name: "home",
            generation: "home-1",
            bytes: Some(missing),
        },
        Layer {
            name: "after",
            generation: "after-1",
            bytes: Some(source.as_bytes()),
        },
    ];
    let cut = compose_with_evidence("agent.ts", &layers, &tree, base.clone())
        .await
        .map_err(|e| e.to_string())?;
    if cut.layers().len() != layers.len()
        || cut.layers().iter().zip(layers).any(|(actual, expected)| {
            actual.name != expected.name || actual.generation != expected.generation
        })
        || cut.layers().get(1).map(|input| input.state) != Some(LayerState::Absent)
    {
        return Err("composition discarded an original layer observation".into());
    }
    let attempted = cut.modules();
    if !matches!(cut.result(), Err(Error::Invalid(_)))
        || attempted.len() != 2
        || attempted.first().map(|module| module.layer) != Some(0)
        || attempted.get(1).map(|module| module.layer) != Some(2)
        || attempted.get(1).map(|module| module.reads.as_slice())
            != Some(
                [
                    ModuleRead {
                        path: "agent.ts".into(),
                        digest: Some(blake3::hash(missing).into()),
                    },
                    ModuleRead {
                        path: "sys/input.json".into(),
                        digest: None,
                    },
                ]
                .as_slice(),
            )
    {
        return Err("composition discarded its original refused module cut".into());
    }
    let identity = cut.identity();
    layers[1].generation = "scope-2";
    let changed = compose_with_evidence("agent.ts", &layers, &tree, base.clone())
        .await
        .map_err(|e| e.to_string())?;
    if changed.identity() == identity {
        return Err("composition identity discarded its original layer generations".into());
    }
    // A higher off marker removes even an invalid lower root. The absent
    // remaining layer still contributes its generation and original absence.
    layers[0].bytes = Some(b"export default {");
    layers[2].bytes = Some(b"off: true");
    layers[3].bytes = None;
    let cut = compose_with_evidence("agent.ts", &layers, &tree, base.clone())
        .await
        .map_err(|e| e.to_string())?;
    match cut.result() {
        Ok((value, chain)) if value == &base && chain.is_empty() && cut.modules().is_empty() => {}
        _ => return Err("disabled composition did not preserve its original base".into()),
    }
    if cut.layers().get(2).map(|input| input.state) != Some(LayerState::Off) {
        return Err("composition discarded its original off observation".into());
    }
    let changed = compose_with_evidence("agent.ts", &layers, &tree, Value::Null)
        .await
        .map_err(|e| e.to_string())?;
    if changed.identity() == cut.identity() {
        return Err("empty composition identity discarded its original base".into());
    }
    Ok(())
}
