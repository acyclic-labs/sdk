//! Reject collisions in the shipped registry when its definitions are built.
#[path = "src/client/commands/name_index.rs"]
mod name_index;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=src/client/commands.json");
    println!("cargo:rerun-if-changed=src/client/commands/name_index.rs");
    let commands: Vec<serde_json::Value> =
        serde_json::from_slice(include_bytes!("src/client/commands.json"))?;
    let mut names = name_index::NameIndex::default();
    for command in commands {
        let name = command["name"]
            .as_str()
            .ok_or("built-in command name must be a string")?;
        let aliases = command["aliases"].as_array().into_iter().flatten();
        for label in std::iter::once(Ok(name)).chain(aliases.map(|alias| {
            alias
                .as_str()
                .ok_or("built-in command alias must be a string")
        })) {
            let label = label?;
            names.insert(label, name)?;
            if names.get(label) != Some(name) {
                return Err("built-in command lost its registered owner".into());
            }
        }
    }
    Ok(())
}
