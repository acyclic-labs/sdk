//! Generate typed defaults and named bindings from the shipped declarative files.
use serde_json::Value;
use std::{env, fs, path::PathBuf};
fn variant(name: &str) -> String {
    name.split(['_', '-'])
        .map(|part| {
            let mut chars = part.chars();
            chars.next().unwrap().to_uppercase().collect::<String>() + chars.as_str()
        })
        .collect()
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo::rustc-check-cfg=cfg(acyclic_harness_tui_loom)");
    println!("cargo:rerun-if-changed=data");
    let out = PathBuf::from(env::var("OUT_DIR")?);
    let actions: Vec<Value> = serde_json::from_str(&fs::read_to_string("data/actions.json")?)?;
    let bindings: Value = serde_json::from_str(&fs::read_to_string("data/keymap.json")?)?;
    let binding = |chord: &str, action: &Value| {
        format!(
            "Binding {{ chord: {:?}, action: {}, help: {:?}, repeat: {} }},\n",
            chord,
            action["action"].as_str().unwrap(),
            action["help"].as_str().unwrap(),
            action["repeat"]
        )
    };
    let mut source = String::from(
        "/// Every bindable action and its generated help.\npub const ACTIONS: &[Binding<'static>] = &[\n",
    );
    for action in &actions {
        source.push_str(&binding(action["name"].as_str().unwrap(), action));
    }
    source.push_str("];\n/// Default bindings from data/keymap.json.\npub const KEYMAP: &[Binding<'static>] = &[\n");
    for (chord, name) in bindings["bindings"].as_object().unwrap() {
        let action = actions
            .iter()
            .find(|a| a["name"] == *name)
            .ok_or("unknown default action")?;
        source.push_str(&binding(chord, action));
    }
    source.push_str("];\n");
    fs::write(out.join("keymap.rs"), source)?;
    let defaults: Value = serde_json::from_str(&fs::read_to_string("data/settings.json")?)?;
    let mut source = String::from(
        "/// Default preferences from data/settings.json.\npub const DEFAULT_SETTINGS: UiSettings = UiSettings {\n",
    );
    for (key, value) in defaults.as_object().unwrap() {
        let value = if key == "notifications" {
            format!(
                "NotificationPolicy::{}",
                value
                    .as_str()
                    .unwrap()
                    .split('-')
                    .map(|s| {
                        let mut chars = s.chars();
                        chars.next().unwrap().to_uppercase().collect::<String>() + chars.as_str()
                    })
                    .collect::<String>()
            )
        } else {
            value.to_string()
        };
        source.push_str(&format!("{key}: {value},\n"));
    }
    source.push_str("};\nimpl UiSettings { /// Whether preferences obey the settings schema bounds.\npub fn valid(self) -> bool { ");
    let schema: Value = serde_json::from_str(&fs::read_to_string("data/settings.schema.json")?)?;
    let mut bounds = vec![];
    for (key, field) in schema["properties"].as_object().unwrap() {
        if field["type"] == "integer" {
            if field["minimum"] != 0 {
                bounds.push(format!("self.{key} >= {}", field["minimum"]));
            }
            bounds.push(format!("self.{key} <= {}", field["maximum"]));
        }
    }
    source.push_str(&bounds.join(" && "));
    source.push_str(" } }\n");
    fs::write(out.join("settings.rs"), source)?;

    let theme: Value = serde_json::from_str(&fs::read_to_string("data/theme.json")?)?;
    let mut roles = String::from(
        "/// Semantic UI and Markdown color role.\n#[derive(Clone, Copy, Debug, PartialEq, Eq)] #[repr(usize)] pub enum ThemeRole {\n",
    );
    let mut styles = String::from("const DEFAULT_THEME_STYLES: ThemeStyles = ThemeStyles([\n");
    for (role, style) in theme.as_object().unwrap() {
        let variant_name = variant(role);
        roles.push_str(&format!("/// {role} style.\n{variant_name},\n"));
        let mut expression = String::from("Style::new()");
        for (key, method) in [("foreground", "fg"), ("background", "bg")] {
            if let Some(color) = style[key].as_str() {
                let expression_color = if color.starts_with('#') {
                    let rgb = u32::from_str_radix(&color[1..], 16)?;
                    format!(
                        "Color::Rgb({}, {}, {})",
                        (rgb >> 16) & 255,
                        (rgb >> 8) & 255,
                        rgb & 255
                    )
                } else {
                    format!(
                        "Color::{}",
                        if color == "default" {
                            "Reset".to_owned()
                        } else {
                            variant(color)
                        }
                    )
                };
                expression.push_str(&format!(".{method}({expression_color})"));
            }
        }
        for (key, modifier) in [
            ("bold", "BOLD"),
            ("italic", "ITALIC"),
            ("dim", "DIM"),
            ("underline", "UNDERLINED"),
            ("reverse", "REVERSED"),
            ("strikethrough", "CROSSED_OUT"),
        ] {
            if style[key] == true {
                expression.push_str(&format!(".add_modifier(Modifier::{modifier})"));
            }
        }
        styles.push_str(&format!("{expression},\n"));
    }
    let count = theme.as_object().unwrap().len();
    roles.push_str("}\nimpl ThemeRole { /// All semantic roles.\npub const ALL: &[Self] = &[");
    for role in theme.as_object().unwrap().keys() {
        let variant_name = variant(role);
        roles.push_str(&format!("Self::{variant_name},"));
    }
    roles.push_str(
        "]; /// Declarative theme key.\npub fn name(self) -> &'static str { match self {",
    );
    for role in theme.as_object().unwrap().keys() {
        let variant_name = variant(role);
        roles.push_str(&format!("Self::{variant_name} => {role:?},"));
    }
    roles.push_str("} } }\n");
    roles.push_str(&format!(
        "/// Capability-resolved semantic styles.\n#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub struct ThemeStyles([Style; {count}]);\n"
    ));
    styles.push_str("]);\n");
    fs::write(out.join("theme.rs"), roles + &styles)?;
    Ok(())
}
