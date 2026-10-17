//! Generate typed defaults and named bindings from the shipped declarative files.
use serde_json::{Map, Value};
use std::{
    env,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
};

type BuildResult<T> = Result<T, Box<dyn std::error::Error>>;

fn read_json(path: &str) -> BuildResult<Value> {
    Ok(serde_json::from_str(&fs::read_to_string(path)?)?)
}

fn object<'a>(value: &'a Value, what: &'static str) -> BuildResult<&'a Map<String, Value>> {
    Ok(value.as_object().ok_or(what)?)
}

fn text<'a>(value: &'a Value, what: &'static str) -> BuildResult<&'a str> {
    Ok(value.as_str().ok_or(what)?)
}

/// Uppercases the first character of a non-empty name segment.
fn capitalized(part: &str) -> BuildResult<String> {
    let mut chars = part.chars();
    let first = chars.next().ok_or("empty name segment")?;
    Ok(first.to_uppercase().collect::<String>() + chars.as_str())
}

fn variant(name: &str) -> BuildResult<String> {
    name.split(['_', '-']).map(capitalized).collect()
}

fn main() -> BuildResult<()> {
    println!("cargo::rustc-check-cfg=cfg(acyclic_harness_tui_loom)");
    println!("cargo:rerun-if-changed=data");
    let out = PathBuf::from(env::var("OUT_DIR")?);
    keymap(&out)?;
    settings(&out)?;
    theme(&out)
}

fn keymap(out: &Path) -> BuildResult<()> {
    let actions = read_json("data/actions.json")?;
    let actions = actions.as_array().ok_or("actions must be an array")?;
    let bindings: Value = read_json("data/keymap.json")?;
    let binding = |chord: &str, action: &Value| -> BuildResult<String> {
        Ok(format!(
            "Binding {{ chord: {chord:?}, action: {}, help: {:?}, repeat: {} }},\n",
            text(&action["action"], "action id")?,
            text(&action["help"], "action help")?,
            action["repeat"]
        ))
    };
    let mut source = String::from(
        "/// Every bindable action and its generated help.\npub const ACTIONS: &[Binding<'static>] = &[\n",
    );
    for action in actions {
        source.push_str(&binding(text(&action["name"], "action name")?, action)?);
    }
    source.push_str("];\n/// Default bindings from data/keymap.json.\npub const KEYMAP: &[Binding<'static>] = &[\n");
    for (chord, name) in bindings
        .get("bindings")
        .and_then(Value::as_object)
        .ok_or("keymap bindings")?
    {
        let action = actions
            .iter()
            .find(|a| a["name"] == *name)
            .ok_or("unknown default action")?;
        source.push_str(&binding(chord, action)?);
    }
    source.push_str("];\n");
    fs::write(out.join("keymap.rs"), source)?;
    Ok(())
}

fn settings(out: &Path) -> BuildResult<()> {
    let defaults: Value = read_json("data/settings.json")?;
    let mut source = String::from(
        "/// Default preferences from data/settings.json.\npub const DEFAULT_SETTINGS: UiSettings = UiSettings {\n",
    );
    for (key, value) in object(&defaults, "settings")? {
        let value = if key == "notifications" {
            let policy = text(value, "notification policy")?
                .split('-')
                .map(capitalized)
                .collect::<BuildResult<String>>()?;
            format!("NotificationPolicy::{policy}")
        } else {
            value.to_string()
        };
        writeln!(source, "{key}: {value},")?;
    }
    source.push_str("};\nimpl UiSettings { /// Whether preferences obey the settings schema bounds.\npub fn valid(self) -> bool { ");
    let schema: Value = read_json("data/settings.schema.json")?;
    let mut bounds = vec![];
    for (key, field) in schema
        .get("properties")
        .and_then(Value::as_object)
        .ok_or("settings schema properties")?
    {
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
    Ok(())
}

/// The ratatui expression for one declared theme style.
fn style_expression(style: &Value) -> BuildResult<String> {
    let mut expression = String::from("Style::new()");
    for (key, method) in [("foreground", "fg"), ("background", "bg")] {
        if let Some(color) = style[key].as_str() {
            let expression_color = if let Some(hex) = color.strip_prefix('#') {
                let rgb = u32::from_str_radix(hex, 16)?;
                format!(
                    "Color::Rgb({}, {}, {})",
                    (rgb >> 16) & 255,
                    (rgb >> 8) & 255,
                    rgb & 255
                )
            } else if color == "default" {
                "Color::Reset".to_owned()
            } else {
                format!("Color::{}", variant(color)?)
            };
            write!(expression, ".{method}({expression_color})")?;
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
            write!(expression, ".add_modifier(Modifier::{modifier})")?;
        }
    }
    Ok(expression)
}

fn theme(out: &Path) -> BuildResult<()> {
    let theme: Value = read_json("data/theme.json")?;
    let theme = object(&theme, "theme")?;
    let mut roles = String::from(
        "/// Semantic UI and Markdown color role.\n#[derive(Clone, Copy, Debug, PartialEq, Eq)] #[repr(usize)] pub enum ThemeRole {\n",
    );
    let mut styles = String::from("const DEFAULT_THEME_STYLES: ThemeStyles = ThemeStyles([\n");
    for (role, style) in theme {
        let variant_name = variant(role)?;
        writeln!(roles, "/// `{role}` style.\n{variant_name},")?;
        writeln!(styles, "{},", style_expression(style)?)?;
    }
    let count = theme.len();
    roles.push_str("}\nimpl ThemeRole { /// All semantic roles.\npub const ALL: &[Self] = &[");
    for role in theme.keys() {
        write!(roles, "Self::{},", variant(role)?)?;
    }
    roles.push_str(
        "]; /// Declarative theme key.\npub fn name(self) -> &'static str { match self {",
    );
    for role in theme.keys() {
        write!(roles, "Self::{} => {role:?},", variant(role)?)?;
    }
    roles.push_str("} } }\n");
    write!(
        roles,
        "/// Capability-resolved semantic styles.\n#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub struct ThemeStyles([Style; {count}]);\n"
    )?;
    styles.push_str("]);\n");
    fs::write(out.join("theme.rs"), roles + &styles)?;
    Ok(())
}
