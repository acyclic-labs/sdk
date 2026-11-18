//! Colour names and syntax shared by schema validation and rendering.

/// The sixteen named ANSI colours accepted by the theme schema.
pub const NAMES: [&str; 16] = [
    "black",
    "red",
    "green",
    "yellow",
    "blue",
    "magenta",
    "cyan",
    "gray",
    "dark-gray",
    "light-red",
    "light-green",
    "light-yellow",
    "light-blue",
    "light-magenta",
    "light-cyan",
    "white",
];
/// Whether `text` is a colour the theme schema accepts.
pub fn valid_color(text: &str) -> bool {
    text == "default" || NAMES.contains(&text) || rgb(text).is_some()
}
/// Parse a `#rrggbb` colour.
pub fn rgb(text: &str) -> Option<[u8; 3]> {
    let hex = text.strip_prefix('#')?;
    if hex.len() != 6 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let [_, red, green, blue] = u32::from_str_radix(hex, 16).ok()?.to_be_bytes();
    Some([red, green, blue])
}
