const NAMES: [&str; 16] = [
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

/// An admitted colour in a theme file, before terminal projection.
pub enum ThemeColor {
    /// The terminal's default colour.
    Default,
    /// Index into the standard 16-colour ANSI palette.
    Ansi(usize),
    /// A six-digit RGB colour.
    Rgb([u8; 3]),
}

impl ThemeColor {
    /// Parse a named colour, `default`, or a six-digit hexadecimal RGB value.
    pub fn parse(text: &str) -> Option<Self> {
        if text == "default" {
            return Some(Self::Default);
        }
        if let Some(index) = NAMES.iter().position(|name| *name == text) {
            return Some(Self::Ansi(index));
        }
        let hex = text.strip_prefix('#')?;
        if hex.len() != 6 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        let [_, red, green, blue] = u32::from_str_radix(hex, 16).ok()?.to_be_bytes();
        Some(Self::Rgb([red, green, blue]))
    }
}
