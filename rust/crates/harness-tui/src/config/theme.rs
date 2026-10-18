use crate::term::{Capabilities, ColourDepth};
use ratatui::style::{Color, Modifier, Style};
use serde_json::Value;

include!(concat!(env!("OUT_DIR"), "/theme.rs"));
impl Default for ThemeStyles {
    fn default() -> Self {
        DEFAULT_THEME_STYLES
    }
}
impl ThemeStyles {
    /// Resolved style for a semantic role.
    pub fn style(&self, role: ThemeRole) -> Style {
        self.0.get(role as usize).copied().unwrap_or_default()
    }
}
/// Shipped style before a terminal profile is available.
pub fn default_style(role: ThemeRole) -> Style {
    DEFAULT_THEME_STYLES.style(role)
}

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
const ANSI: [[u8; 3]; 16] = [
    [0, 0, 0],
    [128, 0, 0],
    [0, 128, 0],
    [128, 128, 0],
    [0, 0, 128],
    [128, 0, 128],
    [0, 128, 128],
    [192, 192, 192],
    [128, 128, 128],
    [255, 0, 0],
    [0, 255, 0],
    [255, 255, 0],
    [0, 0, 255],
    [255, 0, 255],
    [0, 255, 255],
    [255, 255, 255],
];
const COLORS: [Color; 16] = [
    Color::Black,
    Color::Red,
    Color::Green,
    Color::Yellow,
    Color::Blue,
    Color::Magenta,
    Color::Cyan,
    Color::Gray,
    Color::DarkGray,
    Color::LightRed,
    Color::LightGreen,
    Color::LightYellow,
    Color::LightBlue,
    Color::LightMagenta,
    Color::LightCyan,
    Color::White,
];

pub(super) fn valid_color(text: &str) -> bool {
    text == "default" || NAMES.contains(&text) || rgb(text).is_some()
}
fn rgb(text: &str) -> Option<[u8; 3]> {
    let hex = text.strip_prefix('#')?;
    if hex.len() != 6 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let [_, red, green, blue] = u32::from_str_radix(hex, 16).ok()?.to_be_bytes();
    Some([red, green, blue])
}
/// The xterm 256-colour palette entry for `index`.
fn palette(index: u8) -> [u8; 3] {
    if let Some(ansi) = ANSI.get(usize::from(index)) {
        *ansi
    } else if index < 232 {
        let x = index - 16;
        let component = |c: u8| if c == 0 { 0 } else { 55 + 40 * c };
        [component(x / 36), component(x / 6 % 6), component(x % 6)]
    } else {
        [8 + 10 * (index - 232); 3]
    }
}
fn distance(a: [u8; 3], b: [u8; 3]) -> u32 {
    a.into_iter()
        .zip(b)
        .map(|(a, b)| (i32::from(a) - i32::from(b)).unsigned_abs().pow(2))
        .sum()
}
fn color(text: &str, depth: ColourDepth) -> Color {
    if depth == ColourDepth::Monochrome || text == "default" {
        return Color::Reset;
    }
    if let Some((_, named)) = NAMES.iter().zip(COLORS).find(|(name, _)| **name == text) {
        return named;
    }
    let Some(rgb) = rgb(text) else {
        return Color::Reset;
    };
    if depth == ColourDepth::TrueColour {
        let [red, green, blue] = rgb;
        return Color::Rgb(red, green, blue);
    }
    if depth == ColourDepth::Indexed256 {
        let index = (0..=u8::MAX)
            .min_by_key(|i| distance(rgb, palette(*i)))
            .unwrap_or(0);
        return Color::Indexed(index);
    }
    NAMES
        .iter()
        .zip(COLORS)
        .zip(0..)
        .min_by_key(|(_, index)| distance(rgb, palette(*index)))
        .map_or(Color::Reset, |((_, named), _)| named)
}

/// Resolve semantic UI/Markdown roles using gc-terminal's single detected profile.
pub(super) fn resolve(value: &Value, profile: &Capabilities) -> ThemeStyles {
    let mut result = ThemeStyles::default();
    for &role in ThemeRole::ALL {
        let v = &value[role.name()];
        let mut style = Style::new();
        if let Some(fg) = v["foreground"].as_str() {
            style = style.fg(color(fg, profile.colour));
        }
        if let Some(bg) = v["background"].as_str() {
            style = style.bg(color(bg, profile.colour));
        }
        for (key, modifier) in [
            ("bold", Modifier::BOLD),
            ("italic", Modifier::ITALIC),
            ("dim", Modifier::DIM),
            ("underline", Modifier::UNDERLINED),
            ("reverse", Modifier::REVERSED),
            ("strikethrough", Modifier::CROSSED_OUT),
        ] {
            if v[key] == true {
                style = style.add_modifier(modifier);
            }
        }
        if let Some(slot) = result.0.get_mut(role as usize) {
            *slot = style;
        }
    }
    result
}
