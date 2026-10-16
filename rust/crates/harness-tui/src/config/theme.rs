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
        self.0[role as usize]
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
    let value = u32::from_str_radix(hex, 16).ok()?;
    Some([(value >> 16) as u8, (value >> 8) as u8, value as u8])
}
fn palette(index: usize) -> [u8; 3] {
    if index < 16 {
        ANSI[index]
    } else if index < 232 {
        let x = index - 16;
        let component = |c| if c == 0 { 0 } else { (55 + 40 * c) as u8 };
        [component(x / 36), component(x / 6 % 6), component(x % 6)]
    } else {
        [8 + 10 * (index - 232) as u8; 3]
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
    if let Some(index) = NAMES.iter().position(|name| *name == text) {
        return COLORS[index];
    }
    let Some(rgb) = rgb(text) else {
        return Color::Reset;
    };
    if depth == ColourDepth::TrueColour {
        return Color::Rgb(rgb[0], rgb[1], rgb[2]);
    }
    let count = if depth == ColourDepth::Indexed256 {
        256
    } else {
        16
    };
    let index = (0..count)
        .min_by_key(|i| distance(rgb, palette(*i)))
        .unwrap_or(0);
    if count == 16 {
        COLORS[index]
    } else {
        Color::Indexed(index as u8)
    }
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
        result.0[role as usize] = style;
    }
    result
}
