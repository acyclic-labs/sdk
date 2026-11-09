//! The theme is a handful of semantic colour tokens in a dark and a light
//! set. Widgets name roles, each a token plus text attributes, so a theme
//! file never restates the UI's structure: overriding a colour is one key.
use crate::term::{Capabilities, ColourDepth};
use ratatui::style::{Color, Modifier, Style};
use serde_json::Value;
use std::sync::LazyLock;

include!(concat!(env!("OUT_DIR"), "/theme.rs"));

/// What a widget draws: a token and the attributes it adds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThemeRole {
    /// Ordinary text.
    Text,
    /// Panel borders.
    Border,
    /// The prompt while it edits a sent message.
    Editing,
    /// Selected text.
    Selection,
    /// Placeholder and secondary text.
    Placeholder,
    /// A completion popup.
    Popup,
    /// The selected completion.
    PopupSelected,
    /// A configuration diagnostic.
    Diagnostic,
    /// A heading.
    Heading,
}

impl ThemeRole {
    const fn parts(self) -> (Option<Token>, Modifier) {
        match self {
            Self::Text | Self::Popup => (Some(Token::Fg), Modifier::empty()),
            Self::Border | Self::Placeholder => (Some(Token::Muted), Modifier::empty()),
            Self::Editing => (Some(Token::Attention), Modifier::empty()),
            Self::Selection | Self::PopupSelected => (None, Modifier::REVERSED),
            Self::Diagnostic => (Some(Token::Error), Modifier::BOLD),
            Self::Heading => (Some(Token::Accent), Modifier::BOLD),
        }
    }
}

/// Each token's style for one terminal: its colour at the terminal's depth.
/// Without colour, `muted` is dim text and the rest are plain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ThemeStyles([Style; Token::ALL.len()]);

/// The shipped dark set at full colour, before a terminal is probed.
static SHIPPED: LazyLock<ThemeStyles> = LazyLock::new(|| {
    resolve(
        &serde_json::from_str(super::ConfigFile::Theme.defaults()).unwrap_or_default(),
        &Capabilities {
            colour: ColourDepth::TrueColour,
            ..Capabilities::default()
        },
    )
});

impl Default for ThemeStyles {
    fn default() -> Self {
        *SHIPPED
    }
}

impl ThemeStyles {
    /// A token's resolved style.
    pub fn token(&self, token: Token) -> Style {
        self.0.get(token as usize).copied().unwrap_or_default()
    }

    /// A role's resolved style.
    pub fn style(&self, role: ThemeRole) -> Style {
        let (token, modifier) = role.parts();
        token
            .map_or_else(Style::new, |token| self.token(token))
            .add_modifier(modifier)
    }
}

/// A role's shipped style before a terminal profile is available.
pub fn default_style(role: ThemeRole) -> Style {
    SHIPPED.style(role)
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
/// The six ANSI hues by 60° sector from red, each normal then bright.
const HUES: [[Color; 2]; 6] = [
    [Color::Red, Color::LightRed],
    [Color::Yellow, Color::LightYellow],
    [Color::Green, Color::LightGreen],
    [Color::Cyan, Color::LightCyan],
    [Color::Blue, Color::LightBlue],
    [Color::Magenta, Color::LightMagenta],
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
/// The ANSI colour of the same hue. Nearest-RGB matching would turn most
/// mid-tone colours grey, so greys go by lightness and other colours by
/// hue: bright variants on a dark background, normal ones on a light one.
fn ansi16([red, green, blue]: [u8; 3], light: bool) -> Color {
    let max = red.max(green).max(blue);
    let chroma = i32::from(max - red.min(green).min(blue));
    if chroma < 32 || chroma * 4 < i32::from(max) {
        return match max {
            0..64 => Color::Black,
            64..160 => Color::DarkGray,
            160..224 => Color::Gray,
            _ => Color::White,
        };
    }
    let [r, g, b] = [red, green, blue].map(i32::from);
    let hue = if max == red {
        (60 * (g - b) / chroma).rem_euclid(360)
    } else if max == green {
        60 * (b - r) / chroma + 120
    } else {
        60 * (r - g) / chroma + 240
    };
    let sector = usize::try_from((hue + 30) / 60 % 6).unwrap_or_default();
    HUES.get(sector).map_or(
        Color::Reset,
        |[normal, bright]| {
            if light { *normal } else { *bright }
        },
    )
}
fn color(text: &str, depth: ColourDepth, light: bool) -> Color {
    if depth == ColourDepth::Monochrome || text == "default" {
        return Color::Reset;
    }
    if let Some((_, named)) = NAMES.iter().zip(COLORS).find(|(name, _)| **name == text) {
        return named;
    }
    let Some(rgb) = rgb(text) else {
        return Color::Reset;
    };
    match depth {
        ColourDepth::TrueColour => {
            let [red, green, blue] = rgb;
            Color::Rgb(red, green, blue)
        }
        ColourDepth::Indexed256 => Color::Indexed(
            (0..=u8::MAX)
                .min_by_key(|i| distance(rgb, palette(*i)))
                .unwrap_or(0),
        ),
        ColourDepth::Ansi16 | ColourDepth::Monochrome => ansi16(rgb, light),
    }
}

/// Whether the light set applies: the file's `mode`, or under `auto`, a
/// probed background brighter than mid grey (Rec. 709 luma).
fn is_light(value: &Value, background: Option<[u8; 3]>) -> bool {
    match value["mode"].as_str() {
        Some("light") => true,
        Some("dark") => false,
        _ => background.is_some_and(|[red, green, blue]| {
            2126 * u32::from(red) + 7152 * u32::from(green) + 722 * u32::from(blue)
                > 10_000 * 255 / 2
        }),
    }
}

/// Resolve every token of the effective theme for gc-terminal's profile.
pub(super) fn resolve(value: &Value, profile: &Capabilities) -> ThemeStyles {
    let light = is_light(value, profile.background);
    let tokens = &value[if light { "light" } else { "dark" }];
    ThemeStyles(Token::ALL.map(|token| {
        let text = tokens[token.name()].as_str().unwrap_or("default");
        let style = Style::new().fg(color(text, profile.colour, light));
        if profile.colour == ColourDepth::Monochrome && token == Token::Muted {
            style.add_modifier(Modifier::DIM)
        } else {
            style
        }
    }))
}
