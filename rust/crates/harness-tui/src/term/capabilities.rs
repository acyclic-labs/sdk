use std::collections::BTreeMap;

/// Colour encoding selected once at startup.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum ColourDepth {
    /// No SGR colour.
    Monochrome,
    /// ANSI palette.
    #[default]
    Ansi16,
    /// Indexed palette.
    Indexed256,
    /// RGB colour.
    TrueColour,
}

/// Keyboard protocol in descending preference order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum KeyboardProtocol {
    /// Unambiguous CSI u events.
    Kitty,
    /// xterm modifyOtherKeys level 2.
    ModifyOtherKeys,
    /// Traditional escape sequences.
    #[default]
    Legacy,
}

/// Preferred attachment transport.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ImageProtocol {
    /// Kitty graphics APC.
    Kitty,
    /// iTerm2 inline image OSC.
    Iterm2,
    /// DEC sixel DCS.
    Sixel,
    /// Text attachment label.
    #[default]
    Placeholder,
}

/// Notification transport, gated by focus by the output layer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NotificationProtocol {
    /// Kitty notifications.
    Osc99,
    /// rxvt/foot notifications.
    Osc777,
    /// iTerm2 notifications.
    Osc9,
    /// Audible or visual terminal bell.
    #[default]
    Bell,
}

/// Outer multiplexer. Passthrough support is established by an inner reply.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Multiplexer {
    /// Direct terminal.
    #[default]
    None,
    /// tmux DCS passthrough, requiring allow-passthrough.
    Tmux,
    /// GNU screen DCS passthrough.
    Screen,
}

impl Multiplexer {
    /// Wrap one complete OSC, DCS or APC sequence for the outer multiplexer.
    pub fn wrap(self, bytes: &[u8]) -> Vec<u8> {
        match self {
            Self::None => bytes.to_vec(),
            Self::Tmux => {
                let mut output = b"\x1bPtmux;".to_vec();
                for &byte in bytes {
                    output.push(byte);
                    if byte == 0x1b {
                        output.push(byte);
                    }
                }
                output.extend_from_slice(b"\x1b\\");
                output
            }
            Self::Screen => {
                // screen does not accept a nested ST; OSC callers use BEL.
                let mut output = b"\x1bP".to_vec();
                output.extend_from_slice(bytes);
                output.extend_from_slice(b"\x1b\\");
                output
            }
        }
    }
}

/// DECRQM response, including modes which must never be changed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ModeState {
    /// No reply; preserve the unknown initial state.
    #[default]
    Unknown,
    /// Mode is not recognized.
    Unsupported,
    /// Mode is set and may be reset.
    Set,
    /// Mode is reset and may be set.
    Reset,
    /// Permanently set.
    AlwaysSet,
    /// Permanently reset.
    AlwaysReset,
}

impl ModeState {
    /// Whether this mode can be used.
    pub fn supported(self) -> bool {
        matches!(self, Self::Set | Self::Reset | Self::AlwaysSet)
    }
}

/// All feature choices for one terminal session; emitters never inspect env.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Capabilities {
    /// Terminal identity from XTVERSION, otherwise the environment.
    pub identity: String,
    /// Colour fallback ladder.
    pub colour: ColourDepth,
    /// OSC 11 RGB background, normalized to eight bits per channel.
    pub background: Option<[u8; 3]>,
    /// Keyboard fallback ladder.
    pub keyboard: KeyboardProtocol,
    /// Image fallback ladder.
    pub images: ImageProtocol,
    /// Notification fallback ladder.
    pub notification: NotificationProtocol,
    /// Original DEC mode states indexed by mode number.
    pub modes: BTreeMap<u16, ModeState>,
    /// Underline style and colour support (XTGETTCAP Smulx).
    pub styled_underline: bool,
    /// DECSTBM margins and SU/SD row scrolling are supported together.
    pub scroll_regions: bool,
    /// OSC 8 hyperlinks.
    pub hyperlinks: bool,
    /// OSC 52 clipboard copy.
    pub clipboard: bool,
    /// OSC 9;4 progress.
    pub progress: bool,
    /// XTWINOPS title stack.
    pub title_stack: bool,
    /// OSC 7 cwd.
    pub cwd: bool,
    /// OSC 133 semantic prompt marks.
    pub prompt_marks: bool,
    /// DECSCUSR cursor shape.
    pub cursor_shape: bool,
    /// Pixel mouse coordinates (1016).
    pub pixel_mouse: bool,
    /// Outer multiplexer.
    pub multiplexer: Multiplexer,
    /// An inner OSC/DCS query answered through passthrough.
    pub passthrough: bool,
    /// Remote session; native clipboard fallback is inappropriate.
    pub remote: bool,
    /// A dumb terminal should stay in line mode.
    pub dumb: bool,
}

impl Capabilities {
    /// Original DEC private mode state.
    pub fn mode(&self, mode: u16) -> ModeState {
        self.modes.get(&mode).copied().unwrap_or_default()
    }

    /// Choose a light theme from background luminance (sRGB approximation).
    pub fn light_theme(&self) -> bool {
        self.background.is_some_and(|[r, g, b]| {
            u32::from(r) * 299 + u32::from(g) * 587 + u32::from(b) * 114 > 128_000
        })
    }
}

/// Captured once, and injectable without mutating process-global environment.
#[derive(Clone, Debug, Default)]
pub struct Environment(pub BTreeMap<String, String>);

impl Environment {
    /// Capture only terminal-related environment variables.
    pub fn capture() -> Self {
        Self(
            std::env::vars_os()
                .filter_map(|(key, value)| {
                    Some((key.into_string().ok()?, value.into_string().ok()?))
                })
                .filter(|(key, _)| {
                    matches!(
                        key.as_str(),
                        "TERM"
                            | "COLORTERM"
                            | "TERM_PROGRAM"
                            | "WT_SESSION"
                            | "KITTY_WINDOW_ID"
                            | "TMUX"
                            | "STY"
                            | "NO_COLOR"
                    ) || key.starts_with("SSH_")
                })
                .collect(),
        )
    }

    /// A dumb terminal uses the client's headless runner and no TTY lifecycle.
    pub fn is_dumb(&self) -> bool {
        self.get("TERM") == "dumb"
    }

    pub(crate) fn get(&self, key: &str) -> &str {
        self.0.get(key).map_or("", String::as_str)
    }

    pub(crate) fn transport(&self) -> Capabilities {
        Capabilities {
            remote: self.0.keys().any(|key| key.starts_with("SSH_")),
            multiplexer: if self.0.contains_key("TMUX") {
                Multiplexer::Tmux
            } else if self.0.contains_key("STY") {
                Multiplexer::Screen
            } else {
                Multiplexer::None
            },
            dumb: self.is_dumb(),
            ..Capabilities::default()
        }
    }

    pub(crate) fn fallback(&self) -> Capabilities {
        let mut caps = self.transport();
        if caps.dumb {
            caps.colour = ColourDepth::Monochrome;
            return caps;
        }
        let identity = if self.0.contains_key("KITTY_WINDOW_ID") {
            "kitty"
        } else if self.0.contains_key("WT_SESSION") {
            "Windows Terminal"
        } else if !self.get("TERM_PROGRAM").is_empty() {
            self.get("TERM_PROGRAM")
        } else {
            self.get("TERM")
        };
        caps.apply_identity(identity);
        if self.get("TERM").contains("256color") {
            caps.colour = caps.colour.max(ColourDepth::Indexed256);
        }
        if matches!(self.get("COLORTERM"), "truecolor" | "24bit") {
            caps.colour = ColourDepth::TrueColour;
        }
        caps
    }
}

impl Capabilities {
    pub(crate) fn apply_identity(&mut self, identity: &str) {
        self.identity = identity.to_owned();
        let name = identity.to_ascii_lowercase();
        let kitty = name.contains("kitty");
        let wezterm = name.contains("wezterm");
        let ghostty = name.contains("ghostty");
        let iterm = name.contains("iterm");
        let foot = name.contains("foot");
        let alacritty = name.contains("alacritty");
        let windows = name.contains("windows terminal") || name.contains("conemu");
        let vscode = name.contains("vscode");
        let xterm = name.contains("xterm");
        let modern = kitty || wezterm || ghostty || iterm || foot || alacritty || windows || vscode;
        self.colour = if modern {
            ColourDepth::TrueColour
        } else if xterm
            || name.contains("apple_terminal")
            || name.contains("tmux")
            || name.contains("screen")
        {
            ColourDepth::Indexed256
        } else {
            ColourDepth::Ansi16
        };
        self.keyboard = if kitty || wezterm || ghostty || foot || alacritty {
            KeyboardProtocol::Kitty
        } else if xterm || iterm {
            KeyboardProtocol::ModifyOtherKeys
        } else {
            KeyboardProtocol::Legacy
        };
        self.images = if kitty || wezterm || ghostty {
            ImageProtocol::Kitty
        } else if iterm {
            ImageProtocol::Iterm2
        } else {
            ImageProtocol::Placeholder
        };
        self.notification = if kitty {
            NotificationProtocol::Osc99
        } else if foot {
            NotificationProtocol::Osc777
        } else if iterm || wezterm || ghostty {
            NotificationProtocol::Osc9
        } else {
            NotificationProtocol::Bell
        };
        self.scroll_regions = modern
            || xterm
            || name.contains("apple_terminal")
            || name.contains("vte")
            || name.contains("konsole")
            || name.contains("tmux")
            || name.contains("screen");
        self.hyperlinks = modern;
        self.clipboard = modern || xterm;
        self.progress = windows || ghostty || iterm;
        self.title_stack = modern || xterm;
        self.cwd = modern;
        self.prompt_marks = kitty || wezterm || ghostty || iterm || vscode;
        self.cursor_shape = modern || xterm;
        self.pixel_mouse = kitty || foot;
        self.styled_underline = kitty || wezterm || ghostty || foot || alacritty || vscode;
    }
}
