//! Snapshots of transcript rows as a terminal shows them: representative
//! markdown at a wide and a narrow width, the code sample in every theme at
//! every colour depth, and the resolved token table. Each span shows its
//! style as `«colour,attributes»`, written only where the style changes.
//! Trailing row whitespace and trailing blank rows are omitted.
//!
//! A mismatch prints the whole rendered snapshot. Set `UPDATE_GOLDEN=1` to
//! write it to `tests/golden/` instead, then review the diff.
#![cfg(feature = "tui")]
use acyclic_harness_tui::{
    TranscriptRows,
    config::{ConfigView, ThemeStyles, Token},
    ratatui::{
        style::{Color, Modifier, Style},
        text::Line,
    },
    term::{Capabilities, ColourDepth},
};
/// The code fence sample, also shown in every theme.
const CODE: (&str, &str, &str) = (
    "code",
    "",
    "```rust\n// Answer the question.\nfn answer() -> u32 {\n    let text = \"forty-two\";\n    42\n}\n```\n\n```sh\necho \"$HOME\" # where\n```\n",
);

/// Each sample's name, the prompt before it (or none) and its answer.
const SAMPLES: [(&str, &str, &str); 6] = [
    (
        "headings",
        "Summarise **this**, shown as typed",
        "# Title\n\nIntro with **bold**, *italic*, ~~gone~~ and `code`.\n\n## Section\n\n### Detail\n",
    ),
    (
        "lists",
        "",
        "1. first\n2. second\n   - nested item that is long enough to wrap at a narrow width\n\n- [x] done\n- [ ] later\n",
    ),
    CODE,
    (
        "table",
        "",
        "| crate | lines | note |\n| :- | -: | - |\n| tui | 1200 | renders **rows** |\n| markdown | 4000 | streams |\n",
    ),
    (
        "links",
        "",
        "See [the docs](https://example.com/docs) or <https://example.com>.\n\n> Quoted with a [link](https://example.com/q).\n\n---\n",
    ),
    (
        "unicode",
        "",
        "界面 **宽字符** 👩‍💻 e\u{301} — mixed-width text that wraps\n\n| 名前 | 🦀 |\n| - | - |\n| 値 | ✓ |\n",
    ),
];

const DEPTHS: [ColourDepth; 4] = [
    ColourDepth::TrueColour,
    ColourDepth::Indexed256,
    ColourDepth::Ansi16,
    ColourDepth::Monochrome,
];

/// The shipped themes, dark then light, each at every depth of `DEPTHS`.
type Themes = [(&'static str, [ThemeStyles; 4]); 2];

fn colour(color: Color) -> String {
    match color {
        Color::Rgb(red, green, blue) => format!("#{red:02x}{green:02x}{blue:02x}"),
        Color::Indexed(index) => format!("@{index}"),
        Color::Reset => "default".into(),
        named => format!("{named:?}").to_lowercase(),
    }
}

fn style(style: Style) -> String {
    let mut shown = style.fg.map(colour).unwrap_or_default();
    for (modifier, mark) in [
        (Modifier::BOLD, "b"),
        (Modifier::ITALIC, "i"),
        (Modifier::DIM, "d"),
        (Modifier::UNDERLINED, "u"),
        (Modifier::CROSSED_OUT, "s"),
        (Modifier::REVERSED, "r"),
    ] {
        if style.add_modifier.contains(modifier) {
            shown.push(',');
            shown.push_str(mark);
        }
    }
    shown
}

/// Rows as text, each styled run opened by its style when it changes.
fn rows(lines: &[Line<'_>]) -> String {
    let mut out = String::new();
    for line in lines {
        let mut current = Style::default();
        for span in &line.spans {
            if span.style != current {
                current = span.style;
                out.push('«');
                out.push_str(&style(current));
                out.push('»');
            }
            out.push_str(&span.content);
        }
        out.push('\n');
    }
    out
}

fn transcript((_, prompt, answer): (&str, &str, &str), width: u16, theme: ThemeStyles) -> String {
    let mut transcript = TranscriptRows::default();
    transcript.set_theme(theme);
    if !prompt.is_empty() {
        transcript.push(true, prompt);
    }
    transcript.push(false, answer);
    rows(transcript.rows(width))
}

fn markdown(width: u16, dark: ThemeStyles) -> String {
    SAMPLES
        .iter()
        .map(|sample| {
            format!(
                "== {} at {width} columns\n{}",
                sample.0,
                transcript(*sample, width, dark)
            )
        })
        .collect()
}

fn code_themes(themes: &Themes) -> String {
    let mut out = String::new();
    for (mode, styles) in themes {
        for (depth, theme) in DEPTHS.iter().zip(styles) {
            out.push_str(&format!("== {mode} {depth:?}\n"));
            out.push_str(&transcript(CODE, 60, *theme));
        }
    }
    out
}

fn tokens(themes: &Themes) -> String {
    let mut out = String::from("token      mode   truecolour 256 16 monochrome\n");
    for token in Token::ALL {
        for (mode, styles) in themes {
            out.push_str(&format!("{:<10} {mode:<6}", token.name()));
            for theme in styles {
                out.push(' ');
                out.push_str(&style(theme.token(token)));
            }
            out.push('\n');
        }
    }
    out
}

/// The writer and comparer share one whitespace-safe snapshot format.
fn snapshot(rendered: &str) -> String {
    let mut out = String::new();
    for line in rendered.lines() {
        out.push_str(line.trim_end());
        out.push('\n');
    }
    out.truncate(out.trim_end_matches('\n').len());
    if !out.is_empty() {
        out.push('\n');
    }
    out
}

#[test]
fn transcript_rows_match_their_snapshots() {
    let directory = acyclic_test_support::crate_path!("tests/golden");
    let update = std::env::var_os("UPDATE_GOLDEN").is_some();
    let themes: Themes = [("dark", [0; 3]), ("light", [0xff; 3])].map(|(mode, background)| {
        let theme = |colour| {
            ConfigView::default().theme(&Capabilities {
                colour,
                background: Some(background),
                ..Default::default()
            })
        };
        (mode, DEPTHS.map(|depth| theme(depth).unwrap()))
    });
    let [(_, [dark, ..]), _] = themes;
    let mut stale = Vec::new();
    for (name, actual) in [
        ("markdown-80.txt", markdown(80, dark)),
        ("markdown-24.txt", markdown(24, dark)),
        ("code-themes.txt", code_themes(&themes)),
        ("tokens.txt", tokens(&themes)),
    ] {
        let path = directory.join(name);
        let actual = snapshot(&actual);
        if update {
            std::fs::create_dir_all(&directory).unwrap();
            std::fs::write(&path, &actual).unwrap();
        } else if std::fs::read_to_string(&path)
            .ok()
            .map(|golden| golden.replace("\r\n", "\n"))
            != Some(actual.clone())
        {
            stale.push(format!("---- {name}\n{actual}---- end {name}"));
        }
    }
    assert!(stale.is_empty(), "stale snapshots:\n{}", stale.join("\n"));
}
