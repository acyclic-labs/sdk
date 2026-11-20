//! Capture terminal input and reply bytes for the `term_decoder` target.
use acyclic_harness_tui_core::key_event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use acyclic_harness_tui_terminal::{Multiplexer, encode_key};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = acyclic_test_support::crate_path!("corpus/term_decoder");
    std::fs::create_dir_all(&root)?;
    let mut sources = String::from("file\tblake3\tproducer\n");
    let mut seeds: Vec<(&str, Vec<u8>, &str)> = Vec::new();
    for (name, code, modifiers, kind) in [
        (
            "kitty-char-press",
            KeyCode::Char('a'),
            KeyModifiers::NONE,
            KeyEventKind::Press,
        ),
        (
            "kitty-char-control",
            KeyCode::Char('c'),
            KeyModifiers::CONTROL,
            KeyEventKind::Press,
        ),
        (
            "kitty-char-release",
            KeyCode::Char('a'),
            KeyModifiers::NONE,
            KeyEventKind::Release,
        ),
        (
            "kitty-arrow-alt",
            KeyCode::Left,
            KeyModifiers::ALT,
            KeyEventKind::Press,
        ),
        (
            "kitty-f5",
            KeyCode::F(5),
            KeyModifiers::NONE,
            KeyEventKind::Press,
        ),
        (
            "kitty-unicode",
            KeyCode::Char('é'),
            KeyModifiers::NONE,
            KeyEventKind::Press,
        ),
    ] {
        let key = KeyEvent {
            kind,
            ..KeyEvent::new(code, modifiers)
        };
        seeds.push((
            name,
            encode_key(key)?,
            "Shipping encode_key writer for one representable key event",
        ));
    }
    for (name, bytes) in [
        ("legacy-arrow", b"\x1b[A".as_slice()),
        ("legacy-f1", b"\x1bOP"),
        ("legacy-alt-char", b"\x1ba"),
        ("legacy-escape", b"\x1b"),
        ("utf8-text", "héllo".as_bytes()),
        ("focus-in", b"\x1b[I"),
        ("focus-out", b"\x1b[O"),
        ("mouse-press", b"\x1b[<0;10;5M"),
        ("mouse-release", b"\x1b[<0;10;5m"),
        ("mouse-scroll", b"\x1b[<64;10;5M"),
        ("paste", b"\x1b[200~hello\x1b[201~"),
        ("settlement-query", b"\x1b[?7727;42n"),
        ("da1-reply", b"\x1b[?62;4c"),
        ("keyboard-flags-reply", b"\x1b[?1u"),
        ("mode-reply", b"\x1b[?2026;1$y"),
        ("osc11-reply", b"\x1b]11;rgb:1e1e/1e1e/1e1e\x07"),
        ("xtversion-reply", b"\x1bP>|XTerm(390)\x1b\\"),
        ("xtgettcap-reply", b"\x1bP1+r524742=1b5b33386d\x1b\\"),
        ("graphics-reply", b"\x1b_Gi=314159;OK\x1b\\"),
    ] {
        seeds.push((
            name,
            bytes.to_vec(),
            "Documented terminal input or reply grammar, not a captured session",
        ));
    }
    seeds.push((
        "tmux-osc11-reply",
        Multiplexer::Tmux.wrap(b"\x1b]11;rgb:0000/0000/0000\x07"),
        "Shipping Multiplexer::wrap writer around one OSC 11 reply",
    ));
    let count = seeds.len();
    for (name, bytes, producer) in seeds {
        sources.push_str(&format!("{name}\t{}\t{producer}\n", blake3::hash(&bytes)));
        std::fs::write(root.join(name), bytes)?;
    }
    std::fs::write(root.with_file_name("term_decoder-SOURCES.tsv"), sources)?;
    println!("wrote {count} seeds to {}", root.display());
    Ok(())
}
