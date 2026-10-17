//! Configuration precedence, live rendering, parser invariants and negative controls.
#![cfg(feature = "tui")]
use acyclic_harness_tui::ratatui::{
    Terminal,
    backend::TestBackend,
    style::{Color, Modifier},
};
use acyclic_harness_tui::{
    ACTIONS, Action, AppAction, Editor, InputEffect, PromptInput, PromptWidget, Viewport,
    config::{
        ConfigDiagnostics, ConfigFile, ConfigView, DiagnosticKind, Scope, ThemeRole, fold, parse,
    },
    key_event::{KeyCode, KeyEvent, KeyModifiers as M},
    term::{Capabilities, ColourDepth},
};
use acyclic_test_support::differential::oracle::{Observer, Oracle};
use proptest::prelude::*;
use sequences::{Parsed, Stage};
use serde_json::json;
#[path = "support/config_sequences.rs"]
mod sequences;

fn apply(view: &ConfigView, input: &mut PromptInput) {
    view.apply(input, &Capabilities::default()).unwrap();
}

#[test]
fn all_defaults_validate_and_all_actions_bind_by_name() {
    let mut view = ConfigView::default();
    let mut input = PromptInput::default();
    assert_eq!(view.settings().unwrap(), *input.editor.settings());
    let schema = schema_oracle();
    for file in ConfigFile::ALL {
        schema.check(&(file, file.defaults().as_bytes().to_vec()));
        assert!(parse(file, Scope::User, file.defaults()).is_ok());
    }
    for binding in ACTIONS {
        view.set_file(
            Scope::User,
            ConfigFile::Keymap,
            Some(&json!({"bindings":{"F12":binding.chord}}).to_string()),
        );
        apply(&view, &mut input);
        let resolved = input.keymap.resolve("F12").unwrap();
        assert_eq!(resolved.action, binding.action);
        assert_eq!(resolved.action.name(), binding.chord);
        assert!(!resolved.help.is_empty());
    }
}

#[test]
fn live_keys_settings_and_diagnostics_keep_the_editor() {
    let mut input = PromptInput::new(Editor::new("draft"));
    input.committed_text("!", 1);
    let original = input.editor.text().to_owned();
    let mut view = ConfigView::default();
    view.set_file(
        Scope::User,
        ConfigFile::Settings,
        Some(r##"{"tab_width":2,"wrap":false}"##),
    );
    view.set_file(
        Scope::Workspace,
        ConfigFile::Settings,
        Some(r##"{"tab_width":3}"##),
    );
    view.set_file(
        Scope::User,
        ConfigFile::Keymap,
        Some(r##"{"bindings":{"F12":"app.tree"}}"##),
    );
    apply(&view, &mut input);
    assert!(!input.editor.settings().wrap);
    assert_eq!(input.editor.settings().tab_width, 3);
    assert_eq!(input.editor.text(), original);
    assert!(input.editor.undo());
    assert_eq!(input.editor.text(), "draft");
    assert_eq!(
        input.key(KeyEvent::new(KeyCode::F(12), M::NONE), 2, 80),
        [InputEffect::Host(Action::App(AppAction::Tree))]
    );
    view.set_file(
        Scope::Workspace,
        ConfigFile::Settings,
        Some(r##"{"tab_wdth":0}"##),
    );
    apply(&view, &mut input);
    assert_eq!(input.editor.settings().tab_width, 2);
    assert_eq!(
        view.diagnostics().next().unwrap().kind,
        DiagnosticKind::UnknownKey
    );
    let mut terminal = Terminal::new(TestBackend::new(100, 2)).unwrap();
    terminal
        .draw(|frame| frame.render_widget(ConfigDiagnostics::new(&view, &input), frame.area()))
        .unwrap();
    let text: String = terminal
        .backend()
        .buffer()
        .content
        .iter()
        .map(|c| c.symbol())
        .collect();
    assert!(
        text.contains("config/settings.json $.tab_wdth: unknown configuration key"),
        "{text}"
    );
    view.set_file(Scope::Workspace, ConfigFile::Settings, None);
    assert_eq!(view.diagnostics().count(), 0);
}

#[test]
fn diagnostic_categories_and_canonical_conflicts() {
    for (file, text, kind) in [
        (
            ConfigFile::Settings,
            r##"{"tab_width":0}"##,
            DiagnosticKind::OutOfRange,
        ),
        (
            ConfigFile::Settings,
            r##"{"wrap":"yes"}"##,
            DiagnosticKind::InvalidType,
        ),
        (
            ConfigFile::Theme,
            r##"{"text":{"foreground":"#xyzxyz"}}"##,
            DiagnosticKind::InvalidValue,
        ),
        (
            ConfigFile::Theme,
            r##"{"text":{"blink":true}}"##,
            DiagnosticKind::UnknownKey,
        ),
        (
            ConfigFile::Keymap,
            r##"{"bindings":{"Ctrl+nope":"undo"}}"##,
            DiagnosticKind::InvalidChord,
        ),
        (
            ConfigFile::Keymap,
            r##"{"bindings":{"F1":"not-an-action"}}"##,
            DiagnosticKind::UnknownAction,
        ),
        (
            ConfigFile::Keymap,
            r##"{"bindings":{"Super+k":"undo","Ctrl+k":"redo"}}"##,
            DiagnosticKind::Conflict,
        ),
    ] {
        let errors = parse(file, Scope::Workspace, text).unwrap_err();
        assert!(errors.iter().any(|d| d.kind == kind), "{errors:?}");
        assert!(
            errors
                .iter()
                .all(|d| d.scope == Scope::Workspace && d.file == file)
        );
    }
}

#[test]
fn live_theme_and_no_wrap_render_with_test_backend() {
    let mut input = PromptInput::new(Editor::new("abcdef"));
    input.editor.set_cursor(6, false);
    let mut view = ConfigView::default();
    view.set_file(
        Scope::User,
        ConfigFile::Settings,
        Some(r##"{"wrap":false}"##),
    );
    view.set_file(
        Scope::User,
        ConfigFile::Theme,
        Some(r##"{"text":{"foreground":"#123456","background":"#010203","bold":true}}"##),
    );
    let profile = Capabilities {
        colour: ColourDepth::TrueColour,
        ..Default::default()
    };
    view.apply(&mut input, &profile).unwrap();
    let mut terminal = Terminal::new(TestBackend::new(4, 1)).unwrap();
    let mut viewport = Viewport::default();
    terminal
        .draw(|frame| {
            let mut widget = PromptWidget::new(&input.editor);
            widget.style = input.theme.style(ThemeRole::Text);
            frame.render_stateful_widget(widget, frame.area(), &mut viewport);
        })
        .unwrap();
    let buffer = terminal.backend().buffer();
    let text: String = buffer.content.iter().map(|c| c.symbol()).collect();
    assert_eq!(text, "def ");
    assert_eq!(viewport.cursor, Some((3, 0)));
    assert_eq!(buffer[(3, 0)].bg, Color::Rgb(1, 2, 3));
    assert_eq!(buffer[(0, 0)].fg, Color::Rgb(0x12, 0x34, 0x56));
    assert!(buffer[(0, 0)].modifier.contains(Modifier::BOLD));
    for depth in [
        ColourDepth::Monochrome,
        ColourDepth::Ansi16,
        ColourDepth::Indexed256,
    ] {
        let theme = view
            .theme(&Capabilities {
                colour: depth,
                ..Default::default()
            })
            .unwrap();
        let style = theme.style(ThemeRole::Text);
        match depth {
            ColourDepth::Monochrome => assert_eq!(style.fg, Some(Color::Reset)),
            ColourDepth::Indexed256 => assert!(matches!(style.fg, Some(Color::Indexed(_)))),
            _ => assert!(!matches!(
                style.fg,
                Some(Color::Rgb(..)) | Some(Color::Indexed(_))
            )),
        }
        assert!(style.add_modifier.contains(Modifier::BOLD));
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn merge_is_per_key_defaults_user_workspace(
        defaults in prop::collection::btree_map("[a-z]{1,3}", any::<u16>(), 0..12),
        user in prop::collection::btree_map("[a-z]{1,3}", any::<u16>(), 0..12),
        workspace in prop::collection::btree_map("[a-z]{1,3}", any::<u16>(), 0..12),
    ) {
        let mut oracle = defaults.clone();
        oracle.extend(user.clone()); oracle.extend(workspace.clone());
        let actual = fold(&json!({"nested":defaults}), Some(&json!({"nested":user})), Some(&json!({"nested":workspace})));
        prop_assert_eq!(actual, json!({"nested":oracle}));
    }
    #[test]
    fn arrival_order_cannot_change_precedence(user in 1usize..17, workspace in 1usize..17) {
        let mut first = ConfigView::default(); let mut second = ConfigView::default();
        let u = json!({"tab_width":user}).to_string(); let w = json!({"tab_width":workspace}).to_string();
        first.set_file(Scope::User, ConfigFile::Settings, Some(&u));
        first.set_file(Scope::Workspace, ConfigFile::Settings, Some(&w));
        second.set_file(Scope::Workspace, ConfigFile::Settings, Some(&w));
        second.set_file(Scope::User, ConfigFile::Settings, Some(&u));
        prop_assert_eq!(first.settings().unwrap(), second.settings().unwrap());
        prop_assert_eq!(first.settings().unwrap().tab_width, workspace);
    }
}

#[derive(Debug)]
enum SchemaFault {
    /// The encoded file parses back as something else.
    Reencoded,
    /// A canonical keymap parses back as something else.
    Recanonicalized,
    /// The schema accepts an unknown key.
    AcceptedUnknown,
}

type Schema = (ConfigFile, Vec<u8>);

fn schema_oracle() -> Oracle<Schema, SchemaFault, sequences::Seen> {
    let declared = Oracle::declare(
        |(file, bytes): &Schema,
         fault: Option<&SchemaFault>,
         see: &mut Observer<'_, sequences::Seen>| {
            let tamper: sequences::Tamper = match fault {
                None => |_, _| {},
                Some(SchemaFault::Reencoded) => |stage, parsed: &mut Parsed| {
                    if stage == Stage::Reparsed {
                        *parsed = Ok(serde_json::Value::Null);
                    }
                },
                Some(SchemaFault::Recanonicalized) => |stage, parsed: &mut Parsed| {
                    if stage == Stage::Canonical {
                        *parsed = Ok(serde_json::Value::Null);
                    }
                },
                Some(SchemaFault::AcceptedUnknown) => |stage, parsed: &mut Parsed| {
                    if stage == Stage::Unknown {
                        *parsed = Ok(serde_json::Value::Null);
                    }
                },
            };
            sequences::observe(*file, bytes, tamper, &mut |seen| see.see(seen))
        },
    );
    let defaults = |file: ConfigFile| (file, file.defaults().as_bytes().to_vec());
    sequences::INVARIANTS
        .into_iter()
        .fold(declared, |oracle, (name, check)| {
            oracle.invariant(name, check)
        })
        .control(
            "roundtrip",
            defaults(ConfigFile::Settings),
            SchemaFault::Reencoded,
        )
        .control(
            "canonical",
            defaults(ConfigFile::Keymap),
            SchemaFault::Recanonicalized,
        )
        .control(
            "unknown",
            defaults(ConfigFile::Settings),
            SchemaFault::AcceptedUnknown,
        )
        .control(
            "unknown",
            defaults(ConfigFile::Keymap),
            SchemaFault::AcceptedUnknown,
        )
        .control(
            "unknown",
            defaults(ConfigFile::Theme),
            SchemaFault::AcceptedUnknown,
        )
        .build()
}

#[test]
fn config_seed_corpus() {
    let schema = schema_oracle();
    for (file, data) in [
        (
            ConfigFile::Keymap,
            include_bytes!("../fuzz/corpus/config_keymap/aliases").as_slice(),
        ),
        (
            ConfigFile::Keymap,
            include_bytes!("../fuzz/corpus/config_keymap/chord"),
        ),
        (
            ConfigFile::Keymap,
            include_bytes!("../fuzz/corpus/config_keymap/defaults"),
        ),
        (
            ConfigFile::Keymap,
            include_bytes!("../fuzz/corpus/config_keymap/duplicate"),
        ),
        (
            ConfigFile::Keymap,
            include_bytes!("../fuzz/corpus/config_keymap/syntax"),
        ),
        (
            ConfigFile::Keymap,
            include_bytes!("../fuzz/corpus/config_keymap/unknown"),
        ),
        (
            ConfigFile::Settings,
            include_bytes!("../fuzz/corpus/config_settings/defaults"),
        ),
        (
            ConfigFile::Theme,
            include_bytes!("../fuzz/corpus/config_theme/defaults"),
        ),
        (
            ConfigFile::Theme,
            include_bytes!("../fuzz/corpus/config_theme/invalid-rgb"),
        ),
        (
            ConfigFile::Theme,
            include_bytes!("../fuzz/corpus/config_theme/rgb"),
        ),
        (
            ConfigFile::Theme,
            include_bytes!("../fuzz/corpus/config_theme/syntax"),
        ),
        (
            ConfigFile::Theme,
            include_bytes!("../fuzz/corpus/config_theme/unknown"),
        ),
    ] {
        // The fuzz entry itself, then the judged observations.
        sequences::exercise(file, data);
        schema.check(&(file, data.to_vec()));
    }
}

#[test]
fn parsers_round_trip_and_reject_unknown_keys() {
    let schema = schema_oracle();
    proptest!(ProptestConfig::with_cases(64), |(bytes in prop::collection::vec(any::<u8>(), 0..512))| {
        for file in ConfigFile::ALL {
            schema.check(&(file, bytes.clone()));
        }
    });
    proptest!(ProptestConfig::with_cases(64), |(tab in 1usize..17, wrap in any::<bool>(), rgb in any::<[u8; 3]>(), bold in any::<bool>(), index in 0..ACTIONS.len(), f in 1u8..25)| {
        let settings = json!({"tab_width": tab, "wrap": wrap});
        let color = format!("#{:02x}{:02x}{:02x}", rgb[0], rgb[1], rgb[2]);
        let theme = json!({"text": {"foreground": color, "bold": bold}});
        let keymap = json!({"bindings": {format!("Ctrl+F{f}"): ACTIONS[index].chord}});
        for (file, value) in [(ConfigFile::Settings, settings), (ConfigFile::Theme, theme), (ConfigFile::Keymap, keymap)] {
            schema.check(&(file, value.to_string().into_bytes()));
        }
    });
}

#[test]
fn cancelling_edit_and_search_retains_live_settings() {
    let mut input = PromptInput::new(Editor::new("draft"));
    input.edit_message("previous".into(), "old message");
    let mut view = ConfigView::default();
    view.set_file(
        Scope::User,
        ConfigFile::Settings,
        Some(r#"{"tab_width":7}"#),
    );
    apply(&view, &mut input);
    input.key(KeyEvent::new(KeyCode::Esc, M::NONE), 1, 80);
    assert_eq!(input.editor.text(), "draft");
    assert_eq!(input.editor.settings().tab_width, 7);
    input.key(KeyEvent::new(KeyCode::Char('r'), M::CONTROL), 2, 80);
    view.set_file(
        Scope::User,
        ConfigFile::Settings,
        Some(r#"{"tab_width":3}"#),
    );
    apply(&view, &mut input);
    input.key(KeyEvent::new(KeyCode::Esc, M::NONE), 3, 80);
    assert_eq!(input.editor.text(), "draft");
    assert_eq!(input.editor.settings().tab_width, 3);
}

#[test]
fn preferences_drive_tab_height_paste_and_image_behavior() {
    let mut input = PromptInput::default();
    let mut view = ConfigView::default();
    view.set_file(
        Scope::User,
        ConfigFile::Settings,
        Some(
            r#"{
        "tab_width":2,"max_input_height_percent":25,"paste_collapse_bytes":4,
        "image_max_dimension":2,"image_quality":80,"scrollback_lines":2
    }"#,
        ),
    );
    apply(&view, &mut input);
    input.key(KeyEvent::new(KeyCode::Tab, M::NONE), 0, 80);
    assert_eq!(input.editor.text(), "  ");
    input.editor.set_text("one\ntwo\nthree\nfour", 1);
    assert_eq!(
        acyclic_harness_tui::PromptBox::new(&input, 0).height(80, 20),
        5
    );
    input.editor.set_text("", 2);
    input.editor.paste("abcdef", 3);
    assert!(
        input.editor.layout(80).rows[0]
            .cells
            .iter()
            .any(|c| c.symbol.contains("Pasted"))
    );
    let settings = *input.editor.settings();
    assert_eq!(settings.scrollback(&[1, 2, 3, 4]), &[3, 4]);
    let image =
        acyclic_harness_tui::prepare_image_with_settings(4, 2, vec![255; 32], settings).unwrap();
    assert!(matches!(
        image,
        acyclic_harness_tui::ClipboardContent::Image {
            width: 2,
            height: 1,
            ..
        }
    ));
    let mut invalid = settings;
    invalid.tab_width = 0;
    assert!(!input.set_settings(invalid));
    assert_eq!(input.editor.settings().tab_width, 2);
}

#[test]
fn duplicate_keys_and_uncommitted_changes_are_not_silent() {
    let errors = parse(
        ConfigFile::Keymap,
        Scope::User,
        r#"{"bindings":{"F1":"undo","F1":"redo"}}"#,
    )
    .unwrap_err();
    assert_eq!(errors[0].kind, DiagnosticKind::Conflict);
    let view = ConfigView::default();
    let bytes = view
        .edit(Scope::User, ConfigFile::Settings, &json!({"wrap":false}))
        .unwrap();
    assert!(view.settings().unwrap().wrap);
    let mut followed = view.clone();
    followed.set_bytes(Scope::User, ConfigFile::Settings, &bytes);
    assert!(!followed.settings().unwrap().wrap);
}

// Independent fixed terminal palette, separate from production's palette builder.
fn palette_oracle(index: usize) -> [u8; 3] {
    let levels = [0, 95, 135, 175, 215, 255];
    if index < 16 {
        [
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
        ][index]
    } else if index < 232 {
        let n = index - 16;
        [levels[n / 36], levels[n / 6 % 6], levels[n % 6]]
    } else {
        [(8 + (index - 232) * 10) as u8; 3]
    }
}

/// What the configuration view projected.
#[derive(Debug)]
enum Projected {
    /// The indexed colour chosen for a true-colour foreground.
    Fallback { rgb: [u8; 3], index: usize },
    /// Parser work while reading the parsed source 64 times.
    Source { reparses: u64 },
}

#[derive(Debug)]
enum Projection {
    Fallback([u8; 3]),
    Source,
}

#[derive(Debug)]
enum ProjectionFault {
    /// The fallback always picks palette entry 0.
    FirstIndex,
    /// Each source read also reparses the effective file.
    Reparsed,
}

fn fallback(rgb: [u8; 3], fault: Option<&ProjectionFault>) -> usize {
    let mut view = ConfigView::default();
    let text = json!({"text":{"foreground":format!("#{:02x}{:02x}{:02x}",rgb[0],rgb[1],rgb[2])}})
        .to_string();
    view.set_file(Scope::User, ConfigFile::Theme, Some(&text));
    let theme = view
        .theme(&Capabilities {
            colour: ColourDepth::Indexed256,
            ..Default::default()
        })
        .unwrap();
    let Some(Color::Indexed(index)) = theme.style(ThemeRole::Text).fg else {
        panic!("indexed palette required")
    };
    match fault {
        Some(ProjectionFault::FirstIndex) => 0,
        _ => usize::from(index),
    }
}

fn source_reparses(fault: Option<&ProjectionFault>) -> u64 {
    let mut view = ConfigView::default();
    view.set_file(Scope::User, ConfigFile::Settings, Some(r#"{"wrap":false}"#));
    let work = acyclic_test_support::measure(|| {
        for _ in 0..64 {
            let source = view.source(Scope::User, ConfigFile::Settings).unwrap();
            assert_eq!(source["wrap"], false);
            if matches!(fault, Some(ProjectionFault::Reparsed)) {
                std::hint::black_box(view.effective(ConfigFile::Settings).unwrap());
            }
        }
    });
    work.count_total
}

fn projection_oracle() -> Oracle<Projection, ProjectionFault, Projected> {
    Oracle::declare(
        |input: &Projection, fault: Option<&ProjectionFault>, see: &mut Observer<'_, Projected>| {
            see.see(match input {
                Projection::Fallback(rgb) => Projected::Fallback {
                    rgb: *rgb,
                    index: fallback(*rgb, fault),
                },
                Projection::Source => Projected::Source {
                    reparses: source_reparses(fault),
                },
            })
        },
    )
    .invariant("nearest", |seen| match seen {
        Projected::Fallback { rgb, index } => {
            let error = |color: [u8; 3]| -> i64 {
                (0..3)
                    .map(|i| (i64::from(rgb[i]) - i64::from(color[i])).pow(2))
                    .sum()
            };
            match (0..256)
                .find(|other| error(palette_oracle(*other)) < error(palette_oracle(*index)))
            {
                Some(closer) => Err(format!("fallback {index} for {rgb:?}; {closer} is closer")),
                None => Ok(()),
            }
        }
        Projected::Source { .. } => Ok(()),
    })
    .invariant("reuse", |seen| match seen {
        Projected::Source { reparses } if *reparses != 0 => Err(format!(
            "source projection reparsed with {reparses} allocations"
        )),
        _ => Ok(()),
    })
    .control(
        "nearest",
        Projection::Fallback([255, 255, 255]),
        ProjectionFault::FirstIndex,
    )
    .control("reuse", Projection::Source, ProjectionFault::Reparsed)
    .build()
}

#[test]
fn indexed_fallback_minimizes_independent_palette_error_and_sources_reuse_parses() {
    let projection = projection_oracle();
    projection.check(&Projection::Source);
    proptest!(ProptestConfig::with_cases(64), |(rgb in any::<[u8; 3]>())| {
        projection.check(&Projection::Fallback(rgb));
    });
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn scrollback_is_a_bounded_tail(lines in prop::collection::vec(any::<u8>(), 0..100), keep in 1usize..100) {
        let mut settings = acyclic_harness_tui::DEFAULT_SETTINGS;
        settings.scrollback_lines = keep;
        let visible = settings.scrollback(&lines);
        prop_assert_eq!(visible.len(), keep.min(lines.len()));
        prop_assert!(lines.ends_with(visible));
    }
}

#[test]
fn named_selection_does_not_require_shift() {
    let mut input = PromptInput::new(Editor::new("word"));
    let mut view = ConfigView::default();
    view.set_file(
        Scope::User,
        ConfigFile::Keymap,
        Some(r#"{"bindings":{"F1":"select.word-left"}}"#),
    );
    apply(&view, &mut input);
    input.key(KeyEvent::new(KeyCode::F(1), M::NONE), 1, 80);
    assert_eq!(input.editor.selected_text(), "word");
}

#[test]
fn invalid_utf8_is_a_typed_diagnostic_and_removes_the_override() {
    let mut view = ConfigView::default();
    view.set_file(
        Scope::User,
        ConfigFile::Settings,
        Some(r#"{"tab_width":2}"#),
    );
    view.set_file(
        Scope::Workspace,
        ConfigFile::Settings,
        Some(r#"{"tab_width":3}"#),
    );
    view.set_bytes(Scope::Workspace, ConfigFile::Settings, &[255]);
    assert_eq!(view.settings().unwrap().tab_width, 2);
    assert_eq!(
        view.diagnostics().next().unwrap().kind,
        DiagnosticKind::Syntax
    );
}

#[test]
fn no_wrap_clips_collapsed_paste_marker_by_visible_graphemes() {
    let mut input = PromptInput::default();
    let mut view = ConfigView::default();
    view.set_file(
        Scope::User,
        ConfigFile::Settings,
        Some(r#"{"wrap":false,"paste_collapse_bytes":4}"#),
    );
    apply(&view, &mut input);
    input.editor.paste("abcdef", 0);
    let mut terminal = Terminal::new(TestBackend::new(4, 1)).unwrap();
    let mut viewport = Viewport::default();
    terminal
        .draw(|frame| {
            frame.render_stateful_widget(
                PromptWidget::new(&input.editor),
                frame.area(),
                &mut viewport,
            )
        })
        .unwrap();
    let text: String = terminal
        .backend()
        .buffer()
        .content
        .iter()
        .map(|c| c.symbol())
        .collect();
    assert_eq!(text, "es] ");
    assert_eq!(viewport.cursor, Some((3, 0)));
}

#[test]
fn reloading_keymap_cancels_pending_cut_and_retains_deferred_typing() {
    let mut input = PromptInput::new(Editor::new("draft"));
    let mut view = ConfigView::default();
    apply(&view, &mut input);
    input.editor.select_all();
    assert!(
        input
            .key(KeyEvent::new(KeyCode::Char('x'), M::CONTROL), 0, 80)
            .is_empty()
    );
    view.set_file(
        Scope::User,
        ConfigFile::Keymap,
        Some(r#"{"bindings":{"Ctrl+x":null,"Ctrl+x Ctrl+e":null}}"#),
    );
    apply(&view, &mut input);
    assert!(input.tick(1000).is_empty());
    assert_eq!(input.editor.selected_text(), "draft");
    input.editor.set_text("", 1001);
    view.set_file(
        Scope::User,
        ConfigFile::Keymap,
        Some(r#"{"bindings":{"a b":"app.tree"}}"#),
    );
    apply(&view, &mut input);
    assert!(
        input
            .key(KeyEvent::new(KeyCode::Char('a'), M::NONE), 1002, 80)
            .is_empty()
    );
    assert_eq!(input.editor.text(), "");
    view.set_file(Scope::User, ConfigFile::Keymap, None);
    apply(&view, &mut input);
    assert_eq!(input.editor.text(), "a");
    assert!(input.tick(2000).is_empty());
    assert_eq!(input.editor.text(), "a");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn keymap_reload_preserves_exactly_the_deferred_character(text in "[a-z]", same in any::<bool>()) {
        let mut input = PromptInput::default();
        let mut view = ConfigView::default();
        let chord = format!("{text} F1");
        view.set_file(Scope::User, ConfigFile::Keymap,
            Some(&json!({"bindings":{chord:"app.tree"}}).to_string()));
        apply(&view, &mut input);
        let key = KeyEvent::new(KeyCode::Char(text.chars().next().unwrap()), M::NONE);
        prop_assert!(input.key(key, 0, 80).is_empty());
        if !same { view.set_file(Scope::User, ConfigFile::Keymap, None); }
        apply(&view, &mut input);
        prop_assert!(input.tick(1000).is_empty());
        prop_assert_eq!(input.editor.text(), text);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn saved_keymap_loads_and_live_file_projection_agree(
        action in 0usize..ACTIONS.len(), disabled in any::<bool>(), key in 1u8..25,
    ) {
        let chord = format!("F{key}");
        let target = if disabled { serde_json::Value::Null } else { ACTIONS[action].chord.into() };
        let text = json!({"bindings":{chord.clone():target}}).to_string();
        let map = acyclic_harness_tui::InputKeymap::parse(&text).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config/keymap.json");
        map.save(&path).unwrap();
        let restored = acyclic_harness_tui::InputKeymap::load(&path).unwrap();
        prop_assert_eq!(&restored, &map);
        let saved = std::fs::read_to_string(path).unwrap();
        let mut view = ConfigView::default();
        view.set_file(Scope::User, ConfigFile::Keymap, Some(&saved));
        prop_assert_eq!(view.diagnostics().count(), 0);
        let mut input = PromptInput::default();
        apply(&view, &mut input);
        prop_assert_eq!(input.keymap.resolve(&chord).map(|b| b.action), map.resolve(&chord).map(|b| b.action));
        prop_assert_eq!(parse(ConfigFile::Keymap, Scope::User, &saved).unwrap()["bindings"][&chord].clone(), target);
    }
}

#[test]
fn obsolete_flat_and_chord_target_formats_are_rejected() {
    for text in [
        r#"{"Ctrl+l":"Home"}"#,
        r#"{"bindings":{"Ctrl+l":"Home"}}"#,
        r#"{"bindings":{"Ctrl+l":""}}"#,
    ] {
        assert!(acyclic_harness_tui::InputKeymap::parse(text).is_err());
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn reduced_motion_setting_round_trips_and_static_label_ignores_time(
        reduced in any::<bool>(), now in any::<u64>(), later in any::<u64>()
    ) {
        let mut view = ConfigView::default();
        let text = serde_json::json!({"reduced_motion": reduced}).to_string();
        view.set_file(Scope::User, ConfigFile::Settings, Some(&text));
        let preferences = view.settings().unwrap();
        prop_assert_eq!(preferences.reduced_motion, reduced);
        let encoded = serde_json::to_string(&preferences).unwrap();
        let decoded: acyclic_harness_tui::UiSettings = serde_json::from_str(&encoded).unwrap();
        prop_assert_eq!(preferences, decoded);
        if reduced {
            let theme = acyclic_harness_tui::config::ThemeStyles::default();
            let label = |time| acyclic_harness_tui::Thinking::new(&theme,time).reduced_motion(reduced).line();
            prop_assert_eq!(label(now),label(later));
        }
    }
}
