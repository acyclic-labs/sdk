use super::*;

#[test]
#[allow(deprecated)]
fn new() {
    let cell = Cell::new("あ");
    assert_eq!(
        cell,
        Cell {
            symbol: Some(CompactString::const_new("あ")),
            symbol_reset: false,
            fg: Color::Reset,
            bg: Color::Reset,
            #[cfg(feature = "underline-color")]
            underline_color: Color::Reset,
            modifier: Modifier::empty(),
            diff_option: CellDiffOption::None,
            skip: false,
        }
    );
}

#[test]
fn empty() {
    let cell = Cell::EMPTY;
    assert_eq!(cell.symbol(), " ");
}

#[test]
fn set_symbol() {
    let mut cell = Cell::EMPTY;
    cell.set_symbol("あ"); // Multi-byte character
    assert_eq!(cell.symbol(), "あ");
    cell.set_symbol("👨‍👩‍👧‍👦"); // Multiple code units combined with ZWJ
    assert_eq!(cell.symbol(), "👨‍👩‍👧‍👦");
}

#[test]
fn append_symbol() {
    let mut cell = Cell::EMPTY;
    cell.set_symbol("あ"); // Multi-byte character
    cell.append_symbol("\u{200B}"); // zero-width space
    assert_eq!(cell.symbol(), "あ\u{200B}");
}

#[test]
fn set_char() {
    let mut cell = Cell::EMPTY;
    cell.set_char('あ'); // Multi-byte character
    assert_eq!(cell.symbol(), "あ");
}

#[test]
fn set_fg() {
    let mut cell = Cell::EMPTY;
    cell.set_fg(Color::Red);
    assert_eq!(cell.fg, Color::Red);
}

#[test]
fn set_bg() {
    let mut cell = Cell::EMPTY;
    cell.set_bg(Color::Red);
    assert_eq!(cell.bg, Color::Red);
}

#[test]
fn set_style() {
    let mut cell = Cell::EMPTY;
    cell.set_style(Style::new().fg(Color::Red).bg(Color::Blue));
    assert_eq!(cell.fg, Color::Red);
    assert_eq!(cell.bg, Color::Blue);
}

#[test]
fn set_skip() {
    let mut cell = Cell::EMPTY;
    cell.set_diff_option(CellDiffOption::Skip);
    assert_eq!(cell.diff_option, CellDiffOption::Skip);
}

#[test]
fn set_always_update() {
    let mut cell = Cell::EMPTY;
    cell.set_diff_option(CellDiffOption::AlwaysUpdate);
    assert_eq!(cell.diff_option, CellDiffOption::AlwaysUpdate);
}

#[test]
fn reset() {
    let mut cell = Cell::EMPTY;
    cell.set_symbol("あ");
    cell.set_fg(Color::Red);
    cell.set_bg(Color::Blue);
    cell.set_diff_option(CellDiffOption::Skip);
    cell.reset();
    assert_eq!(cell.symbol(), " ");
    assert_eq!(cell.fg, Color::Reset);
    assert_eq!(cell.bg, Color::Reset);
    assert_eq!(cell.diff_option, CellDiffOption::None);
}

#[test]
fn style() {
    let cell = Cell::EMPTY;
    assert_eq!(
        cell.style(),
        Style {
            fg: Some(Color::Reset),
            bg: Some(Color::Reset),
            #[cfg(feature = "underline-color")]
            underline_color: Some(Color::Reset),
            add_modifier: Modifier::empty(),
            sub_modifier: Modifier::empty(),
        }
    );
}

#[test]
fn default() {
    let cell = Cell::default();
    assert_eq!(cell.symbol(), " ");
}

#[test]
fn cell_eq() {
    let cell1 = Cell::new("あ");
    let cell2 = Cell::new("あ");
    assert_eq!(cell1, cell2);
}

#[test]
fn cell_ne() {
    let cell1 = Cell::new("あ");
    let cell2 = Cell::new("い");
    assert_ne!(cell1, cell2);
}

#[test]
fn retained_storage_preserves_missing_and_explicit_empty_symbols() {
    let mut cell = Cell::EMPTY;
    cell.set_symbol(
        "x\u{301}\u{301}\u{301}\u{301}\u{301}\u{301}\u{301}\u{301}\u{301}\u{301}\u{301}\u{301}",
    );
    cell.reset();
    assert_eq!(cell, Cell::EMPTY);
    assert_eq!(cell.symbol(), " ");
    cell.append_symbol("\u{301}");
    assert_eq!(cell.symbol(), "\u{301}");
    cell.reset();
    cell.set_symbol("");
    assert_eq!(cell.symbol(), "");
    cell.reset();
    cell.merge_symbol("┏", MergeStrategy::Exact);
    assert_eq!(cell.symbol(), "┏");
}
