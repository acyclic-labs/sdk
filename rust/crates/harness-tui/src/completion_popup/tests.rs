use super::*;
use proptest::prelude::*;

fn allocation_bound(bytes: usize, long_label: bool, broken: bool) {
    let name = if long_label {
        "x".repeat(bytes)
    } else {
        "theme".into()
    };
    let description = if long_label {
        String::new()
    } else {
        "d".repeat(bytes)
    };
    let data =
        serde_json::to_vec(&serde_json::json!([{"name":name,"description":description}])).unwrap();
    let mut menu = CompletionMenu::default();
    menu.set_file("commands/catalog.json", Some(&data)).unwrap();
    let editor = Editor::new("/");
    menu.refresh(&editor);
    let area = Rect::new(0, 0, 12, 3);
    let mut buffer = Buffer::empty(area);
    let measured = acyclic_test_support::measure(|| {
        if broken {
            // Reproduce whole-row formatting before clipping. The same bound
            // must reject this even though its visible output is identical.
            let item = menu.items().next().unwrap();
            let text = format!("{}  {}", item.label, item.description);
            let sanitized: String = text
                .chars()
                .map(|c| if c.is_control() { '�' } else { c })
                .collect();
            std::hint::black_box(sanitized);
        }
        CompletionPopup::new(&menu, &editor).render(area, &mut buffer);
    });
    assert!(
        measured.bytes_total <= 4096,
        "popup allocation must depend on visible cells: {measured:?}"
    );
    let row: String = (1..11).map(|x| buffer[(x, 1)].symbol()).collect();
    assert_eq!(
        row,
        if long_label {
            "/xxxxxxxxx"
        } else {
            "/theme  dd"
        }
    );
}

#[test]
fn megabyte_labels_and_descriptions_have_bounded_redraw_allocations() {
    allocation_bound(1 << 20, false, false);
    allocation_bound(1 << 20, true, false);
}

#[test]
#[should_panic(expected = "popup allocation must depend on visible cells")]
fn allocation_negative_control_rejects_full_row_copies() {
    allocation_bound(1 << 20, false, true);
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]
    #[test]
    fn redraw_allocation_is_independent_of_source_length(bytes in 64usize..16384, label in any::<bool>()) {
        allocation_bound(bytes, label, false);
    }
}

#[test]
fn control_sanitizing_and_unbounded_combining_clusters_stay_cell_bounded() {
    let area = Rect::new(0, 0, 12, 1);
    let mut buffer = Buffer::empty(area);
    let mut x = 0;
    render_part(
        "a\r\nb",
        &mut x,
        area.right(),
        0,
        Style::default(),
        &mut buffer,
    );
    assert_eq!(x, 4);
    assert_eq!(buffer[(1, 0)].symbol(), "�");
    assert_eq!(buffer[(2, 0)].symbol(), "�");
    let cluster = format!("a{}", "\u{301}".repeat(1 << 20));
    x = 0;
    let measured = acyclic_test_support::measure(|| {
        render_part(
            &cluster,
            &mut x,
            area.right(),
            0,
            Style::default(),
            &mut buffer,
        );
    });
    assert_eq!(buffer[(0, 0)].symbol(), "�");
    assert_eq!(x, 1);
    assert!(measured.bytes_total <= 4096);
}
