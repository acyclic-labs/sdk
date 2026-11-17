// The unrelated page cursor in the same source file needs this type to compile;
// the proof invokes the production Groups implementation directly.
pub struct Child<K, P> {
    pub first: K,
    pub page: P,
}

#[path = "../../crates/collections/src/page/selection.rs"]
mod production;

#[kani::proof]
#[kani::unwind(4)]
fn group_selection_failures_are_terminal() {
    let mode: u8 = kani::any();
    kani::assume(mode < 5);
    let keys: [u8; 3] = kani::any();
    if mode == 3 {
        kani::assume(keys[0] > keys[1]);
    }
    if mode == 4 {
        kani::assume(keys[0] < keys[1] && keys[1] > keys[2]);
    }
    let mut cursor = production::Groups::new(if mode == 0 { 3..2 } else { 0..3 });
    let select = |cursor: &mut production::Groups| {
        cursor.select(
            |position| {
                if mode == 1 {
                    Err("missing")
                } else {
                    keys.get(position).ok_or("missing")
                }
            },
            |left, right| {
                if mode == 2 {
                    Err("admission")
                } else {
                    Ok(left.cmp(right))
                }
            },
            || "invalid",
        )
    };
    let result = select(&mut cursor);
    if mode == 4 {
        assert_eq!(result, Ok(Some(0..1)));
        assert_eq!(select(&mut cursor), Err("invalid"));
    } else {
        assert_eq!(
            result,
            Err(match mode {
                1 => "missing",
                2 => "admission",
                _ => "invalid",
            })
        );
    }
    assert_eq!(
        cursor.select::<u8, _>(
            |_| panic!("terminal cursor examined a key"),
            |_, _| panic!("terminal cursor compared a key"),
            || "terminal",
        ),
        Err("terminal")
    );
}
