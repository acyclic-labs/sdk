// The unrelated page cursor in the same source file needs this type to compile;
// the proof invokes the production Groups implementation directly.
pub struct Child<K, P> {
    pub first: K,
    pub page: P,
}

#[path = "../../crates/collections/src/page/selection.rs"]
mod production;

#[derive(Debug, PartialEq, Eq)]
enum Failure {
    Missing,
    Admission,
    Invalid,
    Terminal,
}

#[kani::proof]
#[kani::unwind(4)]
fn group_selection_failures_are_terminal() {
    let mode: u8 = kani::any();
    kani::assume(mode < 7);
    let keys: [u8; 3] = kani::any();
    if mode == 3 {
        kani::assume(keys[0] > keys[1]);
    }
    if mode == 4 {
        kani::assume(keys[0] < keys[1] && keys[1] > keys[2]);
    }
    if mode == 6 {
        kani::assume(keys[0] == keys[1] && keys[1] != keys[2]);
    }
    let mut cursor = production::Groups::new(if mode == 0 { 3..2 } else { 0..3 });
    let select = |cursor: &mut production::Groups| {
        cursor.select(
            |position| {
                if mode == 1 || (mode == 5 && position == 1) {
                    Err(Failure::Missing)
                } else {
                    keys.get(position).ok_or(Failure::Missing)
                }
            },
            |left, right| {
                if mode == 2 || (mode == 6 && *right == keys[2]) {
                    Err(Failure::Admission)
                } else {
                    Ok(left.cmp(right))
                }
            },
            || Failure::Invalid,
        )
    };
    let result = select(&mut cursor);
    if mode == 4 {
        assert_eq!(result, Ok(Some(0..1)));
        assert_eq!(select(&mut cursor), Err(Failure::Invalid));
    } else {
        assert_eq!(
            result,
            Err(match mode {
                1 | 5 => Failure::Missing,
                2 | 6 => Failure::Admission,
                _ => Failure::Invalid,
            })
        );
    }
    assert_eq!(
        cursor.select::<u8, _>(
            |_| panic!("terminal cursor examined a key"),
            |_, _| panic!("terminal cursor compared a key"),
            || Failure::Terminal,
        ),
        Err(Failure::Terminal)
    );
}
