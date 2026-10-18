use super::*;

fn cells(bytes: &[u8]) -> Vec<String> {
    let mut term = Term::new(Config::default(), &Grid, Events);
    let mut parser: Processor = Processor::new();
    parser.advance(&mut term, bytes);
    parser.stop_sync(&mut term);
    (0..i32::from(HEIGHT))
        .flat_map(|row| (0..usize::from(WIDTH)).map(move |column| (row, column)))
        .map(|(row, column)| format!("{:?}", term.grid()[Point::new(Row(row), Column(column))]))
        .collect()
}

fn check_attributes<const KEEP_RESET: bool>(during_render: bool, changed: bool, hidden: bool) {
    // An independent old per-command terminal supplies all cells, colours and
    // attributes, rather than sharing the candidate's journal or buffer.
    let cursor = if hidden { None } else { Some((3, 2)) };
    let base: Scene = (vec!["a".into()], cursor);
    let target: Scene = (vec![if changed { "b" } else { "a" }.into()], cursor);
    let last: Scene = (vec!["b".into()], cursor);
    let old = per_command(&[base.clone(), target.clone(), last.clone()]).unwrap();
    let device = Device::default();
    let guard = Rc::new(RefCell::new(RestoreGuard::new(device.clone(), &caps(true))));
    let size = Size::new(WIDTH, HEIGHT);
    let mut terminal = FrameTerminal::new(Shared(Rc::clone(&guard), true), size).unwrap();
    terminal.draw(size, |frame| paint(frame, &base)).unwrap();
    let mut bytes = device.take().concat();
    let outside = || {
        guard
            .borrow_mut()
            .writer()
            .write_all(b"\x1b[31;44;1;3;4m")
            .unwrap()
    };
    if !during_render {
        outside();
    }
    terminal
        .draw(size, |frame| {
            paint(frame, &target);
            if during_render {
                outside();
            }
        })
        .unwrap();
    let mut recovered = device.take().concat();
    if !KEEP_RESET {
        // Restore the skipped-attribute-reset bug while preserving all cursor
        // recovery, lifecycle checks, diff cells and trailing resets.
        let at = recovered
            .windows(4)
            .position(|bytes| bytes == b"\x1b[0m")
            .unwrap();
        recovered.drain(at..at + 4);
    }
    bytes.extend(recovered);
    assert_eq!(
        cells(&bytes),
        cells(&old[..2].concat().concat()),
        "outside output must not leak attributes into default cells"
    );
    terminal.draw(size, |frame| paint(frame, &last)).unwrap();
    bytes.extend(device.take().concat());
    assert_eq!(
        cells(&bytes),
        cells(&old.concat().concat()),
        "outside output must not leak attributes into default cells"
    );
    terminal.draw(size, |frame| paint(frame, &last)).unwrap();
    assert!(
        device.take().is_empty(),
        "recovered attributes permit a zero-write repeat"
    );
}

#[test]
fn outside_attributes_are_recovered_for_empty_and_changed_frames() {
    for during_render in [false, true] {
        for changed in [false, true] {
            for hidden in [false, true] {
                check_attributes::<true>(during_render, changed, hidden);
            }
        }
    }
}

#[test]
#[should_panic(expected = "outside output must not leak attributes into default cells")]
fn negative_control_skipping_only_the_attribute_reset_is_detected() {
    check_attributes::<false>(false, false, true);
}
