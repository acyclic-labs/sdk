use super::*;
use proptest::prelude::*;

fn oracle(scroll: RegionScroll, area: Rect, cells: usize) -> Option<Range<usize>> {
    let r = scroll.region;
    let top = u64::from(r.y);
    let end = top + u64::from(r.height);
    let screen_top = u64::from(area.y);
    let screen_end = screen_top + u64::from(area.height);
    if r.x != area.x
        || r.width != area.width
        || area.width == 0
        || top < screen_top
        || end > screen_end
        || scroll.rows == 0
        || scroll.rows.unsigned_abs() >= usize::from(r.height)
    {
        return None;
    }
    let first = (top - screen_top) * u64::from(area.width);
    let last = (end - screen_top) * u64::from(area.width);
    (last <= cells as u64).then_some(first as usize..last as usize)
}

fn check(scroll: RegionScroll, area: Rect, cells: usize) {
    let actual = Band::checked(scroll, area, cells);
    assert_eq!(
        actual.as_ref().map(|band| band.cells.clone()),
        oracle(scroll, area, cells)
    );
    if let Some(band) = actual {
        assert!(band.cells.start < band.cells.end && band.cells.end <= cells);
        assert!(band.moved > 0 && band.moved < band.cells.len());
        assert_eq!(
            band.moved,
            scroll.rows.unsigned_abs() * usize::from(area.width)
        );
        assert_eq!(band.top, u32::from(scroll.region.y) + 1);
        assert_eq!(
            band.bottom,
            u32::from(scroll.region.y) + u32::from(scroll.region.height)
        );
        assert_eq!(band.up, scroll.rows > 0);
    }
}

#[test]
fn widened_endpoints_cover_saturated_boundaries() {
    let edges = [0, 1, 2, u16::MAX - 2, u16::MAX - 1, u16::MAX];
    for screen_y in edges {
        for screen_height in edges {
            for y in edges {
                for height in edges {
                    for width in [0, 1, 2] {
                        for rows in [isize::MIN, -2, -1, 0, 1, 2, isize::MAX] {
                            let area = Rect {
                                x: 0,
                                y: screen_y,
                                width,
                                height: screen_height,
                            };
                            let scroll = RegionScroll {
                                region: Rect {
                                    x: 0,
                                    y,
                                    width,
                                    height,
                                },
                                rows,
                            };
                            let cells = usize::from(width) * usize::from(screen_height);
                            check(scroll, area, cells);
                        }
                    }
                }
            }
        }
    }
}

proptest! {
    #[test]
    fn all_raw_rectangles_obey_checked_storage_and_device_bounds(
        screen in any::<[u16; 4]>(), region in any::<[u16; 4]>(), rows in any::<isize>(),
        available in any::<u32>(),
    ) {
        let [x,y,width,height] = screen;
        let area = Rect { x,y,width,height };
        let [x,y,width,height] = region;
        check(RegionScroll { region: Rect { x,y,width,height }, rows }, area, available as usize);
        // Also exercise accepted full-width bands, rather than depending on random equality.
        check(RegionScroll { region: Rect { x: area.x,y,width: area.width,height }, rows: rows % 65536 },
            area, usize::from(area.width) * usize::from(area.height));
    }
}

#[test]
fn saturated_hint_is_rejected_before_mutation_or_output() {
    let mut shown = Buffer::empty(Rect::new(0, 0, 1, u16::MAX));
    shown.content.last_mut().unwrap().set_symbol("tail");
    let mut output = Vec::new();
    let hint = RegionScroll {
        region: Rect {
            x: 0,
            y: 65534,
            width: 1,
            height: 2,
        },
        rows: 1,
    };
    assert!(!hint.apply(&mut shown, &mut output).unwrap());
    assert!(output.is_empty());
    assert_eq!(shown.content.last().unwrap().symbol(), "tail");
}
