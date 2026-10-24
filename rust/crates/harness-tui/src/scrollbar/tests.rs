use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn source_track_is_bounded_monotone_and_reaches_both_ends(total in any::<usize>(), height in 2u16..64, row in any::<u16>(), top in any::<usize>(), visible in any::<usize>()) {
        let area=Rect::new(0,0,1,height);
        let at=TranscriptScrollbar::source_at(area,row,total);
        prop_assert!(at<=total);
        prop_assert_eq!(TranscriptScrollbar::source_at(area,0,total),0);
        prop_assert_eq!(TranscriptScrollbar::source_at(area,height-1,total),total);
        prop_assert!(TranscriptScrollbar::source_at(area,row.saturating_add(1),total)>=at);
        let mut buffer=Buffer::empty(area);
        let end=top.saturating_add(visible);
        TranscriptScrollbar{visible:top..end,total,theme:&ThemeStyles::default()}.render(area,&mut buffer);
        let thumb=buffer.content.iter().filter(|cell|cell.symbol()=="█").count();
        prop_assert!(thumb<=usize::from(height));
        prop_assert_eq!(thumb==0,total==0 || end.min(total).saturating_sub(top.min(total))==total);
    }
}

#[test]
#[should_panic(expected = "position must use wide multiplication")]
fn overflowing_source_product_is_detected_negative_control() {
    let expected = TranscriptScrollbar::source_at(Rect::new(0, 0, 1, 20), 19, usize::MAX);
    let broken = 19usize.wrapping_mul(usize::MAX) / 19;
    assert_eq!(broken, expected, "position must use wide multiplication");
}
