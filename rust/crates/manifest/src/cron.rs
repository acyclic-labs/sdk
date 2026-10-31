//! Five-field UTC cron admission. Field expansion is at most 60 values.
fn number(text: &str) -> Option<u8> {
    text.as_bytes()
        .first()
        .filter(|digit| digit.is_ascii_digit())?;
    text.parse().ok()
}
fn field(text: &str, min: u8, max: u8) -> Option<u64> {
    let mut mask = 0;
    for item in text.split(',') {
        let (span, step, stepped) = match item.split_once('/') {
            Some((span, step)) => (span, number(step)?, true),
            None => (item, 1, false),
        };
        if step == 0 || step > max - min + 1 {
            return None;
        }
        let (first, last) = if span == "*" {
            (min, max)
        } else if let Some((first, last)) = span.split_once('-') {
            (number(first)?, number(last)?)
        } else {
            let first = number(span)?;
            (first, if stepped { max } else { first })
        };
        if first < min || last > max || first > last {
            return None;
        }
        for value in (first..=last).step_by(usize::from(step)) {
            mask |= 1u64 << value;
        }
    }
    (mask != 0).then_some(mask)
}
pub(crate) fn valid(schedule: &str) -> bool {
    let mut fields = schedule.split(' ');
    let (Some(minute), Some(hour), Some(day), Some(month), Some(weekday), None) = (
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
    ) else {
        return false;
    };
    let (Some(_), Some(_), Some(days), Some(months), Some(_)) = (
        field(minute, 0, 59),
        field(hour, 0, 23),
        field(day, 1, 31),
        field(month, 1, 12),
        field(weekday, 0, 6),
    ) else {
        return false;
    };
    // A restricted weekday can supply a fire via OR even if the day is impossible.
    if weekday != "*" {
        return true;
    }
    [31u8, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        .into_iter()
        .enumerate()
        .any(|(index, length)| {
            months & (1 << (index + 1)) != 0 && (1..=length).any(|day| days & (1 << day) != 0)
        })
}
