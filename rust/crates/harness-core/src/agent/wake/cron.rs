use super::invalid;
use crate::agent::Error;
use jiff::{Timestamp, civil::Date, tz::TimeZone};

/// Compiled five-field cron. Calendar work happens only when a file is edited
/// or fires, never on a scheduler tick. Fields accept lists, ranges and steps.
#[derive(Clone, Debug)]
pub struct Cron {
    minutes: u64,
    hours: u64,
    days: u64,
    months: u64,
    weekdays: u64,
    any_day: bool,
    any_weekday: bool,
}

impl Cron {
    /// Compile the native five-field calendar expression.
    pub fn parse(expression: &str) -> Result<Self, Error> {
        let mut fields = expression.split_whitespace();
        let mut take = || fields.next().ok_or(invalid("cron requires five fields"));
        let minutes = field(take()?, 0, 59)?;
        let hours = field(take()?, 0, 23)?;
        let day = take()?;
        let days = field(day, 1, 31)?;
        let months = field(take()?, 1, 12)?;
        let weekday = take()?;
        let weekdays = field(weekday, 0, 7)?;
        if fields.next().is_some() {
            return Err(invalid("cron requires five fields"));
        }
        Ok(Self {
            minutes,
            hours,
            days,
            months,
            weekdays: (weekdays & 0x7f) | ((weekdays >> 7) & 1),
            any_day: day.starts_with('*'),
            any_weekday: weekday.starts_with('*'),
        })
    }

    fn matches(&self, date: Date) -> bool {
        let month = self.months & (1 << date.month()) != 0;
        let day = self.days & (1 << date.day()) != 0;
        let weekday = self.weekdays & (1 << date.weekday().to_sunday_zero_offset()) != 0;
        month
            && if self.any_day || self.any_weekday {
                day && weekday
            } else {
                day || weekday
            }
    }

    /// First occurrence strictly after `after`, in Unix milliseconds.
    /// Nonexistent local times are skipped; both instants of a repeated local
    /// time are occurrences. A restart's missed policy is applied by Schedule.
    pub fn next(&self, after: u64, timezone: &str) -> Result<Option<u64>, Error> {
        let zone = TimeZone::get(timezone).map_err(|_| invalid("unknown cron timezone"))?;
        let timestamp = Timestamp::from_millisecond(
            i64::try_from(after).map_err(|_| invalid("cron timestamp exceeds calendar range"))?,
        )
        .map_err(|_| invalid("cron timestamp exceeds calendar range"))?;
        let mut date = timestamp.to_zoned(zone.clone()).date();
        // Gregorian dates and weekdays repeat every 400 years. This is a proof
        // bound for an impossible calendar predicate, not a lifetime/count cap.
        for _ in 0..=146_097 {
            if self.matches(date) {
                let mut first = None;
                for hour in 0..24i8 {
                    if self.hours & (1 << hour) == 0 {
                        continue;
                    }
                    for minute in 0..60i8 {
                        if self.minutes & (1 << minute) == 0 {
                            continue;
                        }
                        let local = date.at(hour, minute, 0, 0);
                        let ambiguous = zone.to_ambiguous_zoned(local);
                        for candidate in [ambiguous.clone().earlier(), ambiguous.later()] {
                            let Ok(candidate) = candidate else { continue };
                            if candidate.datetime() != local {
                                continue;
                            }
                            let Ok(millis) = u64::try_from(candidate.timestamp().as_millisecond())
                            else {
                                continue;
                            };
                            if millis > after && first.is_none_or(|current| millis < current) {
                                first = Some(millis);
                            }
                        }
                    }
                }
                if first.is_some() {
                    return Ok(first);
                }
            }
            date = match date.tomorrow() {
                Ok(date) => date,
                Err(_) => return Ok(None),
            };
        }
        Ok(None)
    }
}

fn field(text: &str, min: u8, max: u8) -> Result<u64, Error> {
    let number = |text: &str| {
        text.parse::<u8>()
            .map_err(|_| invalid("invalid cron number"))
    };
    let mut bits = 0;
    for member in text.split(',') {
        let (range, stride) = match member.split_once('/') {
            Some((range, stride)) => (range, number(stride)?),
            None => (member, 1),
        };
        if stride == 0 {
            return Err(invalid("cron step is zero"));
        }
        let (start, end) = match range {
            "*" => (min, max),
            range => match range.split_once('-') {
                Some((start, end)) => (number(start)?, number(end)?),
                None => {
                    let start = number(range)?;
                    (start, if member.contains('/') { max } else { start })
                }
            },
        };
        if start < min || end > max || start > end {
            return Err(invalid("cron range is invalid"));
        }
        for value in (start..=end).step_by(usize::from(stride)) {
            bits |= 1u64 << value;
        }
    }
    Ok(bits)
}
