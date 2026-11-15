//! The one integer-width conversion. Counts, lengths and limits that change
//! width go through [`saturate`], so no conversion truncates or wraps silently.

/// An integer type with the bounds a saturating conversion clamps to.
pub trait Bounded: Copy {
    /// The type's smallest value.
    const MIN: Self;
    /// The type's largest value.
    const MAX: Self;
}

macro_rules! bounded {
    ($($integer:ty),*) => {
        $(impl Bounded for $integer {
            const MIN: Self = <$integer>::MIN;
            const MAX: Self = <$integer>::MAX;
        })*
    };
}
bounded!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize
);

/// `value` in `T`, clamped to `T`'s bounds: a value that fits is exact, one
/// below zero that `T` cannot hold is `T::MIN`, and any other is `T::MAX`.
#[must_use]
pub fn saturate<T, U>(value: U) -> T
where
    T: Bounded + TryFrom<U>,
    U: Copy + PartialOrd + Default,
{
    T::try_from(value).unwrap_or(if value < U::default() { T::MIN } else { T::MAX })
}

#[cfg(test)]
mod tests {
    use super::saturate;

    #[test]
    fn values_that_fit_are_exact_and_others_clamp_to_the_nearest_bound() {
        assert_eq!(saturate::<u32, usize>(7), 7);
        assert_eq!(saturate::<u32, u64>(u64::MAX), u32::MAX);
        assert_eq!(saturate::<u64, i64>(-1), 0);
        assert_eq!(saturate::<i64, u64>(u64::MAX), i64::MAX);
        assert_eq!(saturate::<i8, i64>(i64::MIN), i8::MIN);
    }
}
