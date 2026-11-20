//! One `#[test]` per listed case, from one static list.
//!
//! [`cases!`](crate::cases!) expands a list of named cases into a module
//! with one test function per case, each calling the same runner with its
//! case. A generator's cases run, report and time out one by one under the
//! test runner, rather than as one sweep that holds a single per-test budget.
//! The list is static: a build script that discovers cases emits the whole
//! invocation for `include!`, since a declarative macro cannot expand one
//! in its matcher.

/// A runner's result: `()`, or a `Result` whose error fails the case.
pub trait Outcome {
    /// Fails the calling test when the runner reported an error.
    fn finish(self);
}

impl Outcome for () {
    fn finish(self) {}
}

impl<E: std::fmt::Display> Outcome for Result<(), E> {
    fn finish(self) {
        if let Err(error) = self {
            panic!("{error:#}");
        }
    }
}

/// Expands one `#[test]` per case into `mod <module>`.
///
/// ```
/// fn check(seed: u64) -> Result<(), String> {
///     (seed < 8).then_some(()).ok_or(format!("seed {seed} out of range"))
/// }
///
/// acyclic_test_support::cases! {
///     module = seeds,
///     values: u64,
///     runner = super::check,
///     cases = [seed_0 => 0, seed_1 => 1],
/// }
///
/// fn main() {
///     assert_eq!(seeds::VALUES, [0, 1]);
/// }
/// ```
///
/// - `runner` is a path resolved inside the generated module, so it is
///   absolute (`crate::…`) or starts with `super::`. It takes one case and
///   returns `()` or `Result<(), E: Display>`.
/// - `async runner = …` awaits the runner under `#[tokio::test]`, from the
///   calling crate's own `tokio`.
/// - `values: T` also emits `pub(crate) const VALUES: &[T]`, the cases in
///   order, so the list has one source.
#[macro_export]
macro_rules! cases {
    (module = $module:ident, values: $ty:ty, runner = $runner:path, cases = $cases:tt $(,)?) => {
        $crate::cases!(@module $module [$ty] [] $runner, $cases);
    };
    (module = $module:ident, values: $ty:ty, async runner = $runner:path, cases = $cases:tt $(,)?) => {
        $crate::cases!(@module $module [$ty] [async] $runner, $cases);
    };
    (module = $module:ident, runner = $runner:path, cases = $cases:tt $(,)?) => {
        $crate::cases!(@module $module [] [] $runner, $cases);
    };
    (module = $module:ident, async runner = $runner:path, cases = $cases:tt $(,)?) => {
        $crate::cases!(@module $module [] [async] $runner, $cases);
    };
    (@module $module:ident [] $kind:tt $runner:path,
     [$($case:ident => $value:expr),* $(,)?]) => {
        mod $module {
            $crate::cases!(@tests $kind $runner; $($case => $value),*);
        }
    };
    (@module $module:ident [$ty:ty] $kind:tt $runner:path,
     [$($case:ident => $value:expr),* $(,)?]) => {
        mod $module {
            pub(crate) const VALUES: &[$ty] = &[$($value),*];
            $crate::cases!(@tests $kind $runner; $($case => $value),*);
        }
    };
    (@tests [] $runner:path; $($case:ident => $value:expr),*) => {
        $(
            #[test]
            fn $case() {
                $crate::cases::Outcome::finish($runner($value));
            }
        )*
    };
    (@tests [async] $runner:path; $($case:ident => $value:expr),*) => {
        $(
            #[::tokio::test]
            async fn $case() {
                $crate::cases::Outcome::finish($runner($value).await);
            }
        )*
    };
}

#[cfg(test)]
mod tests {
    use super::Outcome;

    fn even(value: u64) -> Result<(), String> {
        value
            .is_multiple_of(2)
            .then_some(())
            .ok_or_else(|| format!("{value} is odd"))
    }

    crate::cases! {
        module = evens,
        runner = super::even,
        cases = [zero => 0, two => 2, four => 4],
    }

    #[test]
    #[should_panic(expected = "3 is odd")]
    fn negative_control_an_erring_case_fails_its_test() {
        even(3).finish();
    }
}
