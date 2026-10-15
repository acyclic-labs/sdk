// The contract is also its Arc delegation; method signatures have one source.
macro_rules! contract {
    ($(#[$doc:meta])* pub trait $name:ident: $base:path { $($body:tt)* }) => {
        $(#[$doc])* pub trait $name: $base { $($body)* }
        impl<T: $name + ?Sized> $name for Arc<T> { contract!(@methods $($body)*); }
    };
    (@methods) => {};
    (@methods $(#[$doc:meta])* fn $name:ident $(<$g:ident: $bound:path>)?
        (&$receiver:ident $(, $arg:ident: $ty:ty)* $(,)?)
        -> impl Future<Output = $out:ty $(,)?> + ProviderTask $end:tt $($rest:tt)*) => {
        async fn $name $(<$g: $bound>)? (&$receiver $(, $arg: $ty)*) -> $out {
            (**$receiver).$name($($arg),*).await
        }
        contract!(@methods $($rest)*);
    };
    (@methods $(#[$doc:meta])* fn $name:ident
        (&$receiver:ident $(, $arg:ident: $ty:ty)* $(,)?) -> $out:ty $body:block $($rest:tt)*) => {
        fn $name(&$receiver $(, $arg: $ty)*) -> $out {
            (**$receiver).$name($($arg),*)
        }
        contract!(@methods $($rest)*);
    };
}
