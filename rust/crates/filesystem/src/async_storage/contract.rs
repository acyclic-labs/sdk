// The contract is also its Arc delegation; method signatures have one source.
macro_rules! contract {
    ($(#[$doc:meta])* pub trait $name:ident: $base:path { $($body:tt)* }) => {
        $(#[$doc])* pub trait $name: $base { $($body)* }
        impl<T: $name + ?Sized> $name for Arc<T> { contract!(@methods $($body)*); }
    };
    (@methods) => {};
    (@methods $(#[$doc:meta])* fn $name:ident<$life:lifetime>
        (&$borrow:lifetime $receiver:ident $(, $arg:ident: $ty:ty)* $(,)?)
        -> Result<impl Future<Output = $out:ty> + ProviderTask + use<$capture:lifetime, Self>, $error:ty>
        $body:block $($rest:tt)*) => {
        fn $name<$life>(&$borrow $receiver $(, $arg: $ty)*)
            -> Result<impl Future<Output = $out> + ProviderTask + use<$capture, T>, $error> {
            (**$receiver).$name($($arg),*)
        }
        contract!(@methods $($rest)*);
    };
    (@methods $(#[$doc:meta])* fn $name:ident
        (&$receiver:ident $(, $arg:ident: $ty:ty)* $(,)?) -> $out:ty; $($rest:tt)*) => {
        fn $name(&$receiver $(, $arg: $ty)*) -> $out {
            (**$receiver).$name($($arg),*)
        }
        contract!(@methods $($rest)*);
    };
    (@methods $(#[$doc:meta])* fn $name:ident $(<$g:ident: $bound:path>)?
        (&$receiver:ident $(, $arg:ident: $ty:ty)* $(,)?)
        -> impl Future<Output = $out:ty $(,)?> + ProviderTask $end:tt $($rest:tt)*) => {
        fn $name $(<$g: $bound>)? (&$receiver $(, $arg: $ty)*)
            -> impl Future<Output = $out> + ProviderTask {
            // Forward the provider's actual future: shared ownership must not
            // insert another async frame or defer the provider's admission.
            (**$receiver).$name($($arg),*)
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
