// Contracts generate transparent forwarding from their original signatures.
macro_rules! contract {
    ($(#[$doc:meta])* pub trait $name:ident: $base:path { $($body:tt)* }) => {
        $(#[$doc])* pub trait $name: $base { $($body)* }
        contract!(@parse [$name] [] [] $($body)*);
    };
    ($delegate:ident; $(#[$doc:meta])* pub trait $name:ident: $base:path { $($body:tt)* }) => {
        $(#[$doc])* pub trait $name: $base { $($body)* }
        contract!(@parse [$name] [$delegate] [] $($body)*);
    };
    (@parse [$name:ident] [$($delegate:ident)?] [$($methods:tt)*]) => {
        contract!(@build [$name] [$($delegate)?] [$($methods)*] $);
    };
    (@parse [$trait:ident] [$($delegate:ident)?] [$($methods:tt)*]
        $(#[$doc:meta])* fn $name:ident<$life:lifetime>
        (&$borrow:lifetime $receiver:ident $(, $arg:ident: $ty:ty)* $(,)?)
        -> Result<impl Future<Output = $out:ty> + ProviderTask + use<$capture:lifetime, Self>, $error:ty>
        $body:block $($rest:tt)*) => {
        contract!(@parse [$trait] [$($delegate)?] [$($methods)*
            ($name [<$life>] [&$borrow] $receiver [$($arg: $ty),*]
                [Result<impl Future<Output = $out> + ProviderTask + use<$capture, Self>, $error>]
                [Result<impl Future<Output = $out> + ProviderTask + use<$capture, T>, $error>])
        ] $($rest)*);
    };
    (@parse [$trait:ident] [$($delegate:ident)?] [$($methods:tt)*]
        $(#[$doc:meta])* fn $name:ident $(<$g:ident: $bound:path>)?
        (&$receiver:ident $(, $arg:ident: $ty:ty)* $(,)?) -> $out:ty; $($rest:tt)*) => {
        contract!(@parse [$trait] [$($delegate)?] [$($methods)*
            ($name [$(<$g: $bound>)?] [&] $receiver [$($arg: $ty),*] [$out] [$out])
        ] $($rest)*);
    };
    (@parse [$trait:ident] [$($delegate:ident)?] [$($methods:tt)*]
        $(#[$doc:meta])* fn $name:ident $(<$g:ident: $bound:path>)?
        (&$receiver:ident $(, $arg:ident: $ty:ty)* $(,)?) -> $out:ty $body:block $($rest:tt)*) => {
        contract!(@parse [$trait] [$($delegate)?] [$($methods)*
            ($name [$(<$g: $bound>)?] [&] $receiver [$($arg: $ty),*] [$out] [$out])
        ] $($rest)*);
    };
    (@build [$trait:ident] [$($delegate:ident)?]
        [$(($name:ident [$($generic:tt)*] [$($borrow:tt)*] $receiver:ident
            [$($arg:ident: $ty:ty),*] [$out:ty] [$arc_out:ty]))*] $d:tt) => {
        impl<T: $trait + ?Sized> $trait for Arc<T> {
            $(fn $name $($generic)* ($($borrow)* $receiver $(, $arg: $ty)*) -> $arc_out {
                // Preserve the actual future and synchronous admission.
                (**$receiver).$name($($arg),*)
            })*
        }
        contract!(@delegate [$($delegate)?] [$(($name [$($generic)*] [$($borrow)*]
            [$($arg: $ty),*] [$out]))*] $d);
    };
    (@delegate [] [$($methods:tt)*] $d:tt) => {};
    // Carry the dollar token into a nested, method-selective forwarding macro.
    (@delegate [$delegate:ident]
        [$(($name:ident [$($generic:tt)*] [$($borrow:tt)*]
            [$($arg:ident: $ty:ty),*] [$out:ty]))*] $d:tt) => {
        macro_rules! $delegate {
            $((@ $name, $d receiver:ident => $d target:expr $d(, $d before:block)?) => {
                fn $name $($generic)* ($($borrow)* $d receiver $(, $arg: $ty)*) -> $out {
                    $d($d before)?
                    ($d target).$name($($arg),*)
                }
            };)*
            ($d receiver:ident => $d target:expr; $d($d method:ident $d($d before:block)?),* $d(,)?) => {
                $d($crate::async_storage::$delegate!(@ $d method, $d receiver => $d target $d(, $d before)?);)*
            };
        }
        pub(crate) use $delegate;
    };
}
