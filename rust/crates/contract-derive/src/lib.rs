//! Derive first-party wire shadows and schema from semantic declarations.
use proc_macro::TokenStream;
use syn::parse_macro_input;
mod enumeration;
mod message;
mod options;

/// Derive a prost shadow and fallible semantic ingress.
///
/// Oneof tags have one authority: its cases.
/// ```compile_fail
/// use acyclic_contract_derive::{message, oneof};
/// #[oneof(error = core::convert::Infallible)]
/// enum Case { #[wire(tag = 1)] Flag(bool) }
/// #[message(error = core::convert::Infallible, package = "acyclic.test.v1")]
/// struct Invalid { #[wire(oneof = "1", tag = 27)] choice: Option<Case> }
/// ```
/// Nested references must belong to the enclosing first-party package.
/// ```compile_fail
/// use acyclic_contract_derive::message;
/// use core::convert::Infallible;
/// #[message(error = Infallible, package = "acyclic.foreign.v1")]
/// struct Foreign { #[wire(tag = 1)] number: u64 }
/// #[message(error = Infallible, package = "acyclic.local.v1")]
/// struct Local { #[wire(message, tag = 1)] child: Option<Foreign> }
/// ```
/// Oneof nesting carries the same package requirement.
/// ```compile_fail
/// use acyclic_contract_derive::{message, oneof};
/// use core::convert::Infallible;
/// #[message(error = Infallible, package = "acyclic.foreign.v1")]
/// struct Foreign { #[wire(tag = 1)] number: u64 }
/// #[oneof(error = Infallible)]
/// enum Choice { #[wire(message, tag = 1)] Child(Foreign) }
/// #[message(error = Infallible, package = "acyclic.local.v1")]
/// struct Local { #[wire(oneof = "1")] child: Option<Choice> }
/// ```
/// A nominal enum reference must use its unique file owner's package.
/// ```compile_fail
/// use acyclic_contract_derive::{enumeration, file, message};
/// #[derive(Default)]
/// enum Error { #[default] Missing, Unknown(i32) }
/// #[enumeration(error = Error, unknown = Error::Unknown)]
/// enum State { Unspecified = 0, Ready = 1 }
/// #[file(family = "foreign", enums(State))]
/// struct Foreign;
/// #[message(error = Error, package = "acyclic.local.v1")]
/// struct Local { #[wire(enumeration = State, tag = 1)] state: State }
/// ```
#[proc_macro_attribute]
pub fn message(args: TokenStream, item: TokenStream) -> TokenStream {
    let mut options = options::Options::default();
    let parser = syn::meta::parser(|meta| options.message(&meta));
    parse_macro_input!(args with parser);
    let item = parse_macro_input!(item as syn::ItemStruct);
    message::expand(item, options)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Derive a semantic enum that rejects every unknown wire integer.
#[proc_macro_attribute]
pub fn enumeration(args: TokenStream, item: TokenStream) -> TokenStream {
    let mut options = options::Options::default();
    let parser = syn::meta::parser(|meta| options.enumeration(&meta));
    parse_macro_input!(args with parser);
    let item = parse_macro_input!(item as syn::ItemEnum);
    enumeration::expand(&item, options)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Derive present-payload wire cases and fallible semantic ingress.
/// Nested cases cannot mix first-party packages.
/// ```compile_fail
/// use acyclic_contract_derive::{message, oneof};
/// use core::convert::Infallible;
/// #[message(error = Infallible, package = "acyclic.a.v1")]
/// struct A { #[wire(tag = 1)] number: u64 }
/// #[message(error = Infallible, package = "acyclic.b.v1")]
/// struct B { #[wire(tag = 1)] number: u64 }
/// #[oneof(error = Infallible)]
/// enum Choice { #[wire(message, tag = 1)] A(A), #[wire(message, tag = 2)] B(B) }
/// ```
#[proc_macro_attribute]
pub fn oneof(args: TokenStream, item: TokenStream) -> TokenStream {
    let mut options = options::Options::default();
    let parser = syn::meta::parser(|meta| {
        if !meta.path.is_ident("error") {
            return Err(meta.error("unknown oneof option"));
        }
        options.message(&meta)
    });
    parse_macro_input!(args with parser);
    let item = parse_macro_input!(item as syn::ItemEnum);
    message::oneof(item, options)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

mod registration;
/// Register a canonical protobuf file from first-party semantic declarations.
/// A registered service cannot silently refer to another family's messages.
/// ```compile_fail
/// use acyclic_contract_derive::{file, message, service};
/// use core::convert::Infallible;
/// #[message(error = Infallible, package = "acyclic.foreign.v1")]
/// struct Foreign { #[wire(tag = 1)] number: u64 }
/// #[service]
/// enum Service { Call { request: ForeignProto, response: ForeignProto } }
/// #[file(family = "local", services(Service))]
/// struct Local;
/// ```
/// An enum has one file owner.
/// ```compile_fail
/// use acyclic_contract_derive::{enumeration, file};
/// enum Error { Unknown(i32) }
/// #[enumeration(error = Error, unknown = Error::Unknown)]
/// enum State { Unspecified = 0 }
/// #[file(family = "a", enums(State))]
/// struct A;
/// #[file(family = "b", enums(State))]
/// struct B;
/// ```
#[proc_macro_attribute]
pub fn file(args: TokenStream, item: TokenStream) -> TokenStream {
    let item = parse_macro_input!(item as syn::ItemStruct);
    registration::file(args.into(), &item)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
/// Derive service schema from compiler-linked request and response types.
///
/// Each method has exactly one request and one response.
/// ```compile_fail
/// #[derive(Clone, PartialEq, prost::Message)]
/// struct Body {}
/// impl prost::Name for Body {
///     const NAME: &'static str = "Body";
///     const PACKAGE: &'static str = "acyclic.test.v1";
/// }
/// #[acyclic_contract_derive::service]
/// enum Invalid { Call { request: Body, request: Body, response: Body } }
/// ```
/// Request and response types share one first-party package.
/// ```compile_fail
/// use acyclic_contract_derive::{message, service};
/// use core::convert::Infallible;
/// #[message(error = Infallible, package = "acyclic.a.v1")]
/// struct A { #[wire(tag = 1)] number: u64 }
/// #[message(error = Infallible, package = "acyclic.b.v1")]
/// struct B { #[wire(tag = 1)] number: u64 }
/// #[service]
/// enum Service { Call { request: AProto, response: BProto } }
/// ```
#[proc_macro_attribute]
pub fn service(args: TokenStream, item: TokenStream) -> TokenStream {
    if !args.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "service takes no declaration options",
        )
        .into_compile_error()
        .into();
    }
    let item = parse_macro_input!(item as syn::ItemEnum);
    registration::service(item)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
