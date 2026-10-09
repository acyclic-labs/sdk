//! Derive first-party wire shadows and schema from semantic declarations.
use proc_macro::TokenStream;
use syn::parse_macro_input;
mod enumeration;
mod message;
mod options;
mod registration;

/// Derive a registered prost shadow and fallible semantic ingress.
///
/// Oneof tags have one authority: its cases.
/// ```compile_fail
/// use acyclic_contract_derive::{file, message, oneof};
/// use core::convert::Infallible;
/// #[oneof(error = Infallible, file = File)]
/// enum Case { #[wire(tag = 1)] Flag(bool) }
/// #[message(error = Infallible, file = File)]
/// struct Invalid { #[wire(oneof = "1", tag = 27)] choice: Option<Case> }
/// #[file(family = "test", messages(InvalidProto))]
/// struct File;
/// ```
///
/// Nested references must belong to the exact enclosing file.
/// ```compile_fail
/// use acyclic_contract_derive::{file, message};
/// use core::convert::Infallible;
/// #[message(error = Infallible, file = ForeignFile)]
/// struct Foreign { #[wire(tag = 1)] number: u64 }
/// #[message(error = Infallible, file = LocalFile)]
/// struct Local { #[wire(message, tag = 1)] child: Option<Foreign> }
/// #[file(family = "foreign", messages(ForeignProto))] struct ForeignFile;
/// #[file(family = "local", messages(LocalProto))] struct LocalFile;
/// ```
///
/// Inline oneofs carry the same exact owner requirement.
/// ```compile_fail
/// use acyclic_contract_derive::{file, message, oneof};
/// use core::convert::Infallible;
/// #[message(error = Infallible, file = File)]
/// struct Local { #[wire(oneof = "1")] child: Option<Choice> }
/// #[oneof(error = Infallible, file = OtherFile)]
/// enum Choice { #[wire(tag = 1)] Flag(bool) }
/// #[message(error = Infallible, file = OtherFile)] struct Other {}
/// #[file(family = "test", messages(LocalProto))] struct File;
/// #[file(family = "test", messages(OtherProto))] struct OtherFile;
/// ```
///
/// Enum wire identity comes solely from the semantic field type.
/// ```compile_fail
/// use acyclic_contract_derive::{enumeration, file, message};
/// #[derive(Default)] enum Error { #[default] Missing, Unknown(i32) }
/// #[enumeration(error = Error, unknown = Error::Unknown)] enum State { Unspecified = 0, Ready = 1 }
/// #[enumeration(error = Error, unknown = Error::Unknown)] enum Other { Unspecified = 0, Ready = 2 }
/// #[message(error = Error, file = File)]
/// struct View { #[wire(enumeration = Other, tag = 1)] state: State }
/// #[file(family = "test", messages(ViewProto), enums(State, Other))] struct File;
/// ```
///
/// Enum admission and encoding cannot be replaced by scalar conversion hooks.
/// ```compile_fail
/// use acyclic_contract_derive::{enumeration, file, message};
/// #[derive(Default)] enum Error { #[default] Missing, Unknown(i32) }
/// #[enumeration(error = Error, unknown = Error::Unknown)] enum State { Unspecified = 0, Ready = 1 }
/// fn admit(_: i32) -> Result<State, Error> { Ok(State::Ready) }
/// fn emit(_: State) -> i32 { 2 }
/// #[message(error = Error, file = File)]
/// struct View { #[wire(enumeration, from = admit, into = emit, tag = 1)] state: State }
/// #[file(family = "test", messages(ViewProto), enums(State))] struct File;
/// ```
///
/// A nominal enum must use its unique file owner.
/// ```compile_fail
/// use acyclic_contract_derive::{enumeration, file, message};
/// #[derive(Default)] enum Error { #[default] Missing, Unknown(i32) }
/// #[enumeration(error = Error, unknown = Error::Unknown)]
/// enum State { Unspecified = 0, Ready = 1 }
/// #[file(family = "foreign", enums(State))] struct Foreign;
/// #[message(error = Error, file = LocalFile)]
/// struct Local { #[wire(enumeration, tag = 1)] state: State }
/// #[file(family = "local", messages(LocalProto))] struct LocalFile;
/// ```
///
/// Identical protobuf names cannot impersonate a registered Rust type.
/// ```compile_fail
/// use acyclic_contract_derive::{file, message};
/// use core::convert::Infallible;
/// #[message(error = Infallible, file = File, name = "Record")]
/// struct Registered { #[wire(tag = 1)] text: String }
/// #[message(error = Infallible, file = File, name = "Record")]
/// struct Shadow { #[wire(tag = 1)] different: u64 }
/// #[message(error = Infallible, file = File)]
/// struct Parent { #[wire(message, tag = 1)] child: Option<Shadow> }
/// #[file(family = "test", messages(RegisteredProto, ParentProto))] struct File;
/// ```
///
/// An unused declaration still requires inventory membership.
/// ```compile_fail
/// use acyclic_contract_derive::{file, message};
/// use core::convert::Infallible;
/// #[message(error = Infallible, file = File)] struct Registered {}
/// #[message(error = Infallible, file = File)] struct Absent {}
/// #[file(family = "test", messages(RegisteredProto))] struct File;
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
///
/// Enum-valued cases are outside the supported oneof grammar.
/// ```compile_fail
/// use acyclic_contract_derive::{enumeration, file, oneof};
/// use core::convert::Infallible;
/// #[derive(Default)] enum Error { #[default] Missing, Unknown(i32) }
/// #[enumeration(error = Error, unknown = Error::Unknown)] enum State { Ready = 0 }
/// #[oneof(error = Error, file = File)]
/// enum Choice { #[wire(enumeration, tag = 1)] State(State) }
/// #[file(family = "test", enums(State))] struct File;
/// ```
///
/// Nested cases cannot mix file owners, even when package strings are identical.
/// ```compile_fail
/// use acyclic_contract_derive::{file, message, oneof};
/// use core::convert::Infallible;
/// #[message(error = Infallible, file = AFile)] struct A { #[wire(tag = 1)] number: u64 }
/// #[message(error = Infallible, file = BFile)] struct B { #[wire(tag = 1)] number: u64 }
/// #[oneof(error = Infallible, file = AFile)]
/// enum Choice { #[wire(message, tag = 1)] A(A), #[wire(message, tag = 2)] B(B) }
/// #[file(family = "test", messages(AProto))] struct AFile;
/// #[file(family = "test", messages(BProto))] struct BFile;
/// ```
#[proc_macro_attribute]
pub fn oneof(args: TokenStream, item: TokenStream) -> TokenStream {
    let mut options = options::Options::default();
    let parser = syn::meta::parser(|meta| {
        if !meta.path.is_ident("error") && !meta.path.is_ident("file") {
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

/// Register the sole authoritative file inventory and its sealed membership.
///
/// Registering a service under another file is rejected.
/// ```compile_fail
/// use acyclic_contract_derive::{file, message, service};
/// use core::convert::Infallible;
/// #[message(error = Infallible, file = A)] struct Body {}
/// #[service(file = A)] enum Service { Call { request: BodyProto, response: BodyProto } }
/// #[file(family = "test", messages(BodyProto), services(Service))] struct A;
/// #[file(family = "test", services(Service))] struct B;
/// ```
///
/// An enum has one file owner.
/// ```compile_fail
/// use acyclic_contract_derive::{enumeration, file};
/// enum Error { Unknown(i32) }
/// #[enumeration(error = Error, unknown = Error::Unknown)] enum State { Unspecified = 0 }
/// #[file(family = "test", enums(State))] struct A;
/// #[file(family = "test", enums(State))] struct B;
/// ```
///
/// Duplicate inventory entries cannot create multiple membership implementations.
/// ```compile_fail
/// use acyclic_contract_derive::{file, message};
/// #[message(error = core::convert::Infallible, file = File)] struct Body {}
/// #[file(family = "test", messages(BodyProto, BodyProto))] struct File;
/// ```
///
/// Consumers cannot forge the private membership trait.
/// ```compile_fail
/// use acyclic_contract_derive::{file, message};
/// mod owned {
///     use super::*;
///     #[message(error = core::convert::Infallible, file = File)] pub struct Body {}
///     #[file(family = "test", messages(BodyProto))] pub struct File;
/// }
/// struct Shadow;
/// impl owned::__file_members::Member for Shadow {}
/// ```
#[proc_macro_attribute]
pub fn file(args: TokenStream, item: TokenStream) -> TokenStream {
    let item = parse_macro_input!(item as syn::ItemStruct);
    registration::file(args.into(), &item)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Derive registered service schema from exact request and response types.
///
/// Each method has exactly one request and one response.
/// ```compile_fail
/// use acyclic_contract_derive::{file, message, service};
/// #[message(error = core::convert::Infallible, file = File)] struct Body {}
/// #[service(file = File)]
/// enum Invalid { Call { request: BodyProto, request: BodyProto, response: BodyProto } }
/// #[file(family = "test", messages(BodyProto), services(Invalid))] struct File;
/// ```
///
/// Streaming request and response types belong to the exact service file.
/// ```compile_fail
/// use acyclic_contract_derive::{file, message, service};
/// use core::convert::Infallible;
/// #[message(error = Infallible, file = AFile)] struct A {}
/// #[message(error = Infallible, file = BFile)] struct B {}
/// #[service(file = AFile)]
/// enum Service { #[wire(client_streaming, server_streaming)] Call { request: AProto, response: BProto } }
/// #[file(family = "test", messages(AProto), services(Service))] struct AFile;
/// #[file(family = "test", messages(BProto))] struct BFile;
/// ```
#[proc_macro_attribute]
pub fn service(args: TokenStream, item: TokenStream) -> TokenStream {
    let mut options = options::Options::default();
    let parser = syn::meta::parser(|meta| {
        if !meta.path.is_ident("file") {
            return Err(meta.error("unknown service option"));
        }
        options.message(&meta)
    });
    parse_macro_input!(args with parser);
    let item = parse_macro_input!(item as syn::ItemEnum);
    let Some(file) = options.file else {
        return syn::Error::new_spanned(&item.ident, "service file owner is required")
            .into_compile_error()
            .into();
    };
    registration::service(item, &file)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
