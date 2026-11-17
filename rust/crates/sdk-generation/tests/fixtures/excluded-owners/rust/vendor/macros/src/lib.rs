//! Public procedural macro documentation.
extern crate proc_macro;
use proc_macro::TokenStream;
/// Return the supplied tokens unchanged.
#[proc_macro]
pub fn demo_macro(input: TokenStream) -> TokenStream {
    input
}
/// Preserve an attributed item.
#[proc_macro_attribute]
pub fn demo_attribute(_args: TokenStream, input: TokenStream) -> TokenStream {
    input
}
/// Add no derived implementation.
#[proc_macro_derive(DemoDerive)]
pub fn demo_derive(_input: TokenStream) -> TokenStream {
    TokenStream::new()
}
