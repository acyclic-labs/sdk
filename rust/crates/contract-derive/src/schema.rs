//! Build a generated schema string from the fewest appends.
//!
//! A schema is mostly text fixed at expansion time: field names, tags, wire
//! kinds and the declaration header. Only a nested message or enum contributes
//! a name known at run time. Emitting one `format!` per field makes every
//! dependent compile the formatting machinery for each field; folding the
//! fixed text into literals leaves one `push_str` per run-time name, and a
//! schema with no run-time part is a single literal.
use proc_macro2::TokenStream;
use quote::quote;

/// One piece of a schema: text fixed at expansion time, or tokens that produce
/// it at run time.
pub enum Segment {
    Static(String),
    Dynamic(TokenStream),
}

impl Segment {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Static(text.into())
    }

    pub fn tokens(tokens: TokenStream) -> Self {
        Self::Dynamic(tokens)
    }
}

/// Emit a block that builds `segments` into a `String`.
pub fn build(segments: Vec<Segment>) -> TokenStream {
    let mut merged: Vec<Segment> = Vec::new();
    for segment in segments {
        match (merged.last_mut(), segment) {
            (Some(Segment::Static(text)), Segment::Static(more)) => text.push_str(&more),
            (_, segment) => merged.push(segment),
        }
    }
    let mut head = String::new();
    let mut statements = Vec::new();
    for segment in merged {
        match segment {
            Segment::Static(text) => {
                if statements.is_empty() && head.is_empty() {
                    head = text;
                } else {
                    statements.push(quote!(schema.push_str(#text);));
                }
            }
            Segment::Dynamic(tokens) => statements.push(quote!(schema.push_str(#tokens);)),
        }
    }
    if statements.is_empty() {
        // Nothing is known only at run time: the schema is one literal, so the
        // dependent compiles no binding and no append at all.
        return quote!(::std::string::String::from(#head));
    }
    quote! {
        let mut schema = ::std::string::String::from(#head);
        #(#statements)*
        schema
    }
}
