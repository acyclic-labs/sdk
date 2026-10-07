use crate::*;

fn process_custom_expression(expr: &PathOrClosure, base_ident: &TokenStream2) -> TokenStream2 {
	match expr {
		PathOrClosure::Path(path) => quote! { #path(#base_ident) },
		PathOrClosure::Closure(closure) => {
			quote_spanned! {closure.span()=>
			  ::protify::__apply(#base_ident, #closure)
			}
		}
	}
}

pub struct ProtoConversions<'a> {
	pub proxy_ident: &'a Ident,
	pub proto_ident: &'a Ident,
	pub kind: ItemKind,
	pub container_attrs: ContainerAttrs<'a>,
	pub fallible_error: Option<&'a syn::Path>,
	pub fields: &'a [FieldDataKind],
}

impl ProtoConversions<'_> {
	pub fn generate_proto_conversions(&self) -> TokenStream2 {
		if self.fallible_error.is_some() {
			let from_proto = self.create_try_from_proto_impl();
			let into_proto = self.create_into_proto_impl();

			// Fallible proxies deliberately do not implement MessageProxy/ProxiedMessage:
			// those traits require infallible From conversions. Outbound conversion remains
			// infallible because authored semantic values are already validated.
			return quote! {
			  #from_proto
			  #into_proto
			};
		}

		let from_proto = self.create_from_proto_impl();
		let into_proto = self.create_into_proto_impl();
		let kind = self.kind;
		let proxy_ident = self.proxy_ident;
		let proto_ident = self.proto_ident;

		let proxy_trait_impl = if kind.is_message() {
			quote! {
			  impl ::protify::MessageProxy for #proxy_ident {
					type Message = #proto_ident;
			  }

			  impl ::protify::ProxiedMessage for #proto_ident {
					type Proxy = #proxy_ident;
			  }
			}
		} else {
			quote! {
			  impl ::protify::OneofProxy for #proxy_ident {
					type Oneof = #proto_ident;
			  }

			  impl ::protify::ProxiedOneof for #proto_ident {
					type Proxy = #proxy_ident;
			  }
			}
		};

		quote! {
		  #from_proto
		  #into_proto
		  #proxy_trait_impl
		}
	}

	fn create_try_from_proto_impl(&self) -> TokenStream2 {
		let Self {
			proxy_ident,
			proto_ident,
			kind,
			container_attrs,
			fields,
			fallible_error,
		} = self;
		let error = fallible_error.expect("fallible conversion requires an error type");
		let custom_from_proto = container_attrs.custom_from_proto_expr();

		let tokens = fields.iter().filter_map(|d| match d {
			FieldDataKind::Ignored { ident, .. } => Some(quote_spanned! {ident.span()=>
				#ident: Default::default()
			}),
			FieldDataKind::Normal(field_data) => {
				let ident = &field_data.ident;
				let span = ident.span();
				let conversion = if let Some(expr) = field_data.from_proto.as_ref() {
					let conversion = process_custom_expression(expr, &quote_spanned! {span=> value.#ident });
					quote_spanned! {span=> (#conversion)? }
				} else {
					field_data
						.proto_field
						.fallible_from_proto(&quote_spanned! {span=> value.#ident})
				};
				Some(quote_spanned! {span=> #ident: #conversion })
			}
		});

		let body = if let Some(expr) = custom_from_proto {
			let conversion = process_custom_expression(expr, &quote! { value });
			quote! { (#conversion)? }
		} else if kind.is_oneof() {
			let arms = fields.iter().filter_map(|field| match field {
				FieldDataKind::Ignored { .. } => None,
				FieldDataKind::Normal(data) => {
					let ident = &data.ident;
					let span = ident.span();
					let conversion = if let Some(expr) = data.from_proto.as_ref() {
						let conversion = process_custom_expression(expr, &quote_spanned! {span=> value });
						quote_spanned! {span=> (#conversion)? }
					} else {
						data.proto_field.fallible_from_proto(&quote_spanned! {span=> value })
					};
					Some(quote_spanned! {span=> #proto_ident::#ident(value) => ::core::result::Result::Ok(#proxy_ident::#ident(#conversion)) })
				}
			});
			quote! { match value { #(#arms),* } }
		} else {
			quote! { Self { #(#tokens),* } }
		};

		let result = if kind.is_oneof() {
			quote! { #body }
		} else {
			quote! { ::core::result::Result::Ok(#body) }
		};

		quote! {
		  impl ::core::convert::TryFrom<#proto_ident> for #proxy_ident {
				type Error = #error;
				fn try_from(value: #proto_ident) -> ::core::result::Result<Self, Self::Error> {
					#result
				}
		  }
		}
	}
fn create_from_proto_impl(&self) -> TokenStream2 {
		let Self {
			proxy_ident,
			proto_ident,
			kind,
			container_attrs,
			fields,
			..
		} = self;

		let custom_from_proto = container_attrs.custom_from_proto_expr();

		let conversion_body = if let Some(from_proto) = custom_from_proto {
			process_custom_expression(from_proto, &quote_spanned! {from_proto.span()=> value })
		} else if fields.is_empty() {
			quote! { unimplemented!() }
		} else {
			let tokens = fields.iter()
        // For oneofs, ignored variants do not map to the original enum
        .filter(|d| !(d.is_ignored() && kind.is_oneof()))
        .map(|d| {
      let field_ident = d.ident();
        let span = field_ident.span();

      let conversion_logic = match d {
        FieldDataKind::Ignored { from_proto, .. } => {
          if let Some(expr) = from_proto {
            match expr {
              // Field is ignored, so we don't pass any args here
              PathOrClosure::Path(path) => quote_spanned! {span=> #path() },
              PathOrClosure::Closure(closure) => {
                let error = error!(closure, "Cannot use a closure for ignored fields");

                error.into_compile_error()
              }
            }
          } else {
            quote_spanned! {span=> Default::default() }
          }
        }
        FieldDataKind::Normal(field_data) => {
          let base_ident = match kind {
            ItemKind::Oneof => quote_spanned! {span=> v },
            ItemKind::Message => {
              quote_spanned! {span=> value.#field_ident }
            }
          };

          if let Some(expr) = field_data.from_proto.as_ref() {
            process_custom_expression(expr, &base_ident)
          } else {
            field_data
              .proto_field
              .default_from_proto(&base_ident)
          }
        }
      };

      match kind {
        ItemKind::Oneof => {
          quote_spanned! {span=> #proto_ident::#field_ident(v) => #proxy_ident::#field_ident(#conversion_logic) }
        }
        ItemKind::Message => quote_spanned! {span=> #field_ident: #conversion_logic },
      }
    });

			match kind {
				ItemKind::Oneof => quote! {
				  match value {
						#(#tokens),*
				  }
				},
				ItemKind::Message => {
					quote! {
					  Self {
							#(#tokens),*
					  }
					}
				}
			}
		};

		quote! {
		  #[allow(clippy::useless_conversion)]
		  impl From<#proto_ident> for #proxy_ident {
				fn from(value: #proto_ident) -> Self {
					#conversion_body
				}
		  }
		}
	}

	fn create_into_proto_impl(&self) -> TokenStream2 {
		let Self {
			proxy_ident,
			proto_ident,
			kind,
			container_attrs,
			fields,
			..
		} = self;

		let custom_into_proto = container_attrs.custom_into_proto_expr();

		let conversion_body = if let Some(into_proto) = custom_into_proto {
			process_custom_expression(into_proto, &quote_spanned! {into_proto.span()=> value })
		} else if fields.is_empty() {
			quote! { unimplemented!() }
		} else {
			let tokens = fields
				.iter()
				.filter(|d| !(d.is_ignored() && kind.is_message()))
				.map(|d| match d {
					// This is only for ignored oneof variants
					FieldDataKind::Ignored {
						ident, into_proto, ..
					} => {
						if let Some(expr) = into_proto {
							let conversion = process_custom_expression(
								expr,
								&quote_spanned! {ident.span()=> v },
							);

							quote_spanned! {ident.span()=> #proxy_ident::#ident(v) => #conversion }
						} else {
							quote_spanned! {ident.span()=> #proxy_ident::#ident(..) => #proto_ident::default() }
						}
					}
					FieldDataKind::Normal(field_data) => {
						let field_ident = &field_data.ident;
						let span = field_ident.span();

						let base_ident = match kind {
							ItemKind::Oneof => quote_spanned! {span=> v },
							ItemKind::Message => {
								quote_spanned! {span=> value.#field_ident }
							}
						};

						let conversion_logic = if let Some(expr) = field_data.into_proto.as_ref() {
							process_custom_expression(expr, &base_ident)
						} else {
							field_data
								.proto_field
								.default_into_proto(&base_ident)
						};

						match kind {
							ItemKind::Oneof => quote_spanned! {span=>
							  #proxy_ident::#field_ident(v) => #proto_ident::#field_ident(#conversion_logic)
							},
							ItemKind::Message => {
								quote_spanned! {span=> #field_ident: #conversion_logic }
							}
						}
					}
				});

			match kind {
				ItemKind::Oneof => quote! {
				  match value {
						#(#tokens),*
				  }
				},
				ItemKind::Message => {
					quote! {
					  Self {
							#(#tokens),*
					  }
					}
				}
			}
		};

		quote! {
		  #[allow(clippy::useless_conversion)]
		  impl From<#proxy_ident> for #proto_ident {
				fn from(value: #proxy_ident) -> Self {
					#conversion_body
				}
		  }
		}
	}
}

#[derive(Clone, Copy)]
pub enum ItemKind {
	Oneof,
	Message,
}

impl ItemKind {
	/// Returns 	rue` if the input item kind is [`Message`].
	///
	/// [`Message`]: InputItemKind::Message
	#[must_use]
	pub const fn is_message(self) -> bool {
		matches!(self, Self::Message)
	}

	/// Returns 	rue` if the item kind is [`Oneof`].
	///
	/// [`Oneof`]: ItemKind::Oneof
	#[must_use]
	pub const fn is_oneof(self) -> bool {
		matches!(self, Self::Oneof)
	}
}








