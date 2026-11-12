//! `#[derive(SanshoObject)]` — the field-walking `SanshoTrait`
//! implementation for an item-shaped struct.
//!
//! The derive implements `SanshoTrait` for `&'a MyStruct`, so the struct
//! is itself a Sansho source: `sansho::evaluate(&program, &my_struct)`
//! walks its fields. Nothing but the implementation is generated — no
//! wrapper type, no constructor to call.
//!
//! Each named field becomes a member carried by reference
//! (`Rc::new(&value.field)`), so the field's type needs a `SanshoTrait`
//! implementation for its *reference*: the engine provides those for
//! `String`, `&str`, `&[u8]`, `BTreeSet<String>`, `HashSet<String>`,
//! `BTreeMap<String, _>`, `HashMap<String, _>`, `serde_json::Value`,
//! `serde_cbor::Value`, and `Option<T>` of any of them. A `Copy` scalar
//! (`bool`, `u64`, `i64`, `f64`) or a reference field is copied into its
//! carrier instead, because the engine implements the trait for those
//! types in their owned form and copying a scalar is cheap.
//!
//! Attributes:
//! - `#[serde(rename = "name")]` is honored: the member takes that name.
//! - `#[serde(skip)]` is honored: the field is not a member.
//! - `#[serde(rename_all = "...")]` is a compile error: the derive does
//!   not implement serde's case conversions, so it cannot honor it.
//! - Any other serde attribute is ignored.
//!
//! ```
//! use sansho_derive::SanshoObject;
//!
//! #[derive(SanshoObject)]
//! struct Item {
//!     identifier: String,
//!     #[serde(rename = "bodyMimeType")]
//!     body_mime_type: String,
//!     #[serde(skip)]
//!     scratch: Vec<u8>,
//! }
//!
//! let item = Item {
//!     identifier: "gitoid:blob:sha256:abc".to_string(),
//!     body_mime_type: "application/java-archive".to_string(),
//!     scratch: vec![],
//! };
//!
//! let program = sansho::compile(&sansho::parse("bodyMimeType")?, None)?;
//! let value = sansho::evaluate(&program, &item)?;
//! assert_eq!(
//!     sansho::materialize(&value)?,
//!     serde_json::json!("application/java-archive")
//! );
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Data, DeriveInput, Fields, LitStr, Meta, Type};

/// One field's contribution to the generated implementation.
struct Member {
    /// The member name as JMESPath sees it: the serde rename when there
    /// is one, otherwise the field's own name.
    name: String,
    /// The field's Rust identifier, for the accessor expressions.
    ident: syn::Ident,
    /// Whether the carrier copies the field rather than borrowing it.
    copied: bool,
}

/// Whether the field is carried by copy rather than by reference.
///
/// The engine implements the trait for `bool`, `u64`, `i64` and `f64` in
/// their owned form, and a reference field (`&str`, `&[u8]`) is itself
/// the value — copying either is cheap. Every other field is carried as a
/// reference to it, whatever its type, as long as that reference
/// implements the trait.
/// A field's member name: the identifier WITHOUT the `r#` prefix.
///
/// `Ident::to_string()` keeps the prefix a raw identifier carries, but the
/// prefix is Rust syntax, not part of the document — a field written `r#type`
/// is the member `type`. `serde` strips it too (its `unraw`), so without this
/// the derived source and serde would describe the same struct with different
/// member names: a query for `type` would return null and `keys(@)` would list
/// a key nobody writes. `proc-macro2` 1.0.107 ships no `IdentExt`, so the
/// prefix is stripped here.
fn member_name(ident: &proc_macro2::Ident) -> String {
    let spelled = ident.to_string();
    match spelled.strip_prefix("r#") {
        Some(name) => name.to_string(),
        None => spelled,
    }
}

fn is_copied(ty: &Type) -> bool {
    match ty {
        Type::Reference(_) => true,
        Type::Path(path) => match path.path.segments.last() {
            Some(segment) => matches!(
                segment.ident.to_string().as_str(),
                "bool" | "u64" | "i64" | "f64"
            ),
            None => false,
        },
        _ => false,
    }
}

#[proc_macro_derive(SanshoObject, attributes(serde))]
pub fn sansho_object(input: TokenStream) -> TokenStream {
    let ast: DeriveInput = syn::parse(input).expect("the derive parses the input type");
    let name = &ast.ident;

    let Data::Struct(data) = &ast.data else {
        return syn::Error::new_spanned(name, "SanshoObject derives structs only")
            .to_compile_error()
            .into();
    };
    let Fields::Named(fields) = &data.fields else {
        return syn::Error::new_spanned(name, "SanshoObject derives named-field structs only")
            .to_compile_error()
            .into();
    };
    if !ast.generics.params.is_empty() {
        return syn::Error::new_spanned(
            &ast.generics,
            "SanshoObject cannot derive a generic struct — hand-write the SanshoTrait implementation",
        )
        .to_compile_error()
        .into();
    }

    let mut errors: Vec<syn::Error> = Vec::new();

    // The struct-level guard: the derive does not implement serde's case
    // conversions, so it cannot honor a rename_all.
    for attr in &ast.attrs {
        if attr.path().is_ident("serde")
            && let Meta::List(list) = attr.meta.clone()
            && list.tokens.to_string().contains("rename_all")
        {
            errors.push(syn::Error::new_spanned(
                attr,
                "SanshoObject cannot honor #[serde(rename_all)] — name the fields \
                 explicitly or hand-write the SanshoTrait implementation",
            ));
        }
    }

    let mut members: Vec<Member> = Vec::new();
    for field in &fields.named {
        let ident = field.ident.clone().expect("named fields");
        let mut name = member_name(&ident);
        let mut skipped = false;

        for attr in &field.attrs {
            if !attr.path().is_ident("serde") {
                continue;
            }
            // A serde knob the derive does not use is ignored rather than
            // rejected: the struct keeps whatever serde configuration it
            // already had.
            let _ = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("rename") {
                    if let Ok(value) = meta.value()
                        && let Ok(literal) = value.parse::<LitStr>()
                    {
                        name = literal.value();
                    }
                    return Ok(());
                }
                if meta.path.is_ident("skip") {
                    skipped = true;
                    return Ok(());
                }
                if meta.input.peek(syn::Token![=]) {
                    let _ = meta.value().and_then(|value| value.parse::<syn::Expr>());
                }
                Ok(())
            });
        }

        if skipped {
            continue;
        }
        members.push(Member {
            name,
            ident,
            copied: is_copied(&field.ty),
        });
    }

    if !errors.is_empty() {
        let errors = errors.into_iter().map(|error| error.to_compile_error());
        return quote!(#(#errors)*).into();
    }

    let names: Vec<&str> = members.iter().map(|member| member.name.as_str()).collect();
    let carriers: Vec<TokenStream2> = members
        .iter()
        .map(|member| {
            let ident = &member.ident;
            if member.copied {
                quote!((::std::rc::Rc::new(value.#ident) as ::sansho::view::SanshoCarrier<'a>))
            } else {
                quote!((::std::rc::Rc::new(&value.#ident) as ::sansho::view::SanshoCarrier<'a>))
            }
        })
        .collect();
    let count = members.len();

    let expanded = quote! {
        impl<'a> ::sansho::view::SanshoTrait<'a> for &'a #name {
            fn kind(&self) -> ::sansho::view::Kind {
                ::sansho::view::Kind::Object
            }

            fn get_key(
                &self,
                name: &str,
            ) -> ::core::result::Result<
                ::core::option::Option<::sansho::view::SanshoCarrier<'a>>,
                ::sansho::SanshoError,
            > {
                let value: &'a #name = *self;
                match name {
                    #(
                        #names => ::core::result::Result::Ok(
                            ::core::option::Option::Some(#carriers)
                        ),
                    )*
                    _ => ::core::result::Result::Ok(::core::option::Option::None),
                }
            }

            fn as_object(
                &self,
            ) -> ::core::result::Result<
                ::std::collections::HashMap<&str, ::sansho::view::SanshoCarrier<'a>>,
                ::sansho::SanshoError,
            > {
                let value: &'a #name = *self;
                let mut members: ::std::collections::HashMap<
                    &str,
                    ::sansho::view::SanshoCarrier<'a>,
                > = ::std::collections::HashMap::with_capacity(#count);
                #(members.insert(#names, #carriers);)*
                ::core::result::Result::Ok(members)
            }

            fn container_len(&self) -> ::core::result::Result<usize, ::sansho::SanshoError> {
                ::core::result::Result::Ok(#count)
            }

            fn to_string_sansho(&self) -> ::std::string::String {
                let value: &'a #name = *self;
                let mut out = ::std::string::String::from("{");
                #(
                    if out.len() > 1 {
                        out.push_str(", ");
                    }
                    out.push_str(#names);
                    out.push_str(": ");
                    out.push_str(&::sansho::view::SanshoTrait::to_string_sansho(&#carriers));
                )*
                out.push('}');
                out
            }
        }
    };
    expanded.into()
}
