//! `#[derive(SanshoNode)]` — the field-walking `Node` implementation
//! for item-shaped structs (the research spike, Phase 3).
//!
//! Generates an enum node wrapping the struct's walkable fields and
//! implements `sansho::view::Node` over it:
//! - `String` / `&str` fields → the `Str` variant;
//! - `Option<String>` fields → the `OptStr` variant;
//! - `serde_json::Value` fields → the `Value` variant (navigation
//!   over the value tree implemented directly in the generated code);
//! - any other field type → the `Skip` variant (not walked; evaluates
//!   as null).
//!
//! `#[serde(rename)]` / `#[serde(rename_all)]` are compile errors —
//! the derive cannot honor them, and silently wrong evaluation
//! results are the alternative.
//!
//! The generated enum is constructed from the wrapped struct via the
//! generated `wrap` constructor: `ItemNode::wrap(&item)`.
//!
//! The serde-rename guard (a compile error — the derive cannot honor
//! renamed fields, and silently wrong evaluation results are the
//! alternative):
//!
//! ```compile_fail
//! use sansho_derive::SanshoNode;
//! #[derive(SanshoNode)]
//! pub struct Renamed {
//!     #[serde(rename = "other")]
//!     pub identifier: String,
//! }
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Meta};

#[proc_macro_derive(SanshoNode)]
pub fn sansho_node(input: TokenStream) -> TokenStream {
    let ast: DeriveInput = syn::parse(input).expect("the derive parses the input type");
    let name = &ast.ident;
    let node_name = syn::Ident::new(&format!("{name}Node"), name.span());

    let Data::Struct(data) = &ast.data else {
        return syn::Error::new_spanned(name, "SanshoNode derives structs only")
            .to_compile_error()
            .into();
    };
    let Fields::Named(fields) = &data.fields else {
        return syn::Error::new_spanned(name, "SanshoNode derives named-field structs only")
            .to_compile_error()
            .into();
    };

    let mut errors = Vec::new();
    let mut wrap_arms = Vec::new();
    let mut entries_items: Vec<String> = Vec::new();

    // the struct-level serde-rename guard
    for attr in &ast.attrs {
        if attr.path().is_ident("serde") {
            if let Meta::List(list) = attr.meta.clone() {
                if list.tokens.to_string().contains("rename_all") {
                    errors.push(syn::Error::new_spanned(
                        attr,
                        "SanshoNode cannot honor #[serde(rename_all)] — remove it or hand-write the Node impl",
                    ));
                }
            }
        }
    }

    for field in &fields.named {
        let field_name = field.ident.as_ref().expect("named fields");
        let field_str = field_name.to_string();

        for attr in &field.attrs {
            if attr.path().is_ident("serde") {
                if let Meta::List(list) = attr.meta.clone() {
                    if list.tokens.to_string().contains("rename") {
                        errors.push(syn::Error::new_spanned(
                            attr,
                            "SanshoNode cannot honor #[serde(rename(...))] — remove it or hand-write the Node impl",
                        ));
                    }
                }
            }
        }

        entries_items.push(field_str.clone());

        // classify the field's shape
        let variant = field_shape(&field.ty);
        match variant {
            Shape::String => {
                // the field is String or &str: `as_str()` works for
                // both (String::as_str and str::as_str)
                wrap_arms.push(quote! {
                    #field_str => Some(Self::Str(item.#field_name.as_str())),
                });
            }
            Shape::OptString => {
                wrap_arms.push(quote! {
                    #field_str => Some(Self::OptStr(item.#field_name.as_deref())),
                });
            }
            Shape::Value => {
                wrap_arms.push(quote! {
                    #field_str => Some(Self::Value(&item.#field_name)),
                });
            }
            Shape::Skip => {
                wrap_arms.push(quote! {
                    #field_str => Some(Self::Skip),
                });
            }
        }
    }

    if !errors.is_empty() {
        let errors = errors.into_iter().map(|e| e.to_compile_error());
        return quote!(#(#errors)*).into();
    }

    let expanded = quote! {
        /// The value-node wrapper generated for `#name`.
        #[derive(Clone)]
        #[allow(dead_code)]
        pub enum #node_name<'a> {
            Struct(&'a #name),
            Str(&'a str),
            OptStr(Option<&'a str>),
            Value(&'a serde_json::Value),
            Skip,
        }

        impl<'a> #node_name<'a> {
            /// Wrap a reference to the struct into its node.
            pub fn wrap(value: &'a #name) -> Self {
                Self::Struct(value)
            }



            /// The field node for a named field of the struct.
            fn node_for(item: &'a #name, name: &str) -> Option<Self> {
                match name {
                    #(#wrap_arms)*
                    _ => None,
                }
            }
        }

        impl<'a> ::sansho::view::Node<'a> for #node_name<'a> {
            fn kind(&self) -> ::sansho::view::Kind {
                match self {
                    Self::Struct(_) => ::sansho::view::Kind::Object,
                    Self::Str(_) | Self::OptStr(_) => ::sansho::view::Kind::String,
                    Self::Value(v) => match v {
                        ::serde_json::Value::Object(_) => ::sansho::view::Kind::Object,
                        ::serde_json::Value::Array(_) => ::sansho::view::Kind::Array,
                        ::serde_json::Value::String(_) => ::sansho::view::Kind::String,
                        ::serde_json::Value::Number(_) => ::sansho::view::Kind::Number,
                        ::serde_json::Value::Bool(_) => ::sansho::view::Kind::Bool,
                        ::serde_json::Value::Null => ::sansho::view::Kind::Null,
                    },
                    Self::Skip => ::sansho::view::Kind::Null,
                }
            }
            fn as_str(&self) -> Option<std::borrow::Cow<'a, str>> {
                match self {
                    Self::Str(s) => Some(std::borrow::Cow::Borrowed(s)),
                    Self::OptStr(Some(s)) => Some(std::borrow::Cow::Borrowed(s)),
                    Self::Value(::serde_json::Value::String(s)) => {
                        Some(std::borrow::Cow::Borrowed(s.as_str()))
                    }
                    _ => None,
                }
            }
            fn as_f64(&self) -> Option<f64> {
                match self {
                    Self::Value(::serde_json::Value::Number(n)) => n.as_f64(),
                    _ => None,
                }
            }
            fn as_bool(&self) -> Option<bool> {
                match self {
                    Self::Value(::serde_json::Value::Bool(b)) => Some(*b),
                    _ => None,
                }
            }
            fn is_null(&self) -> bool {
                matches!(self, Self::Skip | Self::OptStr(None))
                    || matches!(self, Self::Value(::serde_json::Value::Null))
            }
            fn get_key(&self, name: &str) -> Option<Self> {
                match self {
                    Self::Struct(item) => Self::node_for(item, name),
                    Self::Value(::serde_json::Value::Object(map)) => {
                        map.get(name).map(|v| Self::Value(v))
                    }
                    // leaf nodes (Str/OptStr/Skip) answer NO
                    // navigation — JMESPath says null for navigation
                    // on a string
                    Self::Str(_) | Self::OptStr(_) | Self::Skip => None,
                    _ => None,
                }
            }
            fn get_index(&self, index: usize) -> Option<Self> {
                match self {
                    Self::Value(::serde_json::Value::Array(items)) => {
                        items.get(index).map(|v| Self::Value(v))
                    }
                    _ => None,
                }
            }
            fn entries(&self) -> Vec<(String, Self)> {
                match self {
                    Self::Struct(item) => {
                        // the walkable fields, in declaration order,
                        // keyed by field name
                        let mut pairs: Vec<(String, Self)> = Vec::new();
                        for field in [#(#entries_items),*] {
                            pairs.push((
                                field.to_string(),
                                Self::node_for(item, field).unwrap_or(Self::Skip),
                            ));
                        }
                        pairs.sort_by(|a, b| a.0.cmp(&b.0));
                        pairs
                    }
                    Self::Value(::serde_json::Value::Object(map)) => {
                        let mut pairs: Vec<(String, Self)> = map
                            .iter()
                            .map(|(k, v)| (k.clone(), Self::Value(v)))
                            .collect();
                        pairs.sort_by(|a, b| a.0.cmp(&b.0));
                        pairs
                    }
                    _ => Vec::new(),
                }
            }
            fn elements(&self) -> Vec<Self> {
                match self {
                    Self::Value(::serde_json::Value::Array(items)) => {
                        items.iter().map(|v| Self::Value(v)).collect()
                    }
                    _ => Vec::new(),
                }
            }
            fn container_len(&self) -> Option<usize> {
                match self {
                    Self::Value(::serde_json::Value::Array(items)) => Some(items.len()),
                    Self::Value(::serde_json::Value::Object(map)) => Some(map.len()),
                    _ => None,
                }
            }
            fn counts_toward_aggregation() -> bool {
                false
            }
            fn materialize(&self) -> Result<::serde_json::Value, ::sansho::SanshoError> {
                match self {
                    Self::Struct(item) => {
                        // the struct's JSON: the walkable fields
                        let mut object = ::serde_json::Map::new();
                        for field in [#(#entries_items),*] {
                            if let Some(node) = Self::node_for(item, field) {
                                let value = node.materialize()?;
                                object.insert(field.to_string(), value);
                            }
                        }
                        Ok(::serde_json::Value::Object(object))
                    }
                    Self::Str(s) => Ok(::serde_json::Value::String(s.to_string())),
                    Self::OptStr(Some(s)) => Ok(::serde_json::Value::String(s.to_string())),
                    Self::OptStr(None) => Ok(::serde_json::Value::Null),
                    Self::Value(v) => Ok((*v).clone()),
                    Self::Skip => Ok(::serde_json::Value::Null),
                }
            }
        }
    };
    expanded.into()
}

#[cfg(test)]
mod dump {
    #[test]
    fn dump_generated() {
        let input = r#"
        struct T { identifier: String, body: serde_json::Value }
        "#;
        let _ = input;
    }
}

/// The recognized field shapes.
enum Shape {
    String,
    OptString,
    Value,
    Skip,
}

fn field_shape(ty: &syn::Type) -> Shape {
    // &str / &'a str reference fields classify as String
    if let syn::Type::Reference(r) = ty {
        if let syn::Type::Path(inner) = r.elem.as_ref() {
            if inner.path.is_ident("str") {
                return Shape::String;
            }
        }
    }
    let syn::Type::Path(tp) = ty else {
        return Shape::Skip;
    };
    let last = tp.path.segments.last().map(|s| s.ident.to_string());
    match last.as_deref() {
        Some("String") | Some("str") => Shape::String,
        Some("Value") => Shape::Value,
        Some("Option") => {
            // Option<String> only
            let args = &tp.path.segments[0].arguments;
            if let syn::PathArguments::AngleBracketed(ab) = args {
                let inner = ab.args.first().map(|a| quote::ToTokens::to_token_stream(a).to_string());
                if inner.as_deref() == Some("String") {
                    return Shape::OptString;
                }
            }
            Shape::Skip
        }
        _ => Shape::Skip,
    }
}