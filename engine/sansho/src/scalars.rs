//! The scalar source family: the Sansho data types implemented on the
//! concrete scalar Rust types — the "turtles all the way down" layer.
//! `i64`, `u64`, `f64`, `bool`, `&str`, `&String` answer the trait's
//! "what data type is here, what is the value by reference?" directly.
//!
//! Scalars have no members: navigation answers nothing, and nothing is
//! copied. The text of a borrowed string (`&'a str`, `&'a String`, a
//! borrowed `Cow<'a, str>`) is lent for the document's lifetime `'a` —
//! no allocation and no copy. The OWNED `String` form is the value form
//! (a computed string the evaluation itself built, or a `serde_json`
//! string held by value): it has no borrowable storage, so its text is
//! its own, and `as_str` hands out the value form's own rendering.

use crate::error::SanshoError;
use crate::view::{Kind, SanshoNumber, SanshoTrait};

/// The number scalars: a register-sized value, no members, no text.
macro_rules! number_scalar_impl {
    ($t:ty, $con:path) => {
        impl<'a> SanshoTrait<'a> for $t {
            fn kind(&self) -> Kind {
                Kind::Number
            }

            fn as_sansho_number(&self) -> Result<SanshoNumber, SanshoError> {
                Ok($con(*self))
            }

            fn to_string_sansho(&self) -> String {
                self.to_string()
            }
        }
    };
}

number_scalar_impl!(i64, SanshoNumber::I64);
number_scalar_impl!(u64, SanshoNumber::U64);
number_scalar_impl!(f64, SanshoNumber::F64);

impl<'a> SanshoTrait<'a> for bool {
    fn kind(&self) -> Kind {
        Kind::Bool
    }

    fn as_bool(&self) -> Result<bool, SanshoError> {
        Ok(*self)
    }

    fn is_null(&self) -> bool {
        false
    }

    fn to_string_sansho(&self) -> String {
        self.to_string()
    }
}

/// The borrowed string slice: the text IS the document's bytes, lent for
/// `'a` — no copy at any point.
impl<'a> SanshoTrait<'a> for &'a str {
    fn kind(&self) -> Kind {
        Kind::String
    }

    fn as_str(&self) -> Result<&'a str, SanshoError> {
        let ret: &'a str = self;
        Ok(ret)
    }

    fn to_string_sansho(&self) -> String {
        (*self).to_string()
    }
}

/// The borrowed owned string: the text lives in the document (a
/// `serde_json::Value`'s key or string, an `Item`'s field), lent for `'a`
/// — no copy at any point.
impl<'a> SanshoTrait<'a> for &'a String {
    fn kind(&self) -> Kind {
        Kind::String
    }

    fn as_str(&self) -> Result<&'a str, SanshoError> {
        let ret: &'a str = self;
        Ok(ret)
    }

    fn to_string_sansho(&self) -> String {
        (*self).to_string()
    }
}

impl<'a> SanshoTrait<'a> for String {
    fn kind(&self) -> Kind {
        Kind::String
    }

    fn as_str(&self) -> Result<&str, SanshoError> {
        Ok(self as &str)
    }

    fn to_string_sansho(&self) -> String {
        self.clone()
    }
}
