//! The scalar source family: the Sansho data types implemented on the
//! concrete scalar Rust types — the "turtles all the way down" layer.
//! `i64`, `u64`, `f64`, `bool`, `str`, `String` answer the trait's
//! "what data type is here, what is the value by reference?" directly.
//!
//! Scalars have no members: navigation returns nothing.

use crate::error::SanshoError;
use crate::view::{Kind, SanshoNumber, SanshoTrait};
use serde_json::Value as J;

macro_rules! int_scalar_impl {
    ($t:ty, $con:path) => {
        impl<'a> SanshoTrait<'a> for $t {
            fn kind(&self) -> Kind {
                Kind::Number
            }

            fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
                None
            }

            fn as_sansho_number(&self) -> Option<SanshoNumber> {
                Some($con(*self))
            }

            fn as_bool(&self) -> Option<bool> {
                None
            }

            fn is_null(&self) -> bool {
                false
            }

            fn get_key(&self, _name: &str) -> Option<impl SanshoTrait<'a> + use<'a>> {
                None::<J>
            }

            fn get_index(&self, _index: usize) -> Option<impl SanshoTrait<'a> + use<'a>> {
                None::<J>
            }

            fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a>)> {
                Vec::<(String, J)>::new()
            }

            fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a>> {
                Vec::<J>::new()
            }

            fn container_len(&self) -> Option<usize> {
                None
            }

            fn materialize(&self) -> Result<J, SanshoError> {
                Ok(J::Number(serde_json::Number::from(*self)))
            }

            fn counts_toward_aggregation() -> bool {
                false
            }
        }
    };
}

int_scalar_impl!(i64, SanshoNumber::I64);
int_scalar_impl!(u64, SanshoNumber::U64);
impl<'a> SanshoTrait<'a> for f64 {
    fn kind(&self) -> Kind {
        Kind::Number
    }

    fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        None
    }

    fn as_sansho_number(&self) -> Option<SanshoNumber> {
        Some(SanshoNumber::F64(*self))
    }

    fn as_bool(&self) -> Option<bool> {
        None
    }

    fn is_null(&self) -> bool {
        false
    }

    fn get_key(&self, _name: &str) -> Option<impl SanshoTrait<'a> + use<'a>> {
        None::<J>
    }

    fn get_index(&self, _index: usize) -> Option<impl SanshoTrait<'a> + use<'a>> {
        None::<J>
    }

    fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a>)> {
        Vec::<(String, J)>::new()
    }

    fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a>> {
        Vec::<J>::new()
    }

    fn container_len(&self) -> Option<usize> {
        None
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        Ok(serde_json::Number::from_f64(*self)
            .map(J::Number)
            .unwrap_or(J::Null))
    }

    fn counts_toward_aggregation() -> bool {
        false
    }
}

impl<'a> SanshoTrait<'a> for bool {
    fn kind(&self) -> Kind {
        Kind::Bool
    }

    fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        None
    }

    fn as_sansho_number(&self) -> Option<SanshoNumber> {
        None
    }

    fn as_bool(&self) -> Option<bool> {
        Some(*self)
    }

    fn is_null(&self) -> bool {
        false
    }

    fn get_key(&self, _name: &str) -> Option<impl SanshoTrait<'a> + use<'a>> {
        None::<J>
    }

    fn get_index(&self, _index: usize) -> Option<impl SanshoTrait<'a> + use<'a>> {
        None::<J>
    }

    fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a>)> {
        Vec::<(String, J)>::new()
    }

    fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a>> {
        Vec::<J>::new()
    }

    fn container_len(&self) -> Option<usize> {
        None
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        Ok(J::Bool(*self))
    }

    fn counts_toward_aggregation() -> bool {
        false
    }
}

impl<'a> SanshoTrait<'a> for str {
    fn kind(&self) -> Kind {
        Kind::String
    }

    fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        Some(std::borrow::Cow::Borrowed(self))
    }

    fn as_sansho_number(&self) -> Option<SanshoNumber> {
        None
    }

    fn as_bool(&self) -> Option<bool> {
        None
    }

    fn is_null(&self) -> bool {
        false
    }

    fn get_key(&self, _name: &str) -> Option<impl SanshoTrait<'a> + use<'a>> {
        None::<J>
    }

    fn get_index(&self, _index: usize) -> Option<impl SanshoTrait<'a> + use<'a>> {
        None::<J>
    }

    fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a>)> {
        Vec::<(String, J)>::new()
    }

    fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a>> {
        Vec::<J>::new()
    }

    fn container_len(&self) -> Option<usize> {
        None
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        Ok(J::String(self.to_string()))
    }

    fn raw_text_bytes(&self) -> Option<&[u8]> {
        Some(self.as_bytes())
    }

    fn counts_toward_aggregation() -> bool {
        false
    }
}

impl<'a> SanshoTrait<'a> for String {
    fn kind(&self) -> Kind {
        Kind::String
    }

    fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        Some(std::borrow::Cow::Borrowed(self.as_str()))
    }

    fn as_sansho_number(&self) -> Option<SanshoNumber> {
        None
    }

    fn as_bool(&self) -> Option<bool> {
        None
    }

    fn is_null(&self) -> bool {
        false
    }

    fn get_key(&self, _name: &str) -> Option<impl SanshoTrait<'a> + use<'a>> {
        None::<J>
    }

    fn get_index(&self, _index: usize) -> Option<impl SanshoTrait<'a> + use<'a>> {
        None::<J>
    }

    fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a>)> {
        Vec::<(String, J)>::new()
    }

    fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a>> {
        Vec::<J>::new()
    }

    fn container_len(&self) -> Option<usize> {
        None
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        Ok(J::String(self.clone()))
    }

    fn counts_toward_aggregation() -> bool {
        false
    }
}

/// The borrowed-string value: a string carried as a trait value
/// (`Cow<'a, str>`) — the borrowed form of string members (a target
/// string in a set, an identifier): zero-copy where the string is
/// borrowed.
impl<'a> SanshoTrait<'a> for std::borrow::Cow<'a, str> {
    fn kind(&self) -> Kind {
        Kind::String
    }

    fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        Some(self.clone())
    }

    fn as_str_ref(&self) -> Option<&'a str> {
        match self {
            std::borrow::Cow::Borrowed(text) => Some(text),
            std::borrow::Cow::Owned(_) => None,
        }
    }

    fn as_sansho_number(&self) -> Option<SanshoNumber> {
        None
    }

    fn as_bool(&self) -> Option<bool> {
        None
    }

    fn is_null(&self) -> bool {
        false
    }

    fn get_key(&self, _name: &str) -> Option<impl SanshoTrait<'a> + use<'a>> {
        None::<J>
    }

    fn get_index(&self, _index: usize) -> Option<impl SanshoTrait<'a> + use<'a>> {
        None::<J>
    }

    fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a>)> {
        Vec::<(String, J)>::new()
    }

    fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a>> {
        Vec::<J>::new()
    }

    fn container_len(&self) -> Option<usize> {
        None
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        Ok(J::String(self.to_string()))
    }

    fn raw_text_bytes(&self) -> Option<&[u8]> {
        Some(self.as_bytes())
    }

    fn counts_toward_aggregation() -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eval::lookup_value;
    use crate::{compile, parse};

    fn program(expression: &str) -> crate::Program {
        compile(&parse(expression).unwrap()).unwrap()
    }

    /// Requirement: the scalar family is walkable through the trait.
    /// What: `lookup` over scalar sources (i64, u64, f64, bool, str,
    /// String) returns the scalar for `@`. Why: the source trait must
    /// be implemented turtles-all-the-way-down — every concrete type
    /// the evaluator may meet answers the probes itself, and `@` on a
    /// scalar root is the value itself.
    #[test]
    fn scalar_sources_evaluate_to_themselves() {
        let p = program("@");
        assert_eq!(lookup_value(&42i64, &p).unwrap(), serde_json::json!(42));
        assert_eq!(
            lookup_value(&u64::MAX, &p).unwrap(),
            serde_json::json!(u64::MAX)
        );
        assert_eq!(lookup_value(&3.5f64, &p).unwrap(), serde_json::json!(3.5));
        assert_eq!(lookup_value(&true, &p).unwrap(), serde_json::json!(true));
        assert_eq!(
            lookup_value("hi there", &p).unwrap(),
            serde_json::json!("hi there")
        );
        assert_eq!(
            lookup_value(&String::from("owned"), &p).unwrap(),
            serde_json::json!("owned")
        );
    }

    /// Requirement: scalar probes report the exact full-width value.
    /// What: the trait's number probe on the scalar types returns
    /// I64/U64/F64 exactly (i64::MIN and u64::MAX included — no
    /// narrowing). Why: the trait surface is the full-width number
    /// contract.
    #[test]
    fn scalar_number_probes_are_full_width() {
        assert_eq!(
            SanshoTrait::as_sansho_number(&i64::MIN),
            Some(SanshoNumber::I64(i64::MIN))
        );
        assert_eq!(
            SanshoTrait::as_sansho_number(&u64::MAX),
            Some(SanshoNumber::U64(u64::MAX))
        );
        assert!(SanshoTrait::as_sansho_number(&f64::NAN).is_some());
    }
}