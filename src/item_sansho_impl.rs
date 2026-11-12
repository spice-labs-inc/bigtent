//! The Item-side implementations of [`sansho::SanshoTrait`] (`Item`)
//! and the item projection entries: project an item's
//! fields through the Sansho engine, directly over the item's stored
//! CBOR bytes.
//!
//! The trait implementations live here, WITH their types, per the
//! placement rule ("within Item for Item"; standard-library types'
//! implementations live in the Sansho engine itself).
//!
//! This module is the ONLY place `bigtent` touches Sansho. The HTTP attachment point is a separate
//! specification — nothing here changes any existing endpoint.
//!
//! The critical property: the item bytes reach the engine UNDECODED.
//! The host's normal read path materializes the whole item; the
//! no-decode entry takes the raw byte span (via
//! [`crate::rodeo::data::read_item_bytes_at`]) so selective
//! materialization stays intact end to end.

use std::{collections::HashMap, rc::Rc};

use sansho::SanshoCarrier;

use crate::item::Item;

// ---------------------------------------------------------------------------
// The item's Sansho surface
// ---------------------------------------------------------------------------

impl<'d> sansho::SanshoTrait<'d> for &'d Item {
    fn kind(&self) -> sansho::view::Kind {
        sansho::view::Kind::Object
    }

    fn get_key(
        &self,
        name: &str,
    ) -> Result<Option<sansho::view::SanshoCarrier<'d>>, sansho::SanshoError> {
        let item: &'d Item = self;
        match name {
            // the member is the item's own field, borrowed: a refcount
            // around the borrow, never a copy
            "identifier" => Ok(Some(
                Rc::new(&item.identifier) as sansho::view::SanshoCarrier<'d>
            )),
            "connections" => Ok(Some(
                Rc::new(&item.connections) as sansho::view::SanshoCarrier<'d>
            )),
            "body_mime_type" => Ok(Some(
                Rc::new(&item.body_mime_type) as sansho::view::SanshoCarrier<'d>
            )),
            "body" => Ok(Some(Rc::new(&item.body) as sansho::view::SanshoCarrier<'d>)),
            _ => Ok(None),
        }
    }

    fn as_object(&self) -> Result<HashMap<&str, SanshoCarrier<'d>>, sansho::SanshoError> {
        let item: &'d Item = self;
        // the keys are the item's own names: no key string is built
        //
        let mut ret = HashMap::new();
        ret.insert(
            "identifier",
            Rc::new(&item.identifier) as sansho::view::SanshoCarrier<'d>,
        );
        ret.insert(
            "connections",
            Rc::new(&item.connections) as sansho::view::SanshoCarrier<'d>,
        );
        ret.insert(
            "body_mime_type",
            Rc::new(&item.body_mime_type) as sansho::view::SanshoCarrier<'d>,
        );
        ret.insert(
            "body",
            Rc::new(&item.body) as sansho::view::SanshoCarrier<'d>,
        );

        Ok(ret)
    }

    fn container_len(&self) -> Result<usize, sansho::SanshoError> {
        Ok(4)
    }

    fn to_string_sansho(&self) -> String {
        // the item's own four fields, each rendered by its own type
        let item: &'d Item = self;
        format!(
            "{{identifier: {}, connections: {}, body_mime_type: {}, body: {}}}",
            item.identifier.to_string_sansho(),
            (&item.connections).to_string_sansho(),
            (&item.body_mime_type).to_string_sansho(),
            (&item.body).to_string_sansho(),
        )
    }
}

// The JSON view of an in-memory CBOR value — the engine's own
// conversion (moved into the engine with the `serde_cbor::Value`
// source; the item view re-exports it, so the two CBOR paths cannot
// drift).

#[cfg(test)]
mod tests {
    #[test]
    fn raw_accessor_returns_exact_span() {
        // the item bytes: a tiny CBOR map {"a": 1} = 0xA1 0x61 0x61 0x01;
        // the layout = [u32 LE length][item][trailing garbage]
        let item_bytes: &[u8] = &[0xA1, 0x61, 0x61, 0x01];
        let mut data = (item_bytes.len() as u32).to_le_bytes().to_vec();
        data.extend_from_slice(item_bytes);
        data.extend_from_slice(&[0xFF; 3]); // the trailing garbage is NOT the item's

        let span = crate::rodeo::data::read_item_bytes_at(&data, 0)
            .expect("the item exists at the offset");
        assert_eq!(span, item_bytes);
    }
}
