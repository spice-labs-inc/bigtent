use std::sync::Arc;

use im::OrdMap;

use crate::index_file::ItemOffset;

pub struct ClusterPos /*<T>*/ {
  pub cluster: Arc<Vec<ItemOffset>>,
  pub pos: usize,
  pub len: usize,
  // pub thing: T,
}

#[derive(Debug, Clone)]
pub struct ItemOffsetPlusWhich {
  pub item_offset: ItemOffset,
  pub which: usize,
}

pub fn update_top<I: IntoIterator<Item = usize> /* , T*/>(
  top: &mut OrdMap<[u8; 16], Vec<ItemOffsetPlusWhich>>,
  index_holder: &mut Vec<ClusterPos /*<T>*/>,
  what: I,
) {
  for j in what {
    let tmp = &mut index_holder[j];
    if tmp.pos < tmp.len {
      let v = &tmp.cluster[tmp.pos];
      tmp.pos += 1;
      match top.get(&v.hash) {
        None => {
          top.insert(
            v.hash,
            vec![ItemOffsetPlusWhich {
              item_offset: v.clone(),
              which: j,
            }],
          );
        }
        Some(vec) => {
          top.insert(v.hash, {
            let mut vec = vec.clone();
            vec.push(ItemOffsetPlusWhich {
              item_offset: v.clone(),
              which: j,
            });
            vec
          });
        }
      }
    }
  }
}
