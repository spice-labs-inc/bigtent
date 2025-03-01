use std::sync::Arc;

use im::OrdMap;

use crate::index_file::{EitherItemOffset, EitherItemOffsetVec};

pub struct ClusterPos {
  pub cluster: Arc<EitherItemOffsetVec>,
  pub pos: usize,
  pub len: usize,
}

#[derive(Debug, Clone)]
pub struct ItemOffsetPlusWhich {
  pub item_offset: EitherItemOffset,
  pub which: usize,
}

pub fn update_top<I: IntoIterator<Item = usize>>(
  top: &mut OrdMap<[u8; 16], Vec<ItemOffsetPlusWhich>>,
  index_holder: &mut Vec<ClusterPos>,
  what: I,
) {
  for j in what {
    let tmp = &mut index_holder[j];
    if tmp.pos < tmp.len {
      let v = &tmp.cluster.item_at(tmp.pos);
      tmp.pos += 1;
      match top.get(v.hash()) {
        None => {
          top.insert(
            *v.hash(),
            vec![ItemOffsetPlusWhich {
              item_offset: v.clone(),
              which: j,
            }],
          );
        }
        Some(vec) => {
          top.insert(*v.hash(), {
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
