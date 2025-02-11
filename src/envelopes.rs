use rand::{rngs::ThreadRng, Rng};
use serde::{Deserialize, Serialize};

/** Contains an MD5 hash
 *
 * @param hash
 *   the hash contained... should be 16 bytes
 */
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
pub struct MD5 {
  #[serde(alias = "h")]
  #[serde(rename(serialize = "h"))]
  pub hash: [u8; 16],
}

impl MD5 {
  pub fn random(rng: &mut ThreadRng) -> MD5 {
    MD5 { hash: rng.random() }
  }
}

type Position = u64;

// #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
// pub struct Position {
//     #[serde(alias = "o")]
//     #[serde(rename(serialize = "o"))]
//     pub offset: u64,
// }

pub fn random_position(rng: &mut ThreadRng) -> Position {
  rng.random()
}

type MultifilePosition = (u64, u64);

pub const MULTIFILE_NOOP: MultifilePosition = (0u64, 0u64);
pub fn multifile_position_rand(rng: &mut ThreadRng) -> MultifilePosition {
  (rng.random(), rng.random())
}
