use anyhow::{bail, Result};
use rand::{rngs::ThreadRng, Rng};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

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
        MD5 { hash: rng.gen() }
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
    rng.gen()
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]

pub enum PayloadType {
    ENTRY = 0,
}

impl TryInto<PayloadType> for u8 {
    fn try_into(self) -> Result<PayloadType> {
        match self {
            0 => Ok(PayloadType::ENTRY),
            x => bail!("Payload type doesn't have an enum for {}", x),
        }
    }

    type Error = anyhow::Error;
}

impl PayloadType {
    pub fn random(_rng: &mut ThreadRng) -> PayloadType {
        PayloadType::ENTRY
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
#[repr(u8)]
pub enum PayloadFormat {
    CBOR = 0,
    JSON = 1,
}

impl TryInto<PayloadFormat> for u8 {
    fn try_into(self) -> Result<PayloadFormat> {
        match self {
            0 => Ok(PayloadFormat::CBOR),
            1 => Ok(PayloadFormat::JSON),
            _ => bail!("Unable to find PayloadFormat for {}", self),
        }
    }

    type Error = anyhow::Error;
}

impl PayloadFormat {
    pub fn random(rng: &mut ThreadRng) -> PayloadFormat {
        loop {
            if let Ok(pf) = rng.gen::<u8>().try_into() {
                return pf;
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
#[repr(u8)]
pub enum PayloadCompression {
    NONE = 0,
    ZLIB = 1,
    DEFLATE = 2,
    GZIP = 3,
}

impl TryInto<PayloadCompression> for u8 {
    fn try_into(self) -> Result<PayloadCompression> {
        match self {
            0 => Ok(PayloadCompression::NONE),
            1 => Ok(PayloadCompression::ZLIB),
            2 => Ok(PayloadCompression::DEFLATE),
            3 => Ok(PayloadCompression::GZIP),
            x => bail!("Payload compression doesn't have an enum for {}", x),
        }
    }

    type Error = anyhow::Error;
}

impl PayloadCompression {
    pub fn random(rng: &mut ThreadRng) -> PayloadCompression {
        loop {
            if let Ok(pf) = rng.gen::<u8>().try_into() {
                return pf;
            }
        }
    }
}

// #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
// pub struct MultifilePosition {
//     #[serde(alias = "o")]
//     #[serde(rename(serialize = "o"))]
//     pub offset: Position,

//     #[serde(alias = "t")]
//     #[serde(rename(serialize = "t"))]
//     pub other: u64,
// }

type MultifilePosition = (u64, u64);

pub const MULTIFILE_NOOP: MultifilePosition = (0u64, 0u64);
pub fn multifile_position_rand(rng: &mut ThreadRng) -> MultifilePosition {
    (rng.gen(), rng.gen())
}
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
pub struct ItemEnvelope {
    #[serde(alias = "h")]
    #[serde(rename(serialize = "h"))]
    pub key_md5: MD5,
    #[serde(alias = "p")]
    #[serde(rename(serialize = "p"))]
    pub position: Position,
    #[serde(alias = "t")]
    #[serde(rename(serialize = "t"))]
    pub timestamp: i64,
    #[serde(alias = "pv")]
    #[serde(rename(serialize = "pv"))]
    pub previous_version: MultifilePosition,
    #[serde(alias = "bp")]
    #[serde(rename(serialize = "bp"))]
    pub backpointer: u64,
    #[serde(alias = "l")]
    #[serde(rename(serialize = "l"))]
    pub data_len: u32,
    #[serde(alias = "f")]
    #[serde(rename(serialize = "f"))]
    pub data_format: PayloadFormat,
    #[serde(alias = "pt")]
    #[serde(rename(serialize = "pt"))]
    pub data_type: PayloadType,
    #[serde(alias = "c")]
    #[serde(rename(serialize = "c"))]
    pub compression: PayloadCompression,
    #[serde(alias = "m")]
    #[serde(rename(serialize = "m"))]
    pub merged_with_previous: bool,
}

impl ItemEnvelope {
    pub fn from_bin<R: Read>(r: &mut R) -> Result<ItemEnvelope> {
        serde_cbor::from_reader(r).map_err(|e| e.into())
    }

    pub fn random() -> ItemEnvelope {
        let mut rng = rand::thread_rng();
        ItemEnvelope {
            key_md5: MD5::random(&mut rng),
            position: random_position(&mut rng),
            timestamp: rng.gen(),
            previous_version: multifile_position_rand(&mut rng),
            backpointer: rng.gen(),
            data_len: rng.gen::<u32>() & 0x7ffffff,
            data_format: PayloadFormat::random(&mut rng),
            data_type: PayloadType::random(&mut rng),
            compression: PayloadCompression::random(&mut rng),
            merged_with_previous: rng.gen(),
        }
    }

    pub fn to_bin<W: Write>(&self, w: &mut W) -> Result<()> {
        serde_cbor::to_writer(w, self).map_err(|e| e.into())
    }

    pub fn bytes(&self) -> Result<Vec<u8>> {
        serde_cbor::to_vec(self).map_err(|e| e.into())
    }
}

#[test]
fn test_read_write_data() {
    for _i in 0..=1000 {
        let e: ItemEnvelope = ItemEnvelope::random();
        let bytes = e.bytes().unwrap();
        let e2 = ItemEnvelope::from_bin(&mut &*bytes).unwrap();

        assert_eq!(e, e2);

        assert_eq!(bytes, e2.bytes().unwrap());
    }
}
