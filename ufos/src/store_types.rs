use crate::{Cursor, Nsid};
use bincode::{
    config::{standard, Config},
    de::Decode as BincodeDecode,
    decode_from_slice,
    enc::Encode as BincodeEncode,
    encode_to_vec,
    error::{DecodeError, EncodeError},
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum EncodingError {
    #[error("failed to parse NSID: {0}")]
    BadNSID(&'static str),
    #[error("failed to bincode-encode: {0}")]
    BincodeEncodeFailed(#[from] EncodeError),
    #[error("failed to bincode-decode: {0}")]
    BincodeDecodeFailed(#[from] DecodeError),
    #[error("decode missing suffix bytes")]
    DecodeMissingSuffix,
    #[error("decode ran out of bytes")]
    DecodeNotEnoughBytes,
}

fn bincode_conf() -> impl Config {
    standard().with_big_endian().with_fixed_int_encoding()
}

pub trait DbBytes {
    fn to_bytes(&self) -> Result<Vec<u8>, EncodingError>;
    fn from_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError>
    where
        Self: Sized;
}

pub trait DbStringType: AsRef<str> {
    fn from_string(s: String) -> Result<Self, EncodingError>
    where
        Self: Sized;
}

impl DbBytes for String {
    fn to_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        Ok(encode_to_vec::<&Self, _>(self, bincode_conf())?)
    }
    fn from_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        Ok(decode_from_slice(bytes, bincode_conf())?)
    }
}

pub struct DbKeyWithPrefix<P: DbBytes, S: DbBytes> {
    prefix: P,
    suffix: S,
}

impl<P: DbBytes, S: DbBytes> DbKeyWithPrefix<P, S> {
    pub fn to_prefix_bincoded(&self) -> Result<Vec<u8>, EncodingError> {
        self.prefix.to_bytes()
    }
}

impl<P: DbBytes, S: DbBytes> DbBytes for DbKeyWithPrefix<P, S> {
    fn to_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        let mut combined = self.prefix.to_bytes()?;
        combined.append(&mut self.suffix.to_bytes()?);
        Ok(combined)
    }
    fn from_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError>
    where
        Self: Sized,
    {
        let (prefix, eaten) = P::from_bytes(bytes)?;
        let Some(suffix_bytes) = bytes.get(eaten..) else {
            return Err(EncodingError::DecodeMissingSuffix);
        };
        let (suffix, also_eaten) = S::from_bytes(suffix_bytes)?;
        Ok((Self { prefix, suffix }, eaten + also_eaten))
    }
}

trait Bincodeable: BincodeEncode + BincodeDecode<()> + Sized {}

impl<T> DbBytes for T
where
    T: Bincodeable,
{
    fn to_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        Ok(encode_to_vec(self, bincode_conf())?)
    }
    fn from_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        Ok(decode_from_slice(bytes, bincode_conf())?)
    }
}

//////

impl DbBytes for Nsid {
    fn from_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        let (s, n) = decode_from_slice(bytes, bincode_conf())?;
        let me = Self::new(s).map_err(EncodingError::BadNSID)?;
        Ok((me, n))
    }
    fn to_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        Ok(encode_to_vec(self.as_ref(), bincode_conf())?)
    }
}

impl DbBytes for Cursor {
    fn to_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        Ok(self.to_raw_u64().to_be_bytes().to_vec())
    }
    fn from_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        let Ok(bytes8) = TryInto::<[u8; 8]>::try_into(bytes) else {
            return Err(EncodingError::DecodeNotEnoughBytes);
        };
        let cursor = Cursor::from_raw_u64(u64::from_be_bytes(bytes8));
        Ok((cursor, 8))
    }
}

///////

pub fn blah(nsd: Nsid) {
    let _ = nsd.to_bytes().unwrap();
}
