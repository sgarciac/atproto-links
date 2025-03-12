use crate::Nsid; //, Cursor};
use bincode::{
    config::{standard, Config},
    decode_from_slice, encode_to_vec,
    error::{DecodeError, EncodeError},
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BlahError {
    #[error("could not convert from utf8: {0}")]
    NotUtf8(#[from] std::str::Utf8Error),
    #[error("failed to parse NSID: {0}")]
    BadNSID(&'static str),
    #[error("failed to encode: {0}")]
    EncodeFailed(#[from] EncodeError),
    #[error("failed to decode: {0}")]
    DecodeFailed(#[from] DecodeError),
    #[error("decode missing suffix bytes")]
    DecodeMissingSuffix,
}

fn bconf() -> impl Config {
    standard().with_big_endian().with_fixed_int_encoding()
}

trait DbBytes {
    fn to_bincoded(&self) -> Result<Vec<u8>, BlahError>;
    fn from_bincoded(bytes: &[u8]) -> Result<(Self, usize), BlahError>
    where
        Self: Sized;
}

trait DbStringType: AsRef<str> {
    fn from_string(s: String) -> Result<Self, BlahError>
    where
        Self: Sized;
}

impl<T: DbStringType> DbBytes for T {
    fn to_bincoded(&self) -> Result<Vec<u8>, BlahError> {
        Ok(encode_to_vec(self.as_ref(), bconf())?)
    }
    fn from_bincoded(bytes: &[u8]) -> Result<(Self, usize), BlahError>
    where
        Self: Sized,
    {
        let (s, n) = decode_from_slice(bytes, bconf())?;
        let me = Self::from_string(s)?;
        Ok((me, n))
    }
}

impl DbStringType for Nsid {
    fn from_string(s: String) -> Result<Self, BlahError> {
        Self::new(s).map_err(BlahError::BadNSID)
    }
}

struct DbKeyWithPrefix<P: DbBytes, S: DbBytes> {
    prefix: P,
    suffix: S,
}

impl<P: DbBytes, S: DbBytes> DbKeyWithPrefix<P, S> {
    fn to_prefix_bincoded(&self) -> Result<Vec<u8>, BlahError> {
        self.prefix.to_bincoded()
    }
}

impl<P: DbBytes, S: DbBytes> DbBytes for DbKeyWithPrefix<P, S> {
    fn to_bincoded(&self) -> Result<Vec<u8>, BlahError> {
        let mut combined = self.prefix.to_bincoded()?;
        combined.append(&mut self.suffix.to_bincoded()?);
        Ok(combined)
    }
    fn from_bincoded(bytes: &[u8]) -> Result<(Self, usize), BlahError>
    where
        Self: Sized,
    {
        let (prefix, eaten) = P::from_bincoded(bytes)?;
        let Some(suffix_bytes) = bytes.get(eaten..) else {
            return Err(BlahError::DecodeMissingSuffix);
        };
        let (suffix, also_eaten) = S::from_bincoded(suffix_bytes)?;
        Ok((Self { prefix, suffix }, eaten + also_eaten))
    }
}

fn blah(nsd: Nsid) {
    let _ = nsd.to_bincoded().unwrap();
}
