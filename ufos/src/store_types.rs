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
    #[error("string contained a null byte, which is not allowed, which is annoying, sorry")]
    StringContainedNull,
    #[error("string was not terminated with null byte")]
    UnterminatedString,
    #[error("could not convert from utf8: {0}")]
    NotUtf8(#[from] std::str::Utf8Error),
    #[error("could not get array from slice: {0}")]
    BadSlice(#[from] std::array::TryFromSliceError),
}

fn bincode_conf() -> impl Config {
    standard().with_big_endian().with_fixed_int_encoding()
}

pub trait DbBytes {
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError>;
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError>
    where
        Self: Sized;
}

#[derive(Debug, PartialEq)]
pub struct DbKeyWithPrefix<P: DbBytes, S: DbBytes> {
    prefix: P,
    suffix: S,
}

impl<P: DbBytes, S: DbBytes> DbKeyWithPrefix<P, S> {
    pub fn to_prefix_bincoded(&self) -> Result<Vec<u8>, EncodingError> {
        self.prefix.to_db_bytes()
    }
}

impl<P: DbBytes, S: DbBytes> DbBytes for DbKeyWithPrefix<P, S> {
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        let mut combined = self.prefix.to_db_bytes()?;
        combined.append(&mut self.suffix.to_db_bytes()?);
        Ok(combined)
    }
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError>
    where
        Self: Sized,
    {
        let (prefix, eaten) = P::from_db_bytes(bytes)?;
        let Some(suffix_bytes) = bytes.get(eaten..) else {
            return Err(EncodingError::DecodeMissingSuffix);
        };
        let (suffix, also_eaten) = S::from_db_bytes(suffix_bytes)?;
        Ok((Self { prefix, suffix }, eaten + also_eaten))
    }
}

trait Bincodeable: BincodeEncode + BincodeDecode<()> + Sized {}

impl<T> DbBytes for T
where
    T: Bincodeable,
{
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        Ok(encode_to_vec(self, bincode_conf())?)
    }
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        Ok(decode_from_slice(bytes, bincode_conf())?)
    }
}

//////

/// Lexicographic-sort-friendly null-terminating serialization for String
///
/// Null bytes technically can appear within utf-8 strings. Currently we will just bail in that case.
///
/// In the future, null bytes could be escaped, or maybe this becomes SLIP-encoded. Either should be
/// backwards-compatible I think.
impl DbBytes for String {
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        let mut v = self.as_bytes().to_vec();
        if v.contains(&0x00) {
            return Err(EncodingError::StringContainedNull);
        }
        v.push(0x00);
        Ok(v)
    }
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        for (i, byte) in bytes.iter().enumerate() {
            if *byte == 0x00 {
                let (string_bytes, _) = bytes.split_at(i);
                let s = std::str::from_utf8(string_bytes)?;
                return Ok((s.to_string(), i + 1)); // +1 for the null byte
            }
        }
        Err(EncodingError::UnterminatedString)
    }
}

impl DbBytes for Nsid {
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        let (s, n) = decode_from_slice(bytes, bincode_conf())?;
        let me = Self::new(s).map_err(EncodingError::BadNSID)?;
        Ok((me, n))
    }
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        Ok(encode_to_vec(self.as_ref(), bincode_conf())?)
    }
}

impl DbBytes for Cursor {
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        Ok(self.to_raw_u64().to_be_bytes().to_vec())
    }
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        if bytes.len() < 8 {
            return Err(EncodingError::DecodeNotEnoughBytes);
        }
        let bytes8 = TryInto::<[u8; 8]>::try_into(&bytes[..8])?;
        let cursor = Cursor::from_raw_u64(u64::from_be_bytes(bytes8));
        Ok((cursor, 8))
    }
}

#[cfg(test)]
mod test {
    use super::{Cursor, DbBytes, DbKeyWithPrefix, EncodingError};

    #[test]
    fn test_string_roundtrip() -> Result<(), EncodingError> {
        for (case, desc) in [
            ("", "empty string"),
            ("a", "basic string"),
            ("asdf asdf asdf even µnicode", "unicode string"),
        ] {
            let serialized = case.to_string().to_db_bytes()?;
            let (restored, bytes_consumed) = String::from_db_bytes(&serialized)?;
            assert_eq!(&restored, case, "string round-trip: {desc}");
            assert_eq!(
                bytes_consumed,
                serialized.len(),
                "exact bytes consumed for round-trip: {desc}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_string_serialized_lexicographic_sort() -> Result<(), EncodingError> {
        let aa = "aa".to_string().to_db_bytes()?;
        let b = "b".to_string().to_db_bytes()?;
        assert!(b > aa);
        Ok(())
    }

    #[test]
    fn test_string_cursor_prefix_roundtrip() -> Result<(), EncodingError> {
        type TwoThings = DbKeyWithPrefix<String, Cursor>;
        for (lazy_prefix, tired_suffix, desc) in [
            ("", 0, "empty string and cursor"),
            ("aaa", 0, "zero-cursor"),
            ("", 1234, "empty string"),
            ("aaaaa", 789, "string and cursor"),
        ] {
            let original = TwoThings {
                prefix: lazy_prefix.to_string(),
                suffix: Cursor::from_raw_u64(tired_suffix),
            };
            let serialized = original.to_db_bytes()?;
            let (restored, bytes_consumed) = TwoThings::from_db_bytes(&serialized)?;
            assert_eq!(restored, original, "round-trip: {desc}");
            assert_eq!(
                bytes_consumed,
                serialized.len(),
                "exact bytes consumed for round-trip: {desc}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_cursor_string_prefix_roundtrip() -> Result<(), EncodingError> {
        type TwoThings = DbKeyWithPrefix<Cursor, String>;
        for (tired_prefix, sad_suffix, desc) in [
            (0, "", "empty string and cursor"),
            (0, "aaa", "zero-cursor"),
            (1234, "", "empty string"),
            (789, "aaaaa", "string and cursor"),
        ] {
            eprintln!("{desc}");
            let original = TwoThings {
                prefix: Cursor::from_raw_u64(tired_prefix),
                suffix: sad_suffix.to_string(),
            };
            let serialized = original.to_db_bytes()?;
            let (restored, bytes_consumed) = TwoThings::from_db_bytes(&serialized)?;
            assert_eq!(restored, original, "round-trip: {desc}");
            assert_eq!(
                bytes_consumed,
                serialized.len(),
                "exact bytes consumed for round-trip: {desc}"
            );
        }
        Ok(())
    }
}
