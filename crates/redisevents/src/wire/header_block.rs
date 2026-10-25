//! [`HeaderBlock`], the packed custom headers of an entry (field `h`). **API: W0.2. Bodies: A1**
//! (`WIRE.md` §5).
//!
//! Format: `len(key) ':' key len(value) ':' value …`, lengths in ASCII decimal UTF-8 bytes. At most
//! [`HeaderBlock::MAX_COUNT`] headers and [`HeaderBlock::MAX_BYTES`] packed bytes; the writer errors
//! rather than truncate, the reader errors on malformed input. Order is kept and duplicates are
//! allowed (lookup returns the first).
//!
//! The block is stored packed and parsed lazily, so a message whose handler never looks at its
//! headers pays nothing. Only this module looks inside the packed bytes.

use std::borrow::Cow;
use std::fmt;

use bytes::Bytes;

use crate::error::{Error, Result};

/// Packed custom headers. Cheap to clone (it is a [`Bytes`]); equality is byte equality of the
/// packed form, like the C# struct.
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct HeaderBlock {
    packed: Bytes,
}

impl HeaderBlock {
    /// The most headers one message may carry.
    pub const MAX_COUNT: usize = 64;
    /// The largest packed block, in bytes.
    pub const MAX_BYTES: usize = 8 * 1024;

    /// The empty block. An entry with no headers omits the `h` field.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            packed: Bytes::new(),
        }
    }

    /// Wraps an already packed block, for example the `h` field of an entry read from Redis.
    /// Not validated here: malformed input is reported when the block is read.
    #[must_use]
    pub const fn from_packed(packed: Bytes) -> Self {
        Self { packed }
    }

    /// Packs `headers` in order.
    ///
    /// # Errors
    /// [`Error::InvalidArgument`](crate::Error::InvalidArgument) when there are more than
    /// [`HeaderBlock::MAX_COUNT`] headers or the packed form would exceed
    /// [`HeaderBlock::MAX_BYTES`]. Never truncates.
    pub fn pack<K: AsRef<str>, V: AsRef<str>>(headers: &[(K, V)]) -> Result<Self> {
        if headers.len() > Self::MAX_COUNT {
            return Err(Error::invalid_argument(format!(
                "A message may carry at most {} headers; {} were supplied.",
                Self::MAX_COUNT,
                headers.len()
            )));
        }
        let total = headers.iter().try_fold(0usize, |total, (key, value)| {
            total
                .checked_add(field_size(key.as_ref().len()))
                .and_then(|t| t.checked_add(field_size(value.as_ref().len())))
        });
        match total {
            Some(total) if total <= Self::MAX_BYTES => {}
            _ => {
                return Err(Error::invalid_argument(format!(
                    "Packed headers exceed the {} byte cap.",
                    Self::MAX_BYTES
                )));
            }
        }
        let mut packed = Vec::with_capacity(total.unwrap_or(0));
        for (key, value) in headers {
            push_field(&mut packed, key.as_ref());
            push_field(&mut packed, value.as_ref());
        }
        Ok(Self {
            packed: Bytes::from(packed),
        })
    }

    /// The packed bytes, as written to the `h` field.
    #[must_use]
    pub const fn packed(&self) -> &Bytes {
        &self.packed
    }

    /// `true` when there are no headers.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.packed.is_empty()
    }

    /// The value of the first header whose key equals `key` byte for byte.
    ///
    /// # Errors
    /// [`Error::Transport`](crate::Error::Transport) when the block is malformed up to the match.
    pub fn get_bytes(&self, key: &[u8]) -> Result<Option<&[u8]>> {
        let data: &[u8] = &self.packed;
        let mut at = 0;
        while let Some((k_start, k_len)) = read_field(data, &mut at)? {
            let value = read_value(data, &mut at)?;
            if &data[k_start..k_start + k_len] == key {
                return Ok(Some(&data[value.0..value.0 + value.1]));
            }
        }
        Ok(None)
    }

    /// [`HeaderBlock::get_bytes`] for a UTF-8 key, decoding the value lossily (invalid UTF-8
    /// becomes U+FFFD, as .NET's `Encoding.UTF8.GetString` does).
    ///
    /// # Errors
    /// As [`HeaderBlock::get_bytes`].
    pub fn get(&self, key: &str) -> Result<Option<Cow<'_, str>>> {
        Ok(self.get_bytes(key.as_bytes())?.map(String::from_utf8_lossy))
    }

    /// Iterates the headers in order. Each item is a [`Header`] or, once, the malformed-block error
    /// that ends the iteration.
    #[must_use]
    pub fn iter(&self) -> HeaderIter<'_> {
        HeaderIter {
            data: &self.packed,
            at: 0,
            failed: false,
        }
    }

    /// The number of headers.
    ///
    /// # Errors
    /// [`Error::Transport`](crate::Error::Transport) when the block is malformed.
    pub fn count(&self) -> Result<usize> {
        self.iter().try_fold(0, |n, h| h.map(|_| n + 1))
    }

    /// The headers as owned `(key, value)` pairs, in order, duplicates kept.
    ///
    /// # Errors
    /// [`Error::Transport`](crate::Error::Transport) when the block is malformed.
    pub fn to_vec(&self) -> Result<Vec<(String, String)>> {
        self.iter()
            .map(|h| h.map(|h| (h.key().into_owned(), h.value().into_owned())))
            .collect()
    }
}

impl fmt::Debug for HeaderBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("HeaderBlock")
            .field(&String::from_utf8_lossy(&self.packed))
            .finish()
    }
}

impl<'a> IntoIterator for &'a HeaderBlock {
    type Item = Result<Header<'a>>;
    type IntoIter = HeaderIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// One header, borrowed from its [`HeaderBlock`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header<'a> {
    /// The key's UTF-8 bytes.
    pub key_utf8: &'a [u8],
    /// The value's UTF-8 bytes.
    pub value_utf8: &'a [u8],
}

impl<'a> Header<'a> {
    /// The key, decoded lossily.
    #[must_use]
    pub fn key(&self) -> Cow<'a, str> {
        String::from_utf8_lossy(self.key_utf8)
    }

    /// The value, decoded lossily.
    #[must_use]
    pub fn value(&self) -> Cow<'a, str> {
        String::from_utf8_lossy(self.value_utf8)
    }
}

/// Iterator over a [`HeaderBlock`]; see [`HeaderBlock::iter`].
#[derive(Debug, Clone)]
pub struct HeaderIter<'a> {
    data: &'a [u8],
    at: usize,
    failed: bool,
}

impl<'a> Iterator for HeaderIter<'a> {
    type Item = Result<Header<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        let result = read_header(self.data, &mut self.at);
        match result {
            Ok(Some(header)) => Some(Ok(header)),
            Ok(None) => None,
            Err(e) => {
                self.failed = true;
                Some(Err(e))
            }
        }
    }
}

/// The on-wire size of one field: the decimal length, `:`, then the bytes.
const fn field_size(len: usize) -> usize {
    decimal_digits(len) + 1 + len
}

const fn decimal_digits(mut value: usize) -> usize {
    let mut digits = 1;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

fn push_field(out: &mut Vec<u8>, text: &str) {
    out.extend_from_slice(text.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(text.as_bytes());
}

fn malformed(detail: &str) -> Error {
    Error::transport(format!(
        "Packed stream headers are malformed: {detail}. The entry was written by an incompatible codec or the value has been corrupted."
    ))
}

/// Reads the field at `*at` and returns its `(start, len)`, or `None` at the end of the block.
/// Advances `*at` past the field.
fn read_field(data: &[u8], at: &mut usize) -> Result<Option<(usize, usize)>> {
    if *at >= data.len() {
        return Ok(None);
    }
    let mut declared: usize = 0;
    let mut digits = 0;
    let mut cursor = *at;
    while cursor < data.len() && data[cursor] != b':' {
        let digit = data[cursor];
        if !digit.is_ascii_digit() {
            return Err(malformed("a length prefix contains a non-digit byte"));
        }
        declared = declared
            .checked_mul(10)
            .and_then(|v| v.checked_add(usize::from(digit - b'0')))
            .filter(|&v| v <= data.len())
            .ok_or_else(|| malformed("a length prefix runs past the end of the block"))?;
        cursor += 1;
        digits += 1;
    }
    if digits == 0 || cursor >= data.len() {
        return Err(malformed("a length prefix is empty or unterminated"));
    }
    cursor += 1;
    if declared > data.len() - cursor {
        return Err(malformed(
            "a field's declared length runs past the end of the block",
        ));
    }
    *at = cursor + declared;
    Ok(Some((cursor, declared)))
}

/// Like [`read_field`], but a missing value is an error: a key with no value.
fn read_value(data: &[u8], at: &mut usize) -> Result<(usize, usize)> {
    read_field(data, at)?
        .ok_or_else(|| malformed("the block ends after a key with no matching value"))
}

fn read_header<'a>(data: &'a [u8], at: &mut usize) -> Result<Option<Header<'a>>> {
    let Some((k_start, k_len)) = read_field(data, at)? else {
        return Ok(None);
    };
    let (v_start, v_len) = read_value(data, at)?;
    Ok(Some(Header {
        key_utf8: &data[k_start..k_start + k_len],
        value_utf8: &data[v_start..v_start + v_len],
    }))
}
