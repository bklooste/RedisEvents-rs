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

use crate::error::Result;

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
        let _ = headers;
        todo!("A1")
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
        let _ = key;
        todo!("A1")
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
        let _ = (self.data, self.at, self.failed);
        todo!("A1")
    }
}
