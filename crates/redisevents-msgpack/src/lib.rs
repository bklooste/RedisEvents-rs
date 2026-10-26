//! MessagePack typed publish/consume for redisevents, with size-gated LZ4 in the framing
//! `MessagePack-CSharp` reads and writes.
//!
//! The pure layer is [`encode_with`] / [`decode`] and the [`MessagePack`] and [`MessagePackMap`]
//! [`PayloadCodec`] markers. The typed glue is:
//!
//! - **Publish**: [`MessagePackPublisherExt`] on any [`Publisher`] (the stream publisher, the
//!   buffered publisher, an `Arc` of either, or a test double). `publish_msgpack` takes the
//!   [`Options`] explicitly (layout, compression threshold, or disabling compression). For the
//!   defaults, core's `PublisherExt::publish_message::<MessagePack, _>` does the same thing.
//! - **Consume**: [`MessagePackTyped`] / [`MessagePackMapTyped`] wrap a `TypedHandler` or
//!   `TypedBatchHandler` as `Typed<H, T, MessagePack>`. Decoding detects LZ4 from the bytes, so the
//!   consumer needs no compression setting. To decode one `StreamMsg` by hand, call
//!   [`decode`]`(msg.body())`.
//!
//! ```no_run
//! use redisevents::{Message, PublishOptions, Publisher};
//! use redisevents_msgpack::{MessagePackPublisherExt, Options};
//!
//! #[derive(serde::Serialize, serde::Deserialize)]
//! struct OrderPlaced { id: i32 }
//! impl Message for OrderPlaced { const TYPE: &'static str = "Orders.OrderPlaced"; }
//!
//! # async fn f(p: &redisevents::producer::StreamPublisher) -> redisevents::Result<()> {
//! p.publish_msgpack("order-1", &OrderPlaced { id: 1 }, &Options::default(), &PublishOptions::new())
//!     .await?;
//! # Ok(()) }
//! ```
//!
//! Wire rules (`wire-spec/WIRE.md` section 12):
//! - Structs are MessagePack arrays by default (what .NET `[Key(int)]` types write);
//!   [`Layout::Map`] gives the map form (`[Key("name")]`).
//! - A body whose uncompressed encoding exceeds the threshold (default 1024 bytes) is written as
//!   `Lz4BlockArray`: an array whose first element is ext 98 holding the uncompressed chunk
//!   lengths, then one `bin` of raw LZ4 *block* data per chunk.
//! - Readers detect `Lz4BlockArray` (ext 98), `Lz4Block` (ext 99) and plain bodies from the bytes.

use std::future::Future;

use bytes::Bytes;
use redisevents::consumer::Typed;
use redisevents::message::CodecError;
use redisevents::producer::PublishOptions;
use redisevents::{Message, PayloadCodec, Publisher, Result, StreamId};
use serde::Serialize;
use serde::de::DeserializeOwned;

const NAME: &str = "messagepack";
/// `MessagePack-CSharp` extension type code for `Lz4BlockArray`.
const EXT_LZ4_BLOCK_ARRAY: i8 = 98;
/// `MessagePack-CSharp` extension type code for `Lz4Block`.
const EXT_LZ4_BLOCK: i8 = 99;
/// Upper bound on a declared decompressed size, so a hostile header cannot force a huge allocation.
const MAX_DECOMPRESSED: usize = 256 * 1024 * 1024;

/// How structs are laid out on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Layout {
    /// Positional arrays, as .NET `[Key(int)]` types.
    #[default]
    Array,
    /// Field-name maps, as .NET `[Key("name")]` types.
    Map,
}

/// Size-gated LZ4 settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Compression {
    /// Whether to compress bodies over [`Compression::threshold`]. Default: `true`.
    pub enabled: bool,
    /// Uncompressed size above which a body is compressed. Default: 1024.
    pub threshold: usize,
    /// Largest uncompressed chunk in the `Lz4BlockArray` form. Default: 64 KiB.
    pub chunk_size: usize,
}

impl Compression {
    /// Enabled, 1024-byte threshold.
    pub const DEFAULT: Self = Self {
        enabled: true,
        threshold: 1024,
        chunk_size: 64 * 1024,
    };
    /// Never compresses.
    pub const DISABLED: Self = Self {
        enabled: false,
        ..Self::DEFAULT
    };
}

impl Default for Compression {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Encoding options.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Options {
    /// Struct layout.
    pub layout: Layout,
    /// LZ4 settings.
    pub compression: Compression,
}

fn err(e: impl Into<redisevents::BoxError>) -> CodecError {
    CodecError::new(NAME, e)
}

fn bad(msg: &str) -> CodecError {
    err(msg.to_owned())
}

/// Serializes `value` without any compression.
///
/// # Errors
/// [`CodecError`] when serialization fails.
pub fn encode_plain<T: Serialize + ?Sized>(
    value: &T,
    layout: Layout,
) -> Result<Vec<u8>, CodecError> {
    match layout {
        Layout::Array => rmp_serde::to_vec(value),
        Layout::Map => rmp_serde::to_vec_named(value),
    }
    .map_err(err)
}

/// Serializes `value`, re-encoding as `Lz4BlockArray` when the plain encoding exceeds the
/// threshold.
///
/// # Errors
/// [`CodecError`] when serialization fails.
pub fn encode_with<T: Serialize + ?Sized>(
    value: &T,
    options: &Options,
) -> Result<Bytes, CodecError> {
    let plain = encode_plain(value, options.layout)?;
    let c = &options.compression;
    if c.enabled && plain.len() > c.threshold {
        Ok(Bytes::from(frame_block_array(&plain, c.chunk_size.max(1))))
    } else {
        Ok(Bytes::from(plain))
    }
}

/// Wraps `plain` in `Lz4BlockArray` framing using chunks of at most `chunk_size` bytes.
#[must_use]
pub fn frame_block_array(plain: &[u8], chunk_size: usize) -> Vec<u8> {
    let lens: Vec<usize> = plain.chunks(chunk_size.max(1)).map(<[u8]>::len).collect();
    frame_block_array_with(plain, &lens)
}

/// Wraps `plain` in `Lz4BlockArray` framing with explicit chunk lengths, which must sum to
/// `plain.len()`.
///
/// # Panics
/// When the chunk lengths exceed `plain.len()`.
#[must_use]
pub fn frame_block_array_with(plain: &[u8], lens: &[usize]) -> Vec<u8> {
    let mut ints = Vec::new();
    for &l in lens {
        rmp::encode::write_uint(&mut ints, l as u64).expect("write to Vec");
    }
    let mut out = Vec::new();
    rmp::encode::write_array_len(&mut out, u32::try_from(lens.len() + 1).unwrap_or(u32::MAX))
        .expect("write to Vec");
    write_ext_header(&mut out, ints.len(), EXT_LZ4_BLOCK_ARRAY);
    out.extend_from_slice(&ints);
    let mut at = 0;
    for &l in lens {
        let block = lz4_flex::block::compress(&plain[at..at + l]);
        at += l;
        // MessagePack-CSharp always writes the bin32 header.
        out.push(0xc6);
        out.extend_from_slice(&u32::try_from(block.len()).unwrap_or(u32::MAX).to_be_bytes());
        out.extend_from_slice(&block);
    }
    out
}

/// Wraps `plain` in the `Lz4Block` (ext 99) form: an int32 length then one LZ4 block.
#[must_use]
pub fn frame_block(plain: &[u8]) -> Vec<u8> {
    let block = lz4_flex::block::compress(plain);
    let mut payload = vec![0xd2];
    payload.extend_from_slice(&i32::try_from(plain.len()).unwrap_or(i32::MAX).to_be_bytes());
    payload.extend_from_slice(&block);
    let mut out = Vec::new();
    write_ext_header(&mut out, payload.len(), EXT_LZ4_BLOCK);
    out.extend_from_slice(&payload);
    out
}

fn write_ext_header(out: &mut Vec<u8>, len: usize, ty: i8) {
    let t = ty.to_be_bytes()[0];
    match len {
        1 => out.push(0xd4),
        2 => out.push(0xd5),
        4 => out.push(0xd6),
        8 => out.push(0xd7),
        16 => out.push(0xd8),
        l if u8::try_from(l).is_ok() => {
            out.extend_from_slice(&[0xc7, u8::try_from(l).unwrap_or(0)]);
        }
        l if u16::try_from(l).is_ok() => {
            out.push(0xc8);
            out.extend_from_slice(&u16::try_from(l).unwrap_or(0).to_be_bytes());
        }
        l => {
            out.push(0xc9);
            out.extend_from_slice(&u32::try_from(l).unwrap_or(u32::MAX).to_be_bytes());
        }
    }
    out.push(t);
}

/// A small cursor over msgpack framing bytes.
struct Cur<'a> {
    b: &'a [u8],
}

impl<'a> Cur<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        if self.b.len() < n {
            return Err(bad("truncated LZ4 framing"));
        }
        let (h, t) = self.b.split_at(n);
        self.b = t;
        Ok(h)
    }

    fn byte(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }

    fn be(&mut self, n: usize) -> Result<usize, CodecError> {
        let v = self
            .take(n)?
            .iter()
            .fold(0u64, |a, &x| (a << 8) | u64::from(x));
        usize::try_from(v).map_err(|_| bad("length overflow"))
    }

    /// Reads any msgpack integer as a non-negative length.
    fn uint(&mut self) -> Result<usize, CodecError> {
        let m = self.byte()?;
        match m {
            0..=0x7f => Ok(usize::from(m)),
            0xcc => self.be(1),
            0xcd => self.be(2),
            0xce => self.be(4),
            0xcf => self.be(8),
            0xd0..=0xd3 => {
                let n = 1usize << (m - 0xd0);
                let raw = self.take(n)?;
                if raw[0] & 0x80 != 0 {
                    return Err(bad("negative length in LZ4 framing"));
                }
                Ok(raw.iter().fold(0usize, |a, &x| (a << 8) | usize::from(x)))
            }
            _ => Err(bad("expected an integer in LZ4 framing")),
        }
    }

    /// Reads an ext header, returning `(type, data length)`.
    fn ext(&mut self) -> Result<(i8, usize), CodecError> {
        let m = self.byte()?;
        let len = match m {
            0xd4 => 1,
            0xd5 => 2,
            0xd6 => 4,
            0xd7 => 8,
            0xd8 => 16,
            0xc7 => self.be(1)?,
            0xc8 => self.be(2)?,
            0xc9 => self.be(4)?,
            _ => return Err(bad("expected an extension")),
        };
        Ok((i8::from_be_bytes([self.byte()?]), len))
    }

    fn array_len(&mut self) -> Result<usize, CodecError> {
        let m = self.byte()?;
        match m {
            0x90..=0x9f => Ok(usize::from(m & 0x0f)),
            0xdc => self.be(2),
            0xdd => self.be(4),
            _ => Err(bad("expected an array")),
        }
    }

    fn bin(&mut self) -> Result<&'a [u8], CodecError> {
        let n = match self.byte()? {
            0xc4 => self.be(1)?,
            0xc5 => self.be(2)?,
            0xc6 => self.be(4)?,
            _ => return Err(bad("expected a bin chunk")),
        };
        self.take(n)
    }
}

fn checked_len(n: usize) -> Result<usize, CodecError> {
    if n > MAX_DECOMPRESSED {
        Err(bad("declared decompressed size is too large"))
    } else {
        Ok(n)
    }
}

fn inflate(chunk: &[u8], len: usize) -> Result<Vec<u8>, CodecError> {
    let out = lz4_flex::block::decompress(chunk, len).map_err(err)?;
    if out.len() == len {
        Ok(out)
    } else {
        Err(bad("LZ4 chunk length mismatch"))
    }
}

/// Returns the plain MessagePack bytes for `body`, decompressing `Lz4BlockArray` (ext 98) and
/// `Lz4Block` (ext 99) bodies; a plain body is returned unchanged (borrowed).
///
/// # Errors
/// [`CodecError`] when the LZ4 framing is malformed or a block fails to decompress.
pub fn decompress(body: &[u8]) -> Result<std::borrow::Cow<'_, [u8]>, CodecError> {
    use std::borrow::Cow;
    let Some(&first) = body.first() else {
        return Ok(Cow::Borrowed(body));
    };
    if matches!(first, 0x90..=0x9f | 0xdc | 0xdd) {
        let mut c = Cur { b: body };
        let n = c.array_len()?;
        let mut probe = Cur { b: c.b };
        if n >= 1 && matches!(probe.ext(), Ok((EXT_LZ4_BLOCK_ARRAY, _))) {
            let (_, elen) = c.ext()?;
            let mut lens_cur = Cur { b: c.take(elen)? };
            let mut out = Vec::new();
            for _ in 1..n {
                let len = checked_len(lens_cur.uint()?)?;
                let chunk = c.bin()?;
                out.extend_from_slice(&inflate(chunk, len)?);
                checked_len(out.len())?;
            }
            return Ok(Cow::Owned(out));
        }
    } else if matches!(first, 0xd4..=0xd8 | 0xc7..=0xc9) {
        let mut c = Cur { b: body };
        if let Ok((EXT_LZ4_BLOCK, elen)) = c.ext() {
            let mut d = Cur { b: c.take(elen)? };
            let len = checked_len(d.uint()?)?;
            return Ok(Cow::Owned(inflate(d.b, len)?));
        }
    }
    Ok(Cow::Borrowed(body))
}

/// Decodes a body of any form (plain, `Lz4BlockArray`, `Lz4Block`), detected from the bytes.
///
/// # Errors
/// [`CodecError`] when the framing or the MessagePack content is invalid for `T`.
pub fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T, CodecError> {
    let plain = decompress(body)?;
    rmp_serde::from_slice(&plain).map_err(err)
}

/// MessagePack with structs as arrays (.NET `[Key(int)]`) and default size-gated LZ4.
#[derive(Debug, Clone, Copy, Default)]
pub struct MessagePack;

impl PayloadCodec for MessagePack {
    const NAME: &'static str = NAME;

    fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Bytes, CodecError> {
        encode_with(value, &Options::default())
    }

    fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T, CodecError> {
        decode(body)
    }
}

/// MessagePack with structs as maps (.NET `[Key("name")]`) and default size-gated LZ4.
#[derive(Debug, Clone, Copy, Default)]
pub struct MessagePackMap;

impl PayloadCodec for MessagePackMap {
    const NAME: &'static str = "messagepack-map";

    fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Bytes, CodecError> {
        encode_with(
            value,
            &Options {
                layout: Layout::Map,
                ..Options::default()
            },
        )
    }

    fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T, CodecError> {
        decode(body)
    }
}

/// A `TypedHandler` or `TypedBatchHandler` consuming [`MessagePack`] bodies. Bodies are read as array-layout or map-layout, with or without
/// LZ4.
pub type MessagePackTyped<H, T> = Typed<H, T, MessagePack>;

/// Like [`MessagePackTyped`], for [`MessagePackMap`] bodies. Decoding accepts either layout, so this
/// alias only documents the producer's choice.
pub type MessagePackMapTyped<H, T> = Typed<H, T, MessagePackMap>;

/// Typed MessagePack publishing over any [`Publisher`]. The
/// type string is [`Message::TYPE`]. Blanket-implemented, so it is in scope wherever it is imported.
pub trait MessagePackPublisherExt: Publisher {
    /// Serializes `message` with `options` and publishes it under `M::TYPE`. A body over the
    /// threshold is `Lz4BlockArray` (unless `options.compression` disables it).
    ///
    /// # Errors
    /// [`redisevents::Error::Codec`] when serialization fails, otherwise whatever the publisher
    /// returns.
    fn publish_msgpack<M: Message + Serialize + Sync>(
        &self,
        partition_key: &str,
        message: &M,
        options: &Options,
        publish: &PublishOptions,
    ) -> impl Future<Output = Result<StreamId>> + Send {
        let body = encode_with(message, options);
        async move { self.publish(partition_key, body?, M::TYPE, publish).await }
    }

    /// Serializes each of `messages` with `options` and publishes them as one batch under `M::TYPE`,
    /// in order.
    ///
    /// # Errors
    /// As [`publish_msgpack`](Self::publish_msgpack).
    fn publish_msgpack_batch<M: Message + Serialize + Sync>(
        &self,
        partition_key: &str,
        messages: &[M],
        options: &Options,
        publish: &PublishOptions,
    ) -> impl Future<Output = Result<()>> + Send {
        let bodies: std::result::Result<Vec<Bytes>, CodecError> =
            messages.iter().map(|m| encode_with(m, options)).collect();
        async move {
            self.publish_batch(partition_key, &bodies?, M::TYPE, publish)
                .await
        }
    }
}

impl<P: Publisher + ?Sized> MessagePackPublisherExt for P {}
