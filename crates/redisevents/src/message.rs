//! Typed messages and the payload codec seam. **Owner: W0.2** (final; [`Json`] is complete).
//!
//! A typed message is any serde type that names its wire type string through [`Message::TYPE`].
//! Rust has no stable type name to default to, so the type string is mandatory (`WIRE.md` §4 "Type strings"). The payload format is a type parameter
//! implementing [`PayloadCodec`]: [`Json`] here, MessagePack (+ LZ4) in `redisevents-msgpack` (E1).
//!
//! An LZ4 MessagePack body is in MessagePack-CSharp's `Lz4BlockArray` framing (ext 98), which a
//! plain MessagePack decoder (or C# `Standard` options) cannot read: a codec that may receive such
//! bodies must decode `Lz4BlockArray` (and `Lz4Block`, ext 99) itself.

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::BoxError;

/// A message type with a stable, explicit wire type string (the `t` field).
///
/// ```
/// use redisevents::Message;
///
/// #[derive(serde::Serialize, serde::Deserialize)]
/// #[serde(rename_all = "PascalCase")] // match System.Text.Json's default casing
/// struct OrderPlaced { order_id: String }
///
/// impl Message for OrderPlaced {
///     const TYPE: &'static str = "MyCo.Orders.OrderPlaced";
/// }
/// ```
pub trait Message {
    /// The wire type string. Must equal what the C# side publishes or filters on.
    const TYPE: &'static str;
}

/// A payload format: turns a value into body bytes and back.
///
/// Implementations are zero-sized marker types used as type parameters, so the methods take no
/// `self`.
pub trait PayloadCodec: Send + Sync + 'static {
    /// A short name for errors and logs, for example `"json"`.
    const NAME: &'static str;

    /// Encodes `value` as a message body.
    ///
    /// # Errors
    /// [`CodecError`] when serialization fails.
    fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Bytes, CodecError>;

    /// Decodes a message body.
    ///
    /// # Errors
    /// [`CodecError`] when the body is not a valid `T`.
    fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T, CodecError>;
}

/// JSON via `serde_json`. Use `#[serde(rename_all = "PascalCase")]` on types shared with C#
/// (`docs/PLAN.md` §2.11).
#[derive(Debug, Clone, Copy, Default)]
pub struct Json;

impl PayloadCodec for Json {
    const NAME: &'static str = "json";

    fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Bytes, CodecError> {
        serde_json::to_vec(value)
            .map(Bytes::from)
            .map_err(|e| CodecError::new(Self::NAME, e))
    }

    fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T, CodecError> {
        serde_json::from_slice(body).map_err(|e| CodecError::new(Self::NAME, e))
    }
}

/// A payload failed to encode or decode.
#[derive(Debug, thiserror::Error)]
#[error("{codec} payload error: {source}")]
pub struct CodecError {
    codec: &'static str,
    #[source]
    source: BoxError,
}

impl CodecError {
    /// An error from the codec named `codec`.
    pub fn new(codec: &'static str, source: impl Into<BoxError>) -> Self {
        Self {
            codec,
            source: source.into(),
        }
    }

    /// The codec's [`PayloadCodec::NAME`].
    #[must_use]
    pub fn codec(&self) -> &'static str {
        self.codec
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    struct Sample {
        item_id: u32,
    }

    #[test]
    fn json_round_trips_pascal_case() {
        let body = Json::encode(&Sample { item_id: 7 }).unwrap();
        assert_eq!(&body[..], br#"{"ItemId":7}"#);
        assert_eq!(
            Json::decode::<Sample>(&body).unwrap(),
            Sample { item_id: 7 }
        );
        assert_eq!(Json::decode::<Sample>(b"{}").unwrap_err().codec(), "json");
    }
}
