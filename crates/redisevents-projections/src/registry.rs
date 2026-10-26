//! [`EventTypeRegistry`]: explicit wire type strings to Rust types and codecs.

use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::fmt;

use bytes::Bytes;
use redisevents::{BoxError, Error, Json, PayloadCodec, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;

type AnyBox = Box<dyn Any + Send + Sync>;
type DecodeFn = Box<dyn Fn(&str, &[u8]) -> Result<Option<AnyBox>, BoxError> + Send + Sync>;
type EncodeFn = Box<dyn Fn(&dyn Any) -> Result<Bytes, BoxError> + Send + Sync>;

/// A decoded event, type-erased. Its [`type_id`](Self::type_id) selects the bound projections.
pub struct Decoded {
    type_id: TypeId,
    value: AnyBox,
}

impl Decoded {
    /// The `TypeId` of the Rust type the wire type decoded to.
    #[must_use]
    pub fn type_id(&self) -> TypeId {
        self.type_id
    }

    /// The value as a `T`, if that is what it is.
    #[must_use]
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.value.downcast_ref()
    }

    /// Unwraps the value as a `T`, or gives `self` back.
    ///
    /// # Errors
    /// `self`, when the value is not a `T`.
    pub fn downcast<T: 'static>(self) -> Result<T, Self> {
        let Self { type_id, value } = self;
        value
            .downcast::<T>()
            .map(|b| *b)
            .map_err(|value| Self { type_id, value })
    }

    pub(crate) fn as_any(&self) -> &(dyn Any + Send + Sync) {
        &*self.value
    }
}

impl fmt::Debug for Decoded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Decoded")
            .field("type_id", &self.type_id)
            .finish_non_exhaustive()
    }
}

struct Entry {
    type_id: TypeId,
    decode: DecodeFn,
}

/// The wire types one topic carries.
///
/// A wire type string is the entry's `t` field and is chosen **explicitly** at registration; it
/// never comes from the Rust type name, so renaming or moving a type cannot change what is on the
/// wire. Registering one wire string twice, or one Rust type under two wire strings (with
/// [`register`](Self::register)), is a [`Error::Config`].
///
/// ```
/// use redisevents_projections::EventTypeRegistry;
///
/// #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
/// #[serde(rename_all = "PascalCase")] // .NET producers write System.Text.Json's default casing
/// struct WidgetCreated { id: String, name: String }
///
/// # fn main() -> redisevents::Result<()> {
/// let registry = EventTypeRegistry::new().register_json::<WidgetCreated>("widget.created")?;
/// let (wire_type, body) = registry.encode(&WidgetCreated { id: "w-1".into(), name: "Widget".into() })?;
/// assert_eq!(wire_type, "widget.created");
/// assert_eq!(&body[..], br#"{"Id":"w-1","Name":"Widget"}"#);
/// # Ok(()) }
/// ```
#[derive(Default)]
pub struct EventTypeRegistry {
    by_wire: HashMap<String, Entry>,
    encoders: HashMap<TypeId, (String, EncodeFn)>,
    decodes_to: HashSet<TypeId>,
}

impl fmt::Debug for EventTypeRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut wires: Vec<_> = self.by_wire.keys().collect();
        wires.sort();
        f.debug_struct("EventTypeRegistry")
            .field("wire_types", &wires)
            .field("encodable", &self.encoders.len())
            .field("decoded_types", &self.decodes_to.len())
            .finish()
    }
}

impl EventTypeRegistry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `T` under `wire_type` with codec `C` (both directions).
    ///
    /// # Errors
    /// [`Error::Config`] when `wire_type` is blank or already registered, or `T` is already
    /// registered under another wire type.
    pub fn register<T, C>(self, wire_type: &str) -> Result<Self>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
        C: PayloadCodec,
    {
        self.register_with::<T>(
            wire_type,
            |event| C::encode(event).map_err(BoxError::from),
            |body| C::decode(body).map_err(BoxError::from),
        )
    }

    /// [`register`](Self::register) with the [`Json`] codec.
    ///
    /// # Errors
    /// As [`register`](Self::register).
    pub fn register_json<T>(self, wire_type: &str) -> Result<Self>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        self.register::<T, Json>(wire_type)
    }

    /// Registers `T` with a hand-written serializer pair, for formats without a
    /// [`PayloadCodec`].
    ///
    /// # Errors
    /// As [`register`](Self::register).
    pub fn register_with<T>(
        mut self,
        wire_type: &str,
        encode: impl Fn(&T) -> Result<Bytes, BoxError> + Send + Sync + 'static,
        decode: impl Fn(&[u8]) -> Result<T, BoxError> + Send + Sync + 'static,
    ) -> Result<Self>
    where
        T: Send + Sync + 'static,
    {
        self.check_wire(wire_type)?;
        let type_id = TypeId::of::<T>();
        if let Some((existing, _)) = self.encoders.get(&type_id) {
            return Err(Error::Config(format!(
                "Event type '{}' is already registered under wire type '{existing}'; \
                 it cannot also be registered under '{wire_type}'.",
                std::any::type_name::<T>()
            )));
        }
        self.encoders.insert(
            type_id,
            (
                wire_type.to_owned(),
                Box::new(move |event| {
                    let event = event
                        .downcast_ref::<T>()
                        .ok_or("event is not the registered type")?;
                    encode(event)
                }),
            ),
        );
        self.insert(
            wire_type,
            type_id,
            Box::new(move |_, body| Ok(Some(Box::new(decode(body)?)))),
        );
        Ok(self)
    }

    /// Registers several wire types that all decode to one Rust type `E`, typically an enum over a
    /// topic's events, with a decoder that switches on the wire type. Decode-only: use the enum's
    /// own encoder to publish.
    ///
    /// `decode` returns `Ok(None)` for a wire type it does not know, which the projector treats
    /// as an unknown type (skipped). This is the fit for a domain crate that already owns
    /// `fn decode(wire_type, body) -> Result<Option<E>, _>`.
    ///
    /// # Errors
    /// [`Error::Config`] when any wire type is blank or already registered.
    pub fn register_family<E, Er>(
        mut self,
        wire_types: &[&str],
        decode: impl Fn(&str, &[u8]) -> Result<Option<E>, Er> + Send + Sync + 'static,
    ) -> Result<Self>
    where
        E: Send + Sync + 'static,
        Er: Into<BoxError>,
    {
        let decode = std::sync::Arc::new(decode);
        for wire_type in wire_types {
            self.check_wire(wire_type)?;
            let decode = std::sync::Arc::clone(&decode);
            self.insert(
                wire_type,
                TypeId::of::<E>(),
                Box::new(move |wire, body| {
                    Ok(decode(wire, body)
                        .map_err(Into::into)?
                        .map(|e| Box::new(e) as AnyBox))
                }),
            );
        }
        Ok(self)
    }

    fn check_wire(&self, wire_type: &str) -> Result<()> {
        if wire_type.trim().is_empty() {
            return Err(Error::Config("A wire type must not be blank.".to_owned()));
        }
        if self.by_wire.contains_key(wire_type) {
            return Err(Error::Config(format!(
                "Wire type '{wire_type}' is already registered with the EventTypeRegistry."
            )));
        }
        Ok(())
    }

    fn insert(&mut self, wire_type: &str, type_id: TypeId, decode: DecodeFn) {
        self.decodes_to.insert(type_id);
        self.by_wire
            .insert(wire_type.to_owned(), Entry { type_id, decode });
    }

    /// Encodes a registered event as `(wire type, body)`.
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] when `T` was not registered with an encoder;
    /// [`Error::Transport`] when the codec fails.
    pub fn encode<T: Send + Sync + 'static>(&self, event: &T) -> Result<(&str, Bytes)> {
        let (wire, encode) = self.encoders.get(&TypeId::of::<T>()).ok_or_else(|| {
            Error::invalid_argument(format!(
                "Event type '{}' is not registered with the EventTypeRegistry. \
                 Register it with 'register' or 'register_json' before raising it.",
                std::any::type_name::<T>()
            ))
        })?;
        let body = encode(event).map_err(|e| Error::transport_with("event encode failed", e))?;
        Ok((wire, body))
    }

    /// The wire type `T` is registered under for encoding.
    #[must_use]
    pub fn wire_type_of<T: 'static>(&self) -> Option<&str> {
        self.encoders
            .get(&TypeId::of::<T>())
            .map(|(wire, _)| wire.as_str())
    }

    /// True when `wire_type` is registered.
    #[must_use]
    pub fn contains(&self, wire_type: &str) -> bool {
        self.by_wire.contains_key(wire_type)
    }

    /// True when some wire type decodes to `T`.
    #[must_use]
    pub fn decodes_to<T: 'static>(&self) -> bool {
        self.decodes_to.contains(&TypeId::of::<T>())
    }

    /// Decodes a body. `Ok(None)` is an unknown wire type.
    ///
    /// # Errors
    /// The body does not decode as the registered type: the entry is malformed.
    pub fn try_decode(&self, wire_type: &str, body: &[u8]) -> Result<Option<Decoded>, BoxError> {
        let Some(entry) = self.by_wire.get(wire_type) else {
            return Ok(None);
        };
        Ok((entry.decode)(wire_type, body)?.map(|value| Decoded {
            type_id: entry.type_id,
            value,
        }))
    }

    /// [`try_decode`](Self::try_decode) straight to a `T`; `Ok(None)` when the wire type is unknown
    /// or decodes to something other than `T`.
    ///
    /// # Errors
    /// As [`try_decode`](Self::try_decode).
    pub fn decode<T: 'static>(&self, wire_type: &str, body: &[u8]) -> Result<Option<T>, BoxError> {
        Ok(self
            .try_decode(wire_type, body)?
            .and_then(|d| d.downcast::<T>().ok()))
    }
}
