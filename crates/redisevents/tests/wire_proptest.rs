//! Property tests for the A1 wire layer: header blocks and stream ids round-trip, and reading
//! arbitrary bytes as a header block never panics.

use bytes::Bytes;
use proptest::prelude::*;
use redisevents::error::Error;
use redisevents::wire::{HeaderBlock, StreamId};

fn header_list() -> impl Strategy<Value = Vec<(String, String)>> {
    prop::collection::vec((".{0,40}", ".{0,40}"), 0..=70)
}

proptest! {
    #[test]
    fn headers_round_trip_or_are_refused_over_the_caps(headers in header_list()) {
        let within_caps = headers.len() <= HeaderBlock::MAX_COUNT && packed_len(&headers) <= HeaderBlock::MAX_BYTES;
        match HeaderBlock::pack(&headers) {
            Ok(block) => {
                prop_assert!(within_caps, "pack accepted a block over a cap");
                prop_assert!(block.packed().len() <= HeaderBlock::MAX_BYTES);
                let read = HeaderBlock::from_packed(block.packed().clone());
                prop_assert_eq!(read.to_vec().expect("packed by pack"), headers.clone());
                prop_assert_eq!(read.count().expect("packed by pack"), headers.len());
                for (key, _) in &headers {
                    let first = headers.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone());
                    let got = read.get(key).expect("valid").map(std::borrow::Cow::into_owned);
                    prop_assert_eq!(got, first);
                }
            }
            Err(Error::InvalidArgument(_)) => prop_assert!(!within_caps, "pack refused a block within the caps"),
            Err(other) => prop_assert!(false, "unexpected error {other:?}"),
        }
    }

    #[test]
    fn arbitrary_bytes_never_panic_when_read(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        let block = HeaderBlock::from_packed(Bytes::from(bytes));
        // Each outcome is fine; the property is that reading terminates without panicking, and an
        // error ends the iteration.
        let items: Vec<_> = block.iter().collect();
        if items.iter().any(Result::is_err) {
            prop_assert!(matches!(items.last(), Some(Err(_))));
        }
        let _ = block.count();
        let _ = block.get("k");
    }

    #[test]
    fn stream_id_format_parse_round_trips(ms in 0..=StreamId::MAX_PART, seq in 0..=StreamId::MAX_PART) {
        let id = StreamId::new(ms, seq);
        let text = id.to_string();
        prop_assert_eq!(StreamId::parse(&text).expect("valid"), id);
        prop_assert_eq!(StreamId::parse_bytes(text.as_bytes()).expect("valid"), id);
        let mut buf = [0u8; 41];
        prop_assert_eq!(id.format_into(&mut buf), text.as_str());
    }

    #[test]
    fn stream_id_parts_above_i64_max_are_rejected(
        big in (StreamId::MAX_PART + 1)..=u64::MAX,
        small in 0..=StreamId::MAX_PART,
    ) {
        let high_ms = format!("{big}-{small}");
        let high_seq = format!("{small}-{big}");
        prop_assert!(StreamId::parse(&high_ms).is_err());
        prop_assert!(StreamId::parse(&high_seq).is_err());
    }

    #[test]
    fn stream_id_order_matches_redis_order(a in (0..=StreamId::MAX_PART, 0..=StreamId::MAX_PART), b in (0..=StreamId::MAX_PART, 0..=StreamId::MAX_PART)) {
        let (x, y) = (StreamId::new(a.0, a.1), StreamId::new(b.0, b.1));
        prop_assert_eq!(x.cmp(&y), (a.0, a.1).cmp(&(b.0, b.1)));
    }
}

/// The packed size of `headers` by the wire rule (`len ':' bytes` per field), independent of `pack`.
fn packed_len(headers: &[(String, String)]) -> usize {
    headers
        .iter()
        .flat_map(|(k, v)| [k.len(), v.len()])
        .map(|len| len.to_string().len() + 1 + len)
        .sum()
}
