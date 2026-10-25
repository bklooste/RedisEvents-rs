//! W3C `traceparent` parsing, formatting and the per-batch parent-versus-links decision.
//!
//! Port of the context rules in C# `StreamActivity` (`StartProcess`, `SharesOneTrace`,
//! `TryParseTraceParent`) and of `ActivityContext.TryParse`. Parsing is strict W3C: lowercase hex,
//! version `00` exactly 55 bytes, no all-zero trace or span id. A malformed value is never an error:
//! it simply yields no context (diagnostics must not drop data).

use crate::wire::StreamMsg;

/// Most trace links built for one batch (C# `StreamActivity.MaxTraceLinks`).
pub(crate) const MAX_TRACE_LINKS: usize = 16;

/// Offset of the 32-hex trace id within `00-<trace>-<span>-<flags>`.
const TRACE_ID_START: usize = 3;
/// End (exclusive) of the trace id.
const TRACE_ID_END: usize = 35;
/// Length of a version-00 `traceparent`.
const TRACE_PARENT_LEN: usize = 55;

/// A parsed W3C trace context: the remote span a producer published under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TraceParent {
    /// 16-byte trace id.
    pub(crate) trace_id: [u8; 16],
    /// 8-byte span id.
    pub(crate) span_id: [u8; 8],
    /// The trace-flags byte (bit 0 = sampled).
    pub(crate) flags: u8,
}

impl TraceParent {
    /// Parses a `traceparent` value. `None` for anything not strictly valid.
    pub(crate) fn parse(value: &str) -> Option<Self> {
        let b = value.as_bytes();
        if b.len() < TRACE_PARENT_LEN || (b.len() > TRACE_PARENT_LEN && b[TRACE_PARENT_LEN] != b'-')
        {
            return None;
        }
        if b[2] != b'-' || b[35] != b'-' || b[52] != b'-' {
            return None;
        }
        let version = hex_bytes::<1>(&b[0..2])?[0];
        if version == 0xff || (version == 0 && b.len() != TRACE_PARENT_LEN) {
            return None;
        }
        let trace_id = hex_bytes::<16>(&b[3..35])?;
        let span_id = hex_bytes::<8>(&b[36..52])?;
        if trace_id.iter().all(|&x| x == 0) || span_id.iter().all(|&x| x == 0) {
            return None;
        }
        let flags = hex_bytes::<1>(&b[53..55])?[0];
        Some(Self {
            trace_id,
            span_id,
            flags,
        })
    }

    /// Builds a version-00 context from its parts.
    pub(crate) const fn new(trace_id: [u8; 16], span_id: [u8; 8], flags: u8) -> Self {
        Self {
            trace_id,
            span_id,
            flags,
        }
    }

    /// Formats as `00-<trace>-<span>-<flags>` (lowercase hex, 55 bytes).
    pub(crate) fn format(&self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(TRACE_PARENT_LEN);
        out.push_str("00-");
        for byte in self.trace_id {
            out.push(char::from(HEX[usize::from(byte >> 4)]));
            out.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        out.push('-');
        for byte in self.span_id {
            out.push(char::from(HEX[usize::from(byte >> 4)]));
            out.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        out.push('-');
        out.push(char::from(HEX[usize::from(self.flags >> 4)]));
        out.push(char::from(HEX[usize::from(self.flags & 0x0f)]));
        out
    }
}

/// `None` for an absent or empty value, otherwise [`TraceParent::parse`].
pub(crate) fn parse_optional(value: Option<&str>) -> Option<TraceParent> {
    value.and_then(TraceParent::parse)
}

/// The consumer span's context for one batch: the parent it hangs off, and the producer contexts it
/// links to. C# `StreamActivity.StartProcess` decides both.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct BatchTrace {
    /// The remote parent, when the batch can be parented without lying.
    pub(crate) parent: Option<TraceParent>,
    /// Links to producer spans (capped at [`MAX_TRACE_LINKS`]).
    pub(crate) links: Vec<TraceParent>,
}

/// Decides parent versus links for a batch.
///
/// - One entry: parent off it, no links.
/// - Several entries that all share one trace id: parent off the first usable one, and link the
///   other producer spans within that trace (skipping a sibling with the parent's span id).
/// - Otherwise (mixed traces, malformed values, or no context at all): no parent, link every usable
///   entry.
pub(crate) fn plan_batch(batch: &[StreamMsg]) -> BatchTrace {
    let mut plan = BatchTrace::default();

    if let [only] = batch {
        plan.parent = parse_optional(only.trace_parent());
        return plan;
    }

    let limit = batch.len().min(MAX_TRACE_LINKS);

    match shared_trace_index(batch)
        .and_then(|first| parse_optional(batch[first].trace_parent()).map(|shared| (first, shared)))
    {
        Some((first, shared)) => {
            plan.parent = Some(shared);
            for (i, msg) in batch.iter().enumerate().take(limit) {
                if i == first {
                    continue;
                }
                if let Some(sibling) = parse_optional(msg.trace_parent())
                    && sibling.span_id != shared.span_id
                {
                    plan.links.push(sibling);
                }
            }
        }
        None => {
            plan.links.extend(
                batch
                    .iter()
                    .take(limit)
                    .filter_map(|msg| parse_optional(msg.trace_parent())),
            );
        }
    }

    plan
}

/// Index of the first entry with a trace id, provided every entry that carries one carries the same
/// id (compared case-insensitively). `None` on a disagreement, a malformed (too short) value, or no
/// trace context at all. The whole batch is scanned, not just the link cap.
fn shared_trace_index(batch: &[StreamMsg]) -> Option<usize> {
    let mut first: Option<(usize, &[u8])> = None;

    for (i, msg) in batch.iter().enumerate() {
        let Some(value) = msg.trace_parent().filter(|v| !v.is_empty()) else {
            continue;
        };
        // Too short to hold a trace id: treated as a disagreement, not ignored.
        let id = value.as_bytes().get(TRACE_ID_START..TRACE_ID_END)?;

        match first {
            None => first = Some((i, id)),
            Some((_, incumbent)) if !id.eq_ignore_ascii_case(incumbent) => return None,
            Some(_) => {}
        }
    }

    first.map(|(i, _)| i)
}

/// Decodes exactly `N` bytes from `2 * N` lowercase hex digits.
fn hex_bytes<const N: usize>(hex: &[u8]) -> Option<[u8; N]> {
    if hex.len() != 2 * N {
        return None;
    }
    let mut out = [0u8; N];
    let (pairs, _) = hex.as_chunks::<2>();
    for (byte, pair) in out.iter_mut().zip(pairs) {
        *byte = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Some(out)
}

const fn nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
    const SPAN: &str = "00f067aa0ba902b7";

    #[test]
    fn parses_and_formats_a_w3c_traceparent() {
        let value = format!("00-{TRACE}-{SPAN}-01");
        let parsed = TraceParent::parse(&value).expect("valid");
        assert_eq!(parsed.flags, 1);
        assert_eq!(parsed.format(), value);
    }

    #[test]
    fn rejects_what_w3c_rejects() {
        for bad in [
            "",
            "00-deadbeef",
            &format!("00-{}-{SPAN}-01", TRACE.to_uppercase()),
            &format!("00-{}-{SPAN}-01", "0".repeat(32)),
            &format!("00-{TRACE}-{}-01", "0".repeat(16)),
            &format!("ff-{TRACE}-{SPAN}-01"),
            &format!("00-{TRACE}-{SPAN}-01-extra"),
            &format!("00-{TRACE}-{SPAN}-0g"),
        ] {
            assert!(TraceParent::parse(bad).is_none(), "accepted {bad:?}");
        }
    }

    #[test]
    fn a_future_version_may_carry_trailing_fields() {
        let value = format!("01-{TRACE}-{SPAN}-01-extra");
        assert!(TraceParent::parse(&value).is_some());
    }

    #[test]
    fn absent_and_empty_values_yield_no_context() {
        assert_eq!(parse_optional(None), None);
        assert_eq!(parse_optional(Some("")), None);
    }

    // Ported from C# ConsumerSpanParentingTests (the parent-versus-links decision; the span
    // attributes themselves are not inspectable in the tracing form).

    use bytes::Bytes;

    use crate::wire::StreamId;

    fn entry(index: u64, trace_parent: Option<String>) -> StreamMsg {
        StreamMsg::new(StreamId::new(1, index), 0, "MBet", Bytes::new())
            .with_trace_parent(trace_parent)
    }

    fn producer(trace: u8, span: u8) -> TraceParent {
        TraceParent::new([trace; 16], [span; 8], 1)
    }

    #[test]
    fn a_batch_sharing_one_trace_parents_off_it_so_the_trace_survives_the_hop() {
        let p = producer(0xaa, 0x01);
        let batch: Vec<_> = (0..3).map(|i| entry(i, Some(p.format()))).collect();
        let plan = plan_batch(&batch);
        assert_eq!(plan.parent.map(|x| x.trace_id), Some(p.trace_id));
        assert_eq!(plan.parent.map(|x| x.span_id), Some(p.span_id));
    }

    #[test]
    fn a_batch_mixing_traces_still_adopts_no_parent() {
        let first = producer(0xaa, 0x01);
        let second = producer(0xbb, 0x02);
        let batch = [
            entry(0, Some(first.format())),
            entry(1, Some(second.format())),
        ];
        let plan = plan_batch(&batch);
        assert_eq!(
            plan.parent, None,
            "one batch carrying two traces has no single parent"
        );
        assert_eq!(
            plan.links,
            vec![first, second],
            "both producers must stay reachable"
        );
    }

    #[test]
    fn a_shared_trace_still_links_the_other_producer_spans_within_it() {
        let parent = producer(0xaa, 0x01);
        let sibling = TraceParent::new([0xaa; 16], [0x02; 8], 1);
        let batch = [
            entry(0, Some(parent.format())),
            entry(1, Some(sibling.format())),
        ];
        let plan = plan_batch(&batch);
        assert_eq!(
            plan.parent,
            Some(parent),
            "the first usable context is the parent"
        );
        assert_eq!(plan.links, vec![sibling]);
    }

    #[test]
    fn a_late_disagreement_is_not_missed_by_scanning_only_the_link_cap() {
        let bulk = producer(0xaa, 0x01);
        let odd = producer(0xbb, 0x02);
        let mut batch: Vec<StreamMsg> = (0..MAX_TRACE_LINKS as u64 + 4)
            .map(|i| entry(i, Some(bulk.format())))
            .collect();
        let last = batch.len() - 1;
        batch[last] = entry(last as u64, Some(odd.format()));
        let plan = plan_batch(&batch);
        assert_eq!(
            plan.parent, None,
            "the batch spans two traces, however far in the disagreement is"
        );
    }

    #[test]
    fn entries_without_a_traceparent_do_not_block_parenting() {
        let producer = producer(0xaa, 0x01);
        let batch = [
            entry(0, None),
            entry(1, Some(producer.format())),
            entry(2, Some(String::new())),
        ];
        let plan = plan_batch(&batch);
        assert_eq!(plan.parent.map(|x| x.trace_id), Some(producer.trace_id));
    }

    #[test]
    fn a_batch_with_no_trace_context_at_all_is_a_root_with_no_links() {
        let batch = [entry(0, None), entry(1, None)];
        let plan = plan_batch(&batch);
        assert_eq!(plan, BatchTrace::default());
    }

    #[test]
    fn a_malformed_traceparent_is_treated_as_a_disagreement_not_ignored() {
        let producer = producer(0xaa, 0x01);
        let batch = [
            entry(0, Some(producer.format())),
            entry(1, Some("00-deadbeef".to_owned())),
        ];
        let plan = plan_batch(&batch);
        assert_eq!(
            plan.parent, None,
            "guessing in favour of a parent is the unsafe guess"
        );
    }

    #[test]
    fn a_single_message_batch_parents_and_carries_no_links() {
        let producer = producer(0xaa, 0x01);
        let plan = plan_batch(&[entry(0, Some(producer.format()))]);
        assert_eq!(plan.parent, Some(producer));
        assert_eq!(plan.links.len(), 0);
    }
}
