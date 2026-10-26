//! Validation and startup advice for [`StreamOptions`]. Error messages name the offending field by
//! its path in the configuration (`topics.orders.max_len`, `consumers[0].batch_size`), which is
//! the same in every serde format.

use std::collections::HashMap;

use tracing::Level;

use super::{
    BackpressureOptions, ConsumerOptions, InstanceMode, InstanceOptions, PersistMode, ReadMode,
    StartFrom, StreamOptions, TopicOptions, TrimMode,
};
use crate::error::{Error, Result};

/// One advisory line: informational, or a warning about a configuration that will work but is
/// almost certainly not what the operator wanted.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Advisory {
    /// [`Level::WARN`] or [`Level::INFO`].
    pub level: Level,
    /// The log line, naming the fields involved.
    pub message: String,
}

impl Advisory {
    /// Emits the line through `tracing`, at its level.
    pub fn log(&self) {
        if self.level == Level::WARN {
            tracing::warn!("{}", self.message);
        } else {
            tracing::info!("{}", self.message);
        }
    }
}

pub(super) fn validate(options: &StreamOptions) -> Result<()> {
    validate_topics(options)?;
    validate_consumers(options)?;
    validate_producers(options)
}

fn validate_topics(options: &StreamOptions) -> Result<()> {
    let mut names: Vec<&String> = options.topics.keys().collect();
    names.sort();

    for name in names {
        let topic = &options.topics[name];
        let key = format!("topics.{name}");

        if topic.partitions < 1 {
            return Err(Error::config(format!(
                "{key}.partitions is {}; it must be >= 1.",
                topic.partitions
            )));
        }

        if let Some(retention) = topic.retention_seconds {
            if retention < 1 {
                return Err(Error::config(format!(
                    "{key}.retention_seconds is {retention}; it must be >= 1 or omitted."
                )));
            }

            // Time-based trimming does not bound memory, so a ceiling is mandatory. An omitted
            // max_len is distinguishable from a configured one, which is what makes this rule
            // reachable at all.
            match topic.max_len {
                Some(0) => {
                    return Err(Error::config(format!(
                        "{key}.retention_seconds is set ({retention}) but {key}.max_len is 0. \
                         Time-based trimming does not bound memory, so a max_len ceiling (>= 1) is mandatory."
                    )));
                }
                None => {
                    return Err(Error::config(format!(
                        "{key}.retention_seconds is set ({retention}s) but {key}.max_len is not configured. \
                         Time-based trimming does not bound memory, so the ceiling is what actually protects Redis, \
                         and the default of {} entries per partition is almost certainly not the ceiling a \
                         {retention}s window needs. Set {key}.max_len explicitly (entries per partition), or remove \
                         {key}.retention_seconds and let the inline MAXLEN window do the trimming.",
                        TopicOptions::DEFAULT_MAX_LEN
                    )));
                }
                Some(_) => {}
            }
        }

        if topic.trim == TrimMode::None && !options.allow_untrimmed {
            return Err(Error::config(format!(
                "{key}.trim is none: an untrimmed stream grows until Redis runs out of memory. \
                 Set allow_untrimmed = true to permit it (local development, benchmarks)."
            )));
        }
    }
    Ok(())
}

fn validate_consumers(options: &StreamOptions) -> Result<()> {
    let mut seen: HashMap<(&str, String), usize> = HashMap::new();

    for (i, consumer) in options.consumers.iter().enumerate() {
        let key = format!("consumers[{i}]");

        if consumer.topic.trim().is_empty() {
            return Err(Error::config(format!(
                "{key}.topic is missing; every consumer must name a topic."
            )));
        }
        if consumer.batch_size < 1 {
            return Err(Error::config(format!(
                "{key}.batch_size is {}; it must be >= 1.",
                consumer.batch_size
            )));
        }
        validate_backpressure(&consumer.backpressure, &key)?;

        if consumer.start_from == StartFrom::Date && consumer.start_from_date.is_none() {
            return Err(Error::config(format!(
                "{key}.start_from is date but {key}.start_from_date is not set."
            )));
        }

        if consumer.persist == PersistMode::None && consumer.start_from == StartFrom::Stored {
            return Err(Error::config(format!(
                "{key}.persist is none but {key}.start_from is stored: a contradiction, because nothing is ever stored. \
                 Use start_from now, beginning or date."
            )));
        }

        // Nothing in the consumer host applies a start date as a reset first. Accepting the key
        // would mean a redeploy that was meant to replay quietly not replaying.
        if consumer.reset_on_start {
            return Err(Error::config(format!(
                "{key}.reset_on_start is true, but nothing in the consumer host reads it: the positions would NOT be reset and \
                 the consumer would resume where it left off, silently. Remove the field and perform the replay explicitly: \
                 StreamAdmin::reset_position with the consumer stopped, then start it."
            )));
        }

        validate_instances(consumer.instances.as_ref(), &key)?;

        let name = consumer_name(options, consumer);
        if let Some(&first) = seen.get(&(consumer.topic.as_str(), name.clone())) {
            return Err(Error::config(format!(
                "{key} duplicates consumers[{first}]: both consume topic '{}' as consumer '{name}'. \
                 Two consumers on the same (topic, consumer) pair in one process would fight over the same position hash. \
                 Set a distinct {key}.consumer.",
                consumer.topic
            )));
        }
        seen.insert((consumer.topic.as_str(), name), i);
    }
    Ok(())
}

fn validate_backpressure(backpressure: &BackpressureOptions, key: &str) -> Result<()> {
    if backpressure.capacity < 1 {
        return Err(Error::config(format!(
            "{key}.backpressure.capacity is {}; it must be >= 1.",
            backpressure.capacity
        )));
    }
    Ok(())
}

fn validate_instances(instances: Option<&InstanceOptions>, consumer_key: &str) -> Result<()> {
    let Some(instances) = instances else {
        return Ok(());
    };
    let key = format!("{consumer_key}.instances");
    let ttl = instances.lease_ttl_seconds;
    let renew = instances.lease_renew_seconds;

    if ttl < 1 {
        return Err(Error::config(format!(
            "{key}.lease_ttl_seconds is {ttl}; it must be at least 1 second."
        )));
    }
    if renew < 1 {
        return Err(Error::config(format!(
            "{key}.lease_renew_seconds is {renew}; it must be at least 1 second."
        )));
    }
    if renew >= ttl {
        return Err(Error::config(format!(
            "{key}.lease_renew_seconds is {renew}s against {key}.lease_ttl_seconds of {ttl}s. \
             The renew interval must be shorter than the TTL, comfortably shorter, since one missed renewal must not cost the \
             claim, or every ownership claim expires between renewals. The defaults are {}s and {}s.",
            InstanceOptions::DEFAULT_LEASE_TTL_SECONDS,
            InstanceOptions::DEFAULT_LEASE_RENEW_SECONDS
        )));
    }

    // Lease reads neither count nor index, so declaring them has no effect: refused rather than
    // ignored.
    if instances.mode == InstanceMode::Lease
        && (instances.count.is_some() || instances.index.is_some())
    {
        return Err(Error::config(format!(
            "{key}.mode is lease (the default), but {key}.count / {key}.index are set and lease mode reads neither: \
             it claims free partitions and counts the live instances out of the ownership hash. \
             Remove them, or set {key}.mode = static to keep the fixed assignment."
        )));
    }

    if instances.count == Some(0) {
        return Err(Error::config(format!("{key}.count is 0; it must be >= 1.")));
    }
    if let (Some(count), Some(index)) = (instances.count, instances.index)
        && index >= count
    {
        return Err(Error::config(format!(
            "{key}.index is {index} but {key}.count is {count}; index must be less than count (indexes are zero-based)."
        )));
    }
    Ok(())
}

fn validate_producers(options: &StreamOptions) -> Result<()> {
    for (i, producer) in options.producers.iter().enumerate() {
        let key = format!("producers[{i}]");

        if producer.topic.trim().is_empty() {
            return Err(Error::config(format!(
                "{key}.topic is missing; every producer must name a topic."
            )));
        }
        if producer.max_batch < 1 {
            return Err(Error::config(format!(
                "{key}.max_batch is {}; it must be >= 1.",
                producer.max_batch
            )));
        }
        if producer.max_queue < 1 {
            return Err(Error::config(format!(
                "{key}.max_queue is {}; it must be >= 1.",
                producer.max_queue
            )));
        }
    }
    Ok(())
}

pub(super) fn advise(options: &StreamOptions) -> Vec<Advisory> {
    let mut out = Vec::new();

    for (i, consumer) in options.consumers.iter().enumerate() {
        let topic = topic_with_advice(options, &consumer.topic, &mut out);
        if consumer.read_mode == ReadMode::Block
            && !topic.co_locate_partitions
            && topic.partitions > 2
        {
            out.push(Advisory {
                level: Level::WARN,
                message: format!(
                    "Streams: consumers[{i}].read_mode is block and topics.{}.co_locate_partitions is false with {} partitions. \
                     Each partition needs its own blocking reader connection, so this consumer will open {} dedicated Redis connections. \
                     Set co_locate_partitions to true to use a single multi-stream XREAD.",
                    consumer.topic, topic.partitions, topic.partitions
                ),
            });
        }
    }

    for producer in &options.producers {
        let _ = topic_with_advice(options, &producer.topic, &mut out);
    }

    out
}

fn topic_with_advice(
    options: &StreamOptions,
    topic: &str,
    out: &mut Vec<Advisory>,
) -> TopicOptions {
    if let Some(configured) = options.topics.get(topic) {
        return configured.clone();
    }
    let defaulted = TopicOptions::default();
    out.push(Advisory {
        level: Level::INFO,
        message: format!(
            "Streams: topic {topic} is not configured under topics; using defaults (partitions={}, max_len={}, trim={:?}).",
            defaulted.partitions,
            defaulted.max_len(),
            defaulted.trim
        ),
    });
    defaulted
}

pub(super) fn consumer_name(options: &StreamOptions, consumer: &ConsumerOptions) -> String {
    non_blank(consumer.consumer.as_deref())
        .or_else(|| non_blank(options.consumer.as_deref()))
        .map_or_else(default_consumer_name, str::to_owned)
}

fn default_consumer_name() -> String {
    std::env::current_exe()
        .ok()
        .and_then(|path| path.file_stem().map(|s| s.to_string_lossy().into_owned()))
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

pub(super) fn non_blank(value: Option<&str>) -> Option<&str> {
    value.filter(|v| !v.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DeliveryMode, ErrorPolicy, ProducerOptions};
    use time::macros::datetime;

    /// Every key in the section, each set to something that is not its default.
    const FULL_SECTION: &str = r#"{
      "streams": {
        "redis_url": "redis://localhost:6380/0",
        "consumer": "svc-wide-name",
        "topics": {
          "orders": {
            "partitions": 4,
            "max_len": 250000,
            "trim": "exact",
            "retention_seconds": 3600,
            "background_trim_interval_seconds": 60,
            "co_locate_partitions": false,
            "clamp_release_threshold": 0.5,
            "state_metadata": true
          }
        },
        "consumers": [
          {
            "topic": "orders",
            "consumer": "orders-reader",
            "batch_size": 250,
            "filter": [ "OrderPlaced", "OrderCancelled" ],
            "start_from": "date",
            "start_from_date": "2026-01-02T03:04:05+00:00",
            "persist": "sync_batch",
            "persist_interval_ms": 250,
            "backpressure": { "enabled": false, "capacity": 9 },
            "read_mode": "poll",
            "block_ms": 750,
            "max_idle_delay_ms": 25,
            "on_error": "stop_partition",
            "unhealthy_block_seconds": 90,
            "instances": { "mode": "static", "count": 3, "index": 1 },
            "reader_threads": 2,
            "shutdown_timeout_seconds": 30,
            "unhealthy_lag_ms": 45000,
            "unhealthy_behind_seconds": 240,
            "degraded_silence_seconds": 900,
            "start_from_when_missing": "now"
          }
        ],
        "producers": [
          {
            "topic": "orders",
            "buffered": true,
            "max_wait_ms": 5,
            "max_batch": 1000,
            "max_queue": 250,
            "drop_oldest": true
          }
        ]
      }
    }"#;

    /// A document with the options under a `streams` key, as they would sit in a larger
    /// application config file.
    #[derive(serde::Deserialize)]
    struct Root {
        #[serde(default)]
        streams: StreamOptions,
    }

    fn bind_json(json: &str) -> serde_json::Result<StreamOptions> {
        serde_json::from_str::<Root>(json).map(|root| root.streams)
    }

    fn bind(json: &str) -> StreamOptions {
        bind_json(json).expect("bind")
    }

    fn throws(json: &str) -> String {
        let options = bind_json(json).expect("binding should succeed; the validation should fail");
        validate(&options)
            .expect_err("validation should fail")
            .to_string()
    }

    fn with_topic(name: &str, topic: TopicOptions) -> StreamOptions {
        StreamOptions {
            topics: HashMap::from([(name.to_owned(), topic)]),
            ..StreamOptions::default()
        }
    }

    fn with_consumer(consumer: ConsumerOptions) -> StreamOptions {
        StreamOptions {
            consumer: Some("svc".to_owned()),
            consumers: vec![consumer],
            ..StreamOptions::default()
        }
    }

    fn with_producer(producer: ProducerOptions) -> StreamOptions {
        StreamOptions {
            producers: vec![producer],
            ..StreamOptions::default()
        }
    }

    fn validate_err(options: &StreamOptions) -> String {
        validate(options)
            .expect_err("validation should fail")
            .to_string()
    }

    // ------------------------------------------------------------ binding actually binds

    #[test]
    fn bind_root_keys_are_assigned() {
        let options = bind(FULL_SECTION);

        assert_eq!(
            options.redis_url.as_deref(),
            Some("redis://localhost:6380/0")
        );
        assert_eq!(options.consumer.as_deref(), Some("svc-wide-name"));
        assert!(options.topics.contains_key("orders"));
        assert_eq!(options.consumers.len(), 1);
        assert_eq!(options.producers.len(), 1);
    }

    #[test]
    fn bind_topic_keys_are_assigned() {
        let topic = &bind(FULL_SECTION).topics["orders"];

        assert_eq!(topic.partitions, 4);
        assert_eq!(topic.max_len, Some(250_000));
        assert_eq!(topic.trim, TrimMode::Exact);
        assert_eq!(topic.retention_seconds, Some(3600));
        assert_eq!(topic.background_trim_interval_seconds, 60);
        assert!(!topic.co_locate_partitions);
        assert!((topic.clamp_release_threshold - 0.5).abs() < f64::EPSILON);
        assert!(topic.state_metadata);
    }

    #[test]
    fn bind_consumer_keys_are_assigned() {
        let consumer = &bind(FULL_SECTION).consumers[0];

        assert_eq!(consumer.topic, "orders");
        assert_eq!(consumer.consumer.as_deref(), Some("orders-reader"));
        assert_eq!(consumer.batch_size, 250);
        assert_eq!(
            consumer.filter.as_deref(),
            Some(&["OrderPlaced".to_owned(), "OrderCancelled".to_owned()][..])
        );
        assert_eq!(consumer.start_from, StartFrom::Date);
        assert_eq!(
            consumer.start_from_date,
            Some(datetime!(2026-01-02 03:04:05 UTC))
        );
        assert_eq!(consumer.persist, PersistMode::SyncBatch);
        assert_eq!(consumer.persist_interval_ms, 250);
        assert!(!consumer.backpressure.enabled);
        assert_eq!(consumer.backpressure.capacity, 9);
        assert_eq!(consumer.read_mode, ReadMode::Poll);
        assert_eq!(consumer.block_ms, 750);
        assert_eq!(consumer.max_idle_delay_ms, 25);
        assert_eq!(consumer.on_error, ErrorPolicy::StopPartition);
        assert_eq!(consumer.unhealthy_block_seconds, 90);
        assert_eq!(consumer.reader_threads, 2);
        assert_eq!(consumer.shutdown_timeout_seconds, 30);
        assert_eq!(consumer.unhealthy_lag_ms, 45_000);
        assert_eq!(consumer.unhealthy_behind_seconds, 240);
        assert_eq!(consumer.degraded_silence_seconds, 900);
        assert_eq!(consumer.start_from_when_missing, StartFrom::Now);
        let instances = consumer.instances.as_ref().expect("instances");
        assert_eq!(instances.count, Some(3));
        assert_eq!(instances.index, Some(1));
    }

    #[test]
    fn bind_producer_keys_are_assigned() {
        let producer = &bind(FULL_SECTION).producers[0];

        assert_eq!(producer.topic, "orders");
        assert!(producer.buffered);
        assert_eq!(producer.max_wait_ms, 5);
        assert_eq!(producer.max_batch, 1000);
        assert_eq!(producer.max_queue, 250);
        assert!(producer.drop_oldest);
    }

    #[test]
    fn bind_full_section_validates_cleanly() {
        let options = bind(FULL_SECTION);

        validate(&options).expect("a fully populated section that binds must also be a legal one");
    }

    #[test]
    fn bind_keys_not_present_keep_their_defaults() {
        let options = bind(r#"{ "streams": { "topics": { "orders": { "partitions": 8 } } } }"#);

        let topic = &options.topics["orders"];
        assert_eq!(topic.partitions, 8);
        assert_eq!(
            topic.max_len(),
            TopicOptions::DEFAULT_MAX_LEN,
            "an absent key must not overwrite the default"
        );
        assert_eq!(topic.trim, TrimMode::Approx);
        assert!(topic.co_locate_partitions);
    }

    // ------------------------------------------------------------ MaxLen: configured vs defaulted

    #[test]
    fn bind_max_len_tracks_whether_it_was_configured() {
        let configured = &bind(
            r#"{ "streams": { "topics": { "orders": { "max_len": 10000 } } } }"#,
        )
        .topics["orders"];
        let defaulted = &bind(r#"{ "streams": { "topics": { "orders": { "partitions": 2 } } } }"#)
            .topics["orders"];

        assert_eq!(configured.max_len(), TopicOptions::DEFAULT_MAX_LEN);
        assert_eq!(defaulted.max_len(), TopicOptions::DEFAULT_MAX_LEN);

        assert!(
            configured.max_len_configured(),
            "the two are indistinguishable by value: an operator who writes the default explicitly has still chosen a ceiling"
        );
        assert!(!defaulted.max_len_configured());
    }

    #[test]
    fn validate_retention_without_max_len_throws_naming_both_keys() {
        let message =
            throws(r#"{ "streams": { "topics": { "orders": { "retention_seconds": 604800 } } } }"#);

        assert!(message.contains("topics.orders.retention_seconds"));
        assert!(message.contains("topics.orders.max_len"));
        assert!(message.contains("not configured"));
    }

    #[test]
    fn validate_retention_with_max_len_is_accepted() {
        let options = bind(
            r#"{ "streams": { "topics": { "orders": { "retention_seconds": 604800, "max_len": 5000000 } } } }"#,
        );

        validate(&options).expect("retention with a ceiling is legal");
    }

    // ------------------------------------------------------------ keys that bound and did nothing

    #[test]
    fn validate_lease_mode_is_accepted_now_that_the_host_implements_it() {
        let options = bind(
            r#"{ "streams": { "consumers": [ { "topic": "orders", "instances": { "mode": "lease" } } ] } }"#,
        );

        validate(&options).expect("Lease mode claims its partitions from the ownership registry");
    }

    #[test]
    fn validate_lease_mode_with_a_count_or_index_is_refused() {
        for keys in [
            r#""count": 2"#,
            r#""index": 0"#,
            r#""count": 2, "index": 1"#,
        ] {
            let message = throws(&format!(
                r#"{{ "streams": {{ "consumers": [ {{ "topic": "orders", "instances": {{ "mode": "lease", {keys} }} }} ] }} }}"#
            ));

            assert!(
                message.contains("consumers[0].instances.mode is lease"),
                "{message}"
            );
            assert!(
                message.contains("consumers[0].instances.count"),
                "{message}"
            );
            assert!(
                message.contains("consumers[0].instances.index"),
                "{message}"
            );
        }
    }

    #[test]
    fn bind_instances_with_no_mode_defaults_to_lease() {
        let options = bind(
            r#"{ "streams": { "consumers": [ { "topic": "orders", "instances": { "lease_ttl_seconds": 30 } } ] } }"#,
        );

        assert_eq!(
            options.consumers[0].instances.as_ref().map(|i| i.mode),
            Some(InstanceMode::Lease)
        );
        validate(&options).expect("Lease default validates clean");
    }

    #[test]
    fn validate_lease_timings_are_accepted_because_the_host_passes_them_through() {
        for keys in [
            r#""lease_ttl_seconds": 120, "lease_renew_seconds": 45"#,
            r#""lease_ttl_seconds": 120"#,
            r#""lease_renew_seconds": 5"#,
        ] {
            let options = bind(&format!(
                r#"{{ "streams": {{ "consumers": [ {{ "topic": "orders", "instances": {{ {keys} }} }} ] }} }}"#
            ));

            validate(&options).unwrap_or_else(|e| panic!("{keys}: {e}"));
        }
    }

    #[test]
    fn validate_lease_renew_at_or_past_the_ttl_is_refused() {
        for keys in [
            r#""lease_renew_seconds": 45"#,
            r#""lease_ttl_seconds": 10, "lease_renew_seconds": 10"#,
        ] {
            let message = throws(&format!(
                r#"{{ "streams": {{ "consumers": [ {{ "topic": "orders", "instances": {{ {keys} }} }} ] }} }}"#
            ));

            assert!(
                message.contains("consumers[0].instances.lease_renew_seconds"),
                "{message}"
            );
            assert!(
                message.contains("consumers[0].instances.lease_ttl_seconds"),
                "{message}"
            );
        }
    }

    #[test]
    fn validate_lease_timings_below_one_second_are_refused() {
        for (key, expected) in [
            ("lease_ttl_seconds", "lease_ttl_seconds"),
            ("lease_renew_seconds", "lease_renew_seconds"),
        ] {
            let message = throws(&format!(
                r#"{{ "streams": {{ "consumers": [ {{ "topic": "orders", "instances": {{ "{key}": 0 }} }} ] }} }}"#
            ));

            assert!(
                message.contains(&format!("consumers[0].instances.{expected}")),
                "{message}"
            );
            assert!(message.contains("at least 1 second"), "{message}");
        }
    }

    #[test]
    fn validate_lease_timings_at_their_defaults_are_accepted() {
        let options = bind(
            r#"{ "streams": { "consumers": [
              { "topic": "orders", "instances": { "mode": "static", "count": 2, "index": 0, "lease_ttl_seconds": 30, "lease_renew_seconds": 10 } }
            ] } }"#,
        );

        validate(&options).expect("30s/10s is what the ownership registry claims and renews on");
    }

    #[test]
    fn validate_reset_on_start_is_refused_because_nothing_reads_it() {
        let message = throws(
            r#"{ "streams": { "consumers": [ { "topic": "orders", "reset_on_start": true, "start_from": "date", "start_from_date": "2026-01-01T00:00:00Z" } ] } }"#,
        );

        assert!(message.contains("consumers[0].reset_on_start"));
        assert!(message.contains("StreamAdmin"), "{message}");
    }

    #[test]
    fn validate_reset_on_start_false_is_accepted() {
        let options = bind(
            r#"{ "streams": { "consumers": [ { "topic": "orders", "reset_on_start": false } ] } }"#,
        );

        validate(&options).expect("ResetOnStart false is legal");
    }

    // ------------------------------------------------------------ advisory rules

    #[test]
    fn validate_block_without_co_location_warns() {
        let options = bind(
            r#"{ "streams": {
              "topics": { "orders": { "partitions": 8, "co_locate_partitions": false } },
              "consumers": [ { "topic": "orders", "read_mode": "block" } ]
            } }"#,
        );

        let warnings: Vec<_> = advise(&options)
            .into_iter()
            .filter(|a| a.level == Level::WARN)
            .collect();
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].message.contains("dedicated Redis connections"));
    }

    #[test]
    fn validate_unconfigured_topic_logs_the_defaults_it_will_run_on() {
        let options = bind(r#"{ "streams": { "consumers": [ { "topic": "orders" } ] } }"#);

        let lines = advise(&options);
        assert!(
            lines
                .iter()
                .any(|a| a.level == Level::INFO
                    && a.message.contains("is not configured under topics")),
            "{lines:?}"
        );
    }

    // ------------------------------------------------------------ delivery

    #[test]
    fn delivery_defaults_to_ordered() {
        assert_eq!(
            ConsumerOptions::default().delivery,
            DeliveryMode::Ordered,
            "the ordered named-cursor model is the default; WorkQueue loses per-key order and must be asked for"
        );

        let options = bind(r#"{ "streams": { "consumers": [ { "topic": "orders" } ] } }"#);
        assert_eq!(options.consumers[0].delivery, DeliveryMode::Ordered);
    }

    #[test]
    fn bind_delivery_binds_work_queue() {
        let options = bind(
            r#"{ "streams": { "consumers": [ { "topic": "orders", "delivery": "work_queue" } ] } }"#,
        );

        assert_eq!(options.consumers[0].delivery, DeliveryMode::WorkQueue);
    }

    // ------------------------------------------------------------ binding and defaults

    #[test]
    fn bind_empty_streams_section_yields_all_defaults() {
        let options = bind(r#"{ "streams": { } }"#);

        assert_eq!(options.redis_url, None);
        assert_eq!(options.consumer, None);
        assert_eq!(options.topics.len(), 0);
        assert_eq!(options.consumers.len(), 0);
        assert_eq!(options.producers.len(), 0);
    }

    #[test]
    fn bind_empty_and_missing_section_validate_cleanly() {
        for json in [r#"{ "streams": { } }"#, "{ }"] {
            let options = bind(json);
            validate(&options).expect("zero configuration is legal");
        }
    }

    #[test]
    fn defaults_match_documented_values() {
        let topic = TopicOptions::default();
        assert_eq!(topic.partitions, 1);
        assert_eq!(topic.max_len(), 10_000);
        assert_eq!(topic.trim, TrimMode::Approx);
        assert_eq!(topic.retention_seconds, None);
        assert!(topic.co_locate_partitions);
        assert!((topic.clamp_release_threshold - 0.8).abs() < f64::EPSILON);

        let consumer = ConsumerOptions::default();
        assert_eq!(consumer.batch_size, 100);
        assert_eq!(consumer.start_from, StartFrom::Stored);
        assert_eq!(consumer.persist, PersistMode::AsyncBatch);
        assert_eq!(consumer.persist_interval_ms, 1000);
        assert_eq!(consumer.read_mode, ReadMode::Block);
        assert_eq!(consumer.block_ms, 1000);
        assert_eq!(consumer.on_error, ErrorPolicy::BestEffort);
        assert_eq!(consumer.instances, None);

        // Lease, not Static: the enum's zero value (Static) deliberately no longer expresses the default.
        let instances = InstanceOptions::default();
        assert_eq!(instances.mode, InstanceMode::Lease);
        assert_eq!(instances.count, None);
        assert_eq!(instances.index, None);
        assert_eq!(consumer.start_from_when_missing, StartFrom::Beginning);
        assert!(consumer.backpressure.enabled);
        assert_eq!(consumer.backpressure.capacity, 4);

        let producer = ProducerOptions::default();
        assert!(!producer.buffered);
        assert_eq!(producer.max_wait_ms, 20);
        assert_eq!(producer.max_batch, 500);
        assert_eq!(producer.max_queue, 100_000);
        assert!(!producer.drop_oldest);
    }

    #[test]
    fn resolve_topic_unconfigured_topic_returns_defaults_not_an_error() {
        assert_eq!(
            StreamOptions::default().topic("feed_source_racing"),
            TopicOptions::default()
        );
    }

    #[test]
    fn resolve_consumer_name_prefers_entry_then_root() {
        let options = StreamOptions {
            consumer: Some("root-consumer".to_owned()),
            ..StreamOptions::default()
        };

        let own = ConsumerOptions {
            topic: "t".to_owned(),
            consumer: Some("own".to_owned()),
            ..ConsumerOptions::default()
        };
        assert_eq!(options.consumer_name(&own), "own");

        let inherits = ConsumerOptions {
            topic: "t".to_owned(),
            ..ConsumerOptions::default()
        };
        assert_eq!(options.consumer_name(&inherits), "root-consumer");
        assert_ne!(StreamOptions::default().consumer_name(&inherits).trim(), "");
    }

    // ------------------------------------------------------------ topic rules

    #[test]
    fn validate_topic_partitions_below_one_throws_naming_key() {
        let message = validate_err(&with_topic(
            "orders",
            TopicOptions {
                partitions: 0,
                ..TopicOptions::default()
            },
        ));
        assert!(message.contains("topics.orders.partitions"), "{message}");
    }

    #[test]
    fn validate_topic_partitions_negative_is_a_deserialize_error() {
        let err = bind_json(r#"{ "streams": { "topics": { "orders": { "partitions": -1 } } } }"#);
        assert!(err.is_err(), "{err:?}");
    }

    #[test]
    fn validate_topic_retention_below_one_throws_naming_key() {
        let message = validate_err(&with_topic(
            "orders",
            TopicOptions {
                retention_seconds: Some(0),
                ..TopicOptions::default()
            },
        ));
        assert!(
            message.contains("topics.orders.retention_seconds"),
            "{message}"
        );
    }

    /// An explicit `MaxLen: 0` is a ceiling of zero, which is not a ceiling. The omitted-key case is
    /// covered by [`validate_retention_without_max_len_throws_naming_both_keys`].
    #[test]
    fn validate_topic_retention_with_max_len_zero_throws_naming_both_keys() {
        let message = validate_err(&with_topic(
            "orders",
            TopicOptions {
                retention_seconds: Some(60),
                max_len: Some(0),
                ..TopicOptions::default()
            },
        ));

        assert!(message.contains("topics.orders.retention_seconds"));
        assert!(message.contains("topics.orders.max_len"));
    }

    #[test]
    fn validate_topic_retention_with_no_max_len_at_all_throws_naming_both_keys() {
        let message = validate_err(&with_topic(
            "orders",
            TopicOptions {
                retention_seconds: Some(60),
                ..TopicOptions::default()
            },
        ));

        assert!(message.contains("topics.orders.retention_seconds"));
        assert!(message.contains("topics.orders.max_len"));
        assert!(message.contains("not configured"));
    }

    #[test]
    fn validate_trim_none_without_allow_untrimmed_throws_naming_both_fields() {
        let message = validate_err(&with_topic(
            "orders",
            TopicOptions {
                trim: TrimMode::None,
                ..TopicOptions::default()
            },
        ));

        assert!(message.contains("topics.orders.trim"));
        assert!(message.contains("allow_untrimmed"));
    }

    #[test]
    fn validate_trim_none_with_allow_untrimmed_is_allowed() {
        let mut options = with_topic(
            "orders",
            TopicOptions {
                trim: TrimMode::None,
                ..TopicOptions::default()
            },
        );

        options.allow_untrimmed = true;
        validate(&options).expect("trim = none is allowed once allow_untrimmed is set");
    }

    // ------------------------------------------------------------ consumer rules

    #[test]
    fn validate_consumer_without_topic_throws_naming_key() {
        for topic in ["", "   "] {
            let message = validate_err(&with_consumer(ConsumerOptions {
                topic: topic.to_owned(),
                ..ConsumerOptions::default()
            }));
            assert!(message.contains("consumers[0].topic"), "{message}");
        }
    }

    #[test]
    fn validate_consumer_batch_size_below_one_throws_naming_key() {
        let message = validate_err(&with_consumer(ConsumerOptions {
            topic: "orders".to_owned(),
            batch_size: 0,
            ..ConsumerOptions::default()
        }));
        assert!(message.contains("consumers[0].batch_size"), "{message}");
    }

    #[test]
    fn validate_consumer_batch_size_negative_is_a_deserialize_error() {
        let err = bind_json(
            r#"{ "streams": { "consumers": [ { "topic": "orders", "batch_size": -5 } ] } }"#,
        );
        assert!(err.is_err(), "{err:?}");
    }

    #[test]
    fn validate_backpressure_capacity_below_one_throws_naming_key() {
        let message = validate_err(&with_consumer(ConsumerOptions {
            topic: "orders".to_owned(),
            backpressure: BackpressureOptions {
                enabled: true,
                capacity: 0,
            },
            ..ConsumerOptions::default()
        }));
        assert!(
            message.contains("consumers[0].backpressure.capacity"),
            "{message}"
        );
    }

    #[test]
    fn validate_start_from_date_without_date_throws_naming_key() {
        let message = validate_err(&with_consumer(ConsumerOptions {
            topic: "orders".to_owned(),
            start_from: StartFrom::Date,
            ..ConsumerOptions::default()
        }));

        assert!(message.contains("consumers[0].start_from"));
        assert!(message.contains("consumers[0].start_from_date"));
    }

    #[test]
    fn validate_start_from_date_with_date_is_allowed() {
        let options = with_consumer(ConsumerOptions {
            topic: "orders".to_owned(),
            start_from: StartFrom::Date,
            start_from_date: Some(datetime!(1970-01-01 00:00:00 UTC)),
            ..ConsumerOptions::default()
        });

        validate(&options).expect("a date is set");
    }

    #[test]
    fn validate_persist_none_with_start_from_stored_throws_naming_both_keys() {
        let message = validate_err(&with_consumer(ConsumerOptions {
            topic: "orders".to_owned(),
            persist: PersistMode::None,
            start_from: StartFrom::Stored,
            ..ConsumerOptions::default()
        }));

        assert!(message.contains("consumers[0].persist"));
        assert!(message.contains("consumers[0].start_from"));
    }

    #[test]
    fn validate_duplicate_topic_consumer_pair_throws_naming_both_entries() {
        let options = StreamOptions {
            consumer: Some("svc".to_owned()),
            consumers: vec![
                ConsumerOptions {
                    topic: "orders".to_owned(),
                    ..ConsumerOptions::default()
                },
                ConsumerOptions {
                    topic: "orders".to_owned(),
                    ..ConsumerOptions::default()
                },
            ],
            ..StreamOptions::default()
        };

        let message = validate_err(&options);
        assert!(message.contains("consumers[1]"), "{message}");
        assert!(message.contains("consumers[0]"), "{message}");
        assert!(message.contains("orders"), "{message}");
    }

    #[test]
    fn validate_same_topic_distinct_consumer_names_is_allowed() {
        let options = StreamOptions {
            consumer: Some("svc".to_owned()),
            consumers: vec![
                ConsumerOptions {
                    topic: "orders".to_owned(),
                    consumer: Some("a".to_owned()),
                    ..ConsumerOptions::default()
                },
                ConsumerOptions {
                    topic: "orders".to_owned(),
                    consumer: Some("b".to_owned()),
                    ..ConsumerOptions::default()
                },
            ],
            ..StreamOptions::default()
        };

        validate(&options).expect("distinct names do not collide");
    }

    // ------------------------------------------------------------ instance rules

    #[test]
    fn validate_instance_count_below_one_throws_naming_key() {
        let message = validate_err(&with_consumer(ConsumerOptions {
            topic: "orders".to_owned(),
            instances: Some(InstanceOptions {
                mode: InstanceMode::Static,
                count: Some(0),
                ..InstanceOptions::default()
            }),
            ..ConsumerOptions::default()
        }));
        assert!(
            message.contains("consumers[0].instances.count"),
            "{message}"
        );
    }

    #[test]
    fn validate_instance_index_negative_is_a_deserialize_error() {
        let err = bind_json(
            r#"{ "streams": { "consumers": [ { "topic": "orders", "instances": { "mode": "static", "count": 2, "index": -1 } } ] } }"#,
        );
        assert!(err.is_err(), "{err:?}");
    }

    #[test]
    fn validate_instance_index_not_less_than_count_throws_naming_both_keys() {
        let message = validate_err(&with_consumer(ConsumerOptions {
            topic: "orders".to_owned(),
            instances: Some(InstanceOptions {
                mode: InstanceMode::Static,
                count: Some(2),
                index: Some(2),
                ..InstanceOptions::default()
            }),
            ..ConsumerOptions::default()
        }));

        assert!(message.contains("consumers[0].instances.index"));
        assert!(message.contains("consumers[0].instances.count"));
    }

    // ------------------------------------------------------------ producer rules

    #[test]
    fn validate_producer_without_topic_throws_naming_key() {
        for topic in ["", "  "] {
            let message = validate_err(&with_producer(ProducerOptions {
                topic: topic.to_owned(),
                ..ProducerOptions::default()
            }));
            assert!(message.contains("producers[0].topic"), "{message}");
        }
    }

    #[test]
    fn validate_producer_max_batch_below_one_throws_naming_key() {
        let message = validate_err(&with_producer(ProducerOptions {
            topic: "orders".to_owned(),
            max_batch: 0,
            ..ProducerOptions::default()
        }));
        assert!(message.contains("producers[0].max_batch"), "{message}");
    }

    #[test]
    fn validate_producer_max_queue_below_one_throws_naming_key() {
        let message = validate_err(&with_producer(ProducerOptions {
            topic: "orders".to_owned(),
            max_queue: 0,
            ..ProducerOptions::default()
        }));
        assert!(message.contains("producers[0].max_queue"), "{message}");
    }

    #[test]
    fn topic_names_keep_their_case() {
        let options = bind(r#"{ "streams": { "topics": { "Orders": { "partitions": 3 } } } }"#);

        assert_eq!(options.topics["Orders"].partitions, 3);
        assert!(!options.topics.contains_key("orders"));
    }
}
