//! Topic meta (`WIRE.md` §3). **Owner: B1** (port of the meta part of `Admin/StreamAdmin.cs`).
//! B1 exposes `pub async fn ensure_topic_meta(conn, keys, topic, &TopicOptions) -> Result<TopicMeta>`
//! here; C4 calls it at startup too.
