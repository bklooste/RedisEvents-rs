//! A fake Restate ingress on `127.0.0.1:0`. By default it behaves like Restate 1.8 for workflow
//! `/send`: the first submit of a `{workflow}/{key}` path answers `202 Accepted`, every later one
//! `202 PreviouslyAccepted` with the same invocation id, and any request carrying `Idempotency-Key`
//! is answered `400` (and flagged). `respond` overrides the answer per request.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// One request the fake saw.
#[derive(Debug, Clone)]
pub struct Seen {
    pub path: String,
    pub workflow_key: String,
    pub has_idempotency_key: bool,
    pub trace_parent: Option<String>,
    pub content_type: Option<String>,
    pub body: String,
}

/// What to do with a request.
pub enum Act {
    /// The deduping Restate behaviour.
    Default,
    Reply(u16, String),
    /// Close the socket without answering (connection failure).
    Drop,
    /// Never answer.
    Hang,
    /// Answer with bytes that are not HTTP.
    Garbage,
}

type Responder = Arc<dyn Fn(&Seen) -> Act + Send + Sync>;

#[derive(Default)]
struct State {
    requests: Mutex<Vec<Seen>>,
    answers: Mutex<Vec<(String, String)>>,
    started: Mutex<HashMap<String, String>>,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    idempotency_key_seen: AtomicBool,
    next_id: AtomicUsize,
    latency: Mutex<Duration>,
    respond: Mutex<Option<Responder>>,
}

pub struct FakeIngress {
    state: Arc<State>,
    pub url: reqwest::Url,
}

impl FakeIngress {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url =
            reqwest::Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let state = Arc::new(State::default());
        let st = Arc::clone(&state);
        tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                tokio::spawn(serve(sock, Arc::clone(&st)));
            }
        });
        Self { state, url }
    }

    pub fn respond(&self, f: impl Fn(&Seen) -> Act + Send + Sync + 'static) {
        *self.state.respond.lock().unwrap() = Some(Arc::new(f));
    }

    pub fn clear_respond(&self) {
        *self.state.respond.lock().unwrap() = None;
    }

    pub fn set_latency(&self, d: Duration) {
        *self.state.latency.lock().unwrap() = d;
    }

    pub fn requests(&self) -> Vec<Seen> {
        self.state.requests.lock().unwrap().clone()
    }

    pub fn answers(&self) -> Vec<(String, String)> {
        self.state.answers.lock().unwrap().clone()
    }

    pub fn clear(&self) {
        self.state.requests.lock().unwrap().clear();
        self.state.answers.lock().unwrap().clear();
    }

    pub fn max_in_flight(&self) -> usize {
        self.state.max_in_flight.load(Ordering::SeqCst)
    }

    pub fn idempotency_key_seen(&self) -> bool {
        self.state.idempotency_key_seen.load(Ordering::SeqCst)
    }
}

async fn serve(mut sock: TcpStream, st: Arc<State>) {
    let Some(seen) = read_request(&mut sock).await else {
        return;
    };
    let now = st.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    st.max_in_flight.fetch_max(now, Ordering::SeqCst);
    st.requests.lock().unwrap().push(seen.clone());
    let latency = *st.latency.lock().unwrap();
    if !latency.is_zero() {
        tokio::time::sleep(latency).await;
    }
    let act = if seen.has_idempotency_key {
        st.idempotency_key_seen.store(true, Ordering::SeqCst);
        Act::Reply(400, "{\"code\":400,\"message\":\"cannot use the idempotency key with workflow handlers.\",\"source\":\"ingress\"}".into())
    } else {
        let responder = st.respond.lock().unwrap().clone();
        match responder.map_or(Act::Default, |f| f(&seen)) {
            Act::Default => dedupe(&st, &seen),
            other => other,
        }
    };
    match act {
        Act::Reply(code, body) => {
            let head = format!(
                "HTTP/1.1 {code} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            let _ = sock.write_all(head.as_bytes()).await;
            let _ = sock.write_all(body.as_bytes()).await;
            let _ = sock.shutdown().await;
        }
        Act::Garbage => {
            let _ = sock.write_all(b"this is not http\r\n\r\n").await;
            let _ = sock.shutdown().await;
        }
        Act::Hang => tokio::time::sleep(Duration::from_secs(3600)).await,
        Act::Drop | Act::Default => {}
    }
    st.in_flight.fetch_sub(1, Ordering::SeqCst);
}

fn dedupe(st: &State, seen: &Seen) -> Act {
    let candidate = format!("inv_{}", st.next_id.fetch_add(1, Ordering::SeqCst));
    let mut started = st.started.lock().unwrap();
    let first = started
        .entry(seen.workflow_key.clone())
        .or_insert_with(|| candidate.clone())
        .clone();
    let status = if first == candidate {
        "Accepted"
    } else {
        "PreviouslyAccepted"
    };
    st.answers
        .lock()
        .unwrap()
        .push((seen.workflow_key.clone(), status.to_owned()));
    Act::Reply(
        202,
        format!("{{\"invocationId\":\"{first}\",\"status\":\"{status}\"}}"),
    )
}

async fn read_request(sock: &mut TcpStream) -> Option<Seen> {
    let mut buf = Vec::new();
    let mut chunk = [0_u8; 4096];
    let head_end = loop {
        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break p;
        }
        let n = sock.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let path = lines.next()?.split(' ').nth(1)?.to_owned();
    let mut headers = HashMap::new();
    for l in lines {
        if let Some((k, v)) = l.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_owned());
        }
    }
    let len: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mut body = buf[head_end + 4..].to_vec();
    while body.len() < len {
        let n = sock.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
    let workflow_key = if parts.len() >= 2 {
        format!("{}/{}", parts[0], parts[1])
    } else {
        path.clone()
    };
    Some(Seen {
        path,
        workflow_key,
        has_idempotency_key: headers.contains_key("idempotency-key"),
        trace_parent: headers.get("traceparent").cloned(),
        content_type: headers.get("content-type").cloned(),
        body: String::from_utf8_lossy(&body).into_owned(),
    })
}
