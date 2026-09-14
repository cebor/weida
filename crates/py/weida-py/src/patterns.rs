//! PAIR, SURVEY and BUS on the asyncio surface (B-244).
//!
//! The three patterns the library gained after this binding was written. They
//! are here rather than in [`crate::endpoints`] because they are a slice of
//! their own, not because they are different in kind: whole payloads, an
//! explicit ceiling on every receive, and no pattern behaviour of their own —
//! the one-peer rule, the survey deadline and the never-yourself rule are
//! `weida`'s ([PATTERNS.md](../../../../docs/PATTERNS.md) §4, §5, §6).
//!
//! What a caller has to know per pattern, and nothing else:
//!
//! - **PAIR** keeps its **first** peer. A second dialler is refused on the
//!   transfer, not on the connection, so the refusal cannot be mistaken for a
//!   network failure — and the first peer keeps working, which is the opposite
//!   of ZeroMQ's PAIR.
//! - **SURVEY** is bounded by the **caller's** deadline. Nothing on the wire
//!   carries it and a respondent never learns it; silence is a number.
//! - **BUS** never delivers a member its own message, and forwards for nobody:
//!   a bus of *n* members is *n* × (*n* − 1) deliveries.

use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;
use weida::{BusMember, Paired, Respondent, Runtime, Surveyor, TransferMeta};
use weida_py_core::{Bridge, payload_of};

use crate::endpoints::{PyRequest, endpoint};
use crate::errors::{errno_of, to_py};
use crate::values::{PyIncomingMeta, PySurvey};

/// Turns a Python float of seconds into a deadline, or says why not.
///
/// Mandatory and finite, for the reason `Runtime.drain` gives: a survey
/// without a deadline is a program that waits on a stranger.
fn deadline_of(py: Python<'_>, seconds: f64) -> PyResult<Duration> {
    Duration::try_from_secs_f64(seconds).map_err(|e| {
        to_py(
            py,
            &errno_of(weida::Error::Runtime(format!("deadline: {e}"))),
        )
    })
}

endpoint!(
    PyPaired,
    Paired,
    "Paired",
    "`weida.Paired`: one peer, transfers in both directions, whole payloads."
);

#[pymethods]
impl PyPaired {
    /// The endpoint path this pair uses; empty on a dialling pair that has
    /// not connected yet.
    fn path(&self) -> String {
        self.endpoint.path().to_owned()
    }

    /// Dials `url`, once. See [`crate::endpoints`] for the address forms.
    ///
    /// # Errors
    ///
    /// `weida.LimitExceeded` on a second call — a pair has one peer and no
    /// policy for choosing between two — `weida.Unsupported` on a bound pair,
    /// which has nothing to dial, plus the usual dialling failures.
    fn connect<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(
            py,
            async move { endpoint.connect(&url).await.map_err(errno_of) },
        )
    }

    /// Peers connected: `0` or `1`.
    fn peer_count(&self) -> usize {
        self.endpoint.peer_count()
    }

    /// Sends `payload` to the peer.
    ///
    /// On a **bound** pair the first send waits for the peer to appear:
    /// nothing is buffered, and a bound pair with no peer has nobody to
    /// address rather than a backlog.
    ///
    /// # Errors
    ///
    /// `weida.LimitExceeded` when the peer's pair already belongs to somebody
    /// else, `weida.NotConnected`, `weida.ConnectionLost`.
    fn send<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let body = payload_of(payload)?;
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(py, async move {
            endpoint
                .send_with(
                    TransferMeta::default().with_content_len(body.len() as u64),
                    &body,
                )
                .await
                .map_err(errno_of)
        })
    }

    /// Waits for the next transfer from the peer, at most `max_bytes`, and
    /// returns `(payload, meta)`.
    ///
    /// # Errors
    ///
    /// `weida.NotConnected` when the endpoint is gone,
    /// `weida.LimitExceeded` when the payload exceeds the ceiling.
    fn recv<'py>(&self, py: Python<'py>, max_bytes: usize) -> PyResult<Bound<'py, PyAny>> {
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(py, async move {
            let transfer = endpoint.recv().await.map_err(errno_of)?;
            let meta = PyIncomingMeta::of(transfer.meta());
            let payload = transfer.collect(max_bytes).await.map_err(errno_of)?;
            Ok((payload, meta))
        })
    }

    fn __repr__(&self) -> String {
        format!("<weida.Paired {}>", self.endpoint.path())
    }
}

endpoint!(
    PySurveyor,
    Surveyor,
    "Surveyor",
    "`weida.Surveyor`: one question to every respondent, bounded by a deadline."
);

#[pymethods]
impl PySurveyor {
    /// Dials `url` and adds one respondent to the set a survey fans out over.
    ///
    /// Unlike a requester, a surveyor accumulates peers on purpose.
    fn connect<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(
            py,
            async move { endpoint.connect(&url).await.map_err(errno_of) },
        )
    }

    /// Respondents currently connected.
    fn peer_count(&self) -> usize {
        self.endpoint.peer_count()
    }

    /// Asks every connected respondent and returns a `weida.Survey` of what
    /// arrived within `deadline` seconds, each answer at most
    /// `max_reply_bytes`.
    ///
    /// A survey with no respondents is **not** an error: it is a `Survey` with
    /// nothing in it, because "nobody answered" is an answer. A respondent
    /// that refuses, dies or overruns the ceiling is counted in `failed`, and
    /// one that says nothing at all is `silent()` — neither ends the survey,
    /// because the respondents are independent exchanges.
    ///
    /// # Errors
    ///
    /// `weida.Runtime` for a deadline that is not a finite, non-negative
    /// number of seconds.
    fn survey<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'py, PyAny>,
        deadline: f64,
        max_reply_bytes: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let body = payload_of(payload)?;
        let deadline = deadline_of(py, deadline)?;
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(py, async move {
            let mut run = endpoint
                .survey_with(
                    TransferMeta::default().with_content_len(body.len() as u64),
                    &body,
                    deadline,
                )
                .await
                .map_err(errno_of)?;
            let asked = run.respondents();
            let mut replies = Vec::new();
            let mut failed = 0usize;
            while let Some(answer) = run.next(max_reply_bytes).await {
                match answer {
                    Ok(reply) => replies.push(reply),
                    // A failure is a number and not an exception: one
                    // respondent's refusal is not the survey's outcome.
                    Err(_) => failed += 1,
                }
            }
            Ok(PySurvey {
                replies,
                asked,
                failed,
                late: run.late(),
            })
        })
    }

    fn __repr__(&self) -> String {
        "<weida.Surveyor>".to_owned()
    }
}

endpoint!(
    PyRespondent,
    Respondent,
    "Respondent",
    "`weida.Respondent`: accepts survey questions and answers them."
);

#[pymethods]
impl PyRespondent {
    /// The endpoint path this respondent serves.
    fn path(&self) -> String {
        self.endpoint.path().to_owned()
    }

    /// Waits for the next question and reads it, at most `max_bytes`,
    /// returning a `weida.Request`.
    ///
    /// The same class a replier hands out, because a question **is** an
    /// exchange: answer it with `reply`, decline it with `refuse`, and
    /// dropping it unanswered is the surveyor's `failed` rather than its
    /// deadline.
    ///
    /// # Errors
    ///
    /// As `Replier.accept`.
    fn accept<'py>(&self, py: Python<'py>, max_bytes: usize) -> PyResult<Bound<'py, PyAny>> {
        let endpoint = Arc::clone(&self.endpoint);
        let bridge = self.bridge.clone();
        let runtime = Arc::clone(&self._runtime);
        self.bridge.awaitable(py, async move {
            let request = endpoint.accept().await.map_err(errno_of)?;
            PyRequest::accepted(request, max_bytes, bridge, runtime).await
        })
    }

    fn __repr__(&self) -> String {
        format!("<weida.Respondent {}>", self.endpoint.path())
    }
}

endpoint!(
    PyBusMember,
    BusMember,
    "BusMember",
    "`weida.BusMember`: bound and dialling at once, every message to every other member."
);

#[pymethods]
impl PyBusMember {
    /// The path this member accepts on.
    fn path(&self) -> String {
        self.endpoint.path().to_owned()
    }

    /// Joins the member at `url`.
    ///
    /// Joining is an ordinary connect and leaving an ordinary disconnect:
    /// there is no membership protocol and nothing to synchronize, which is
    /// what keeps a bus a fan-out rather than a group.
    fn connect<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(
            py,
            async move { endpoint.connect(&url).await.map_err(errno_of) },
        )
    }

    /// Members this one has joined.
    fn peer_count(&self) -> usize {
        self.endpoint.peer_count()
    }

    /// Sends `payload` to every **other** member and returns how many it
    /// reached.
    ///
    /// Never the sender itself, structurally: a send writes to the members
    /// this one dialled, and a member does not dial itself. Best effort per
    /// member with counted drops, exactly as a fan-out, so this does not wait
    /// for a receipt — there is no single peer to get one from.
    ///
    /// # Errors
    ///
    /// `weida.LimitExceeded` for a payload above the per-member byte budget:
    /// such a message could never be enqueued for anybody, so reporting it
    /// beats dropping it for every member.
    fn send<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let body = payload_of(payload)?;
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(py, async move {
            endpoint
                .send_with(
                    TransferMeta::default().with_content_len(body.len() as u64),
                    &body,
                )
                .await
                .map_err(errno_of)
        })
    }

    /// Waits for the next message from another member, at most `max_bytes`,
    /// and returns `(payload, meta)`.
    fn recv<'py>(&self, py: Python<'py>, max_bytes: usize) -> PyResult<Bound<'py, PyAny>> {
        let endpoint = Arc::clone(&self.endpoint);
        self.bridge.awaitable(py, async move {
            let transfer = endpoint.recv().await.map_err(errno_of)?;
            let meta = PyIncomingMeta::of(transfer.meta());
            let payload = transfer.collect(max_bytes).await.map_err(errno_of)?;
            Ok((payload, meta))
        })
    }

    /// Copies that never reached a member.
    fn dropped(&self) -> u64 {
        self.endpoint.dropped()
    }

    fn __repr__(&self) -> String {
        format!("<weida.BusMember {}>", self.endpoint.path())
    }
}
