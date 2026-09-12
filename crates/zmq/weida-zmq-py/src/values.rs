//! The values that cross: a message, and what a send did with it.
//!
//! # `Multipart` is one value
//!
//! 37/ZMTP delivers a message "all frames or none", and the zguide's
//! corollary is that nothing goes on the wire until the last frame is given
//! to the socket. So a message here is a *value* — a `Multipart` handed over
//! whole — rather than a sequence of calls that could be interrupted halfway.
//! libzmq's `ZMQ_SNDMORE` is the opposite bargain: a caller that forgets the
//! final frame leaves a half-built message in the socket, and every recipe
//! that reads a partial message is debugging that.
//!
//! A `Multipart` is a sequence of `bytes`: `len()`, `[0]`, iteration and
//! equality against a plain list all work, so
//! `assert await sock.recv() == [b"Hello"]` reads the way a test should while
//! the value that crossed is still one message.
//!
//! # `Sent` and `Published` are reports, not booleans
//!
//! `zmq_send` on a socket whose mute action is *drop* — PUB, ROUTER, REP —
//! returns success whether the message was queued or thrown away. Both
//! outcomes are real and a caller that counts them needs to tell them apart,
//! so they are returned rather than flattened.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes, PyList};
use weida_zmq::{Message, Multipart, Published, Sent};

use crate::errors::{errno_of, to_py};

/// One ZeroMQ message: one or more frames, sent and received atomically.
#[pyclass(frozen, name = "Multipart", module = "weida_zmq", sequence)]
pub struct PyMultipart {
    frames: Vec<Vec<u8>>,
}

impl PyMultipart {
    /// Wraps a library message.
    pub fn of(message: Multipart) -> PyMultipart {
        PyMultipart {
            frames: message
                .into_frames()
                .into_iter()
                .map(Message::into_bytes)
                .collect(),
        }
    }

    /// The library message this stands for.
    pub fn to_library(&self, py: Python<'_>) -> PyResult<Multipart> {
        Multipart::new(self.frames.iter().cloned().map(Message::from).collect())
            .map_err(|error| to_py(py, &errno_of(error)))
    }
}

#[pymethods]
impl PyMultipart {
    /// `Multipart(b"one", b"two")`, or `Multipart([b"one", b"two"])`.
    ///
    /// A message of no frames does not exist — the smallest one is a single
    /// empty frame, which is the envelope delimiter — so an empty call is
    /// `EINVAL` rather than a value that fails later on the wire.
    #[new]
    #[pyo3(signature = (*frames))]
    fn new(py: Python<'_>, frames: &Bound<'_, PyAny>) -> PyResult<PyMultipart> {
        let given: Vec<Bound<'_, PyAny>> = frames.try_iter()?.collect::<PyResult<_>>()?;
        // `Multipart([b"a", b"b"])` and `Multipart(b"a", b"b")` both mean what
        // they look like: one iterable that is not itself a frame is the frame
        // list.
        let collected = match given.as_slice() {
            [only] if !is_frame(only) => only
                .try_iter()?
                .map(|frame| weida_py_core::payload_of(&frame?))
                .collect::<PyResult<Vec<Vec<u8>>>>()?,
            _ => given
                .iter()
                .map(weida_py_core::payload_of)
                .collect::<PyResult<Vec<Vec<u8>>>>()?,
        };
        if collected.is_empty() {
            return Err(to_py(
                py,
                &weida_py_core::Errno::new(
                    "EINVAL",
                    "a message has at least one frame; the smallest one is a single empty frame",
                ),
            ));
        }
        Ok(PyMultipart { frames: collected })
    }

    /// The frames, in order, as a list of `bytes`.
    #[getter]
    fn frames<'py>(&self, py: Python<'py>) -> Bound<'py, PyList> {
        PyList::new(py, self.frames.iter().map(|frame| PyBytes::new(py, frame)))
            .expect("a list of frames")
    }

    fn __len__(&self) -> usize {
        self.frames.len()
    }

    fn __getitem__<'py>(&self, py: Python<'py>, index: isize) -> PyResult<Bound<'py, PyBytes>> {
        let length = self.frames.len() as isize;
        let resolved = if index < 0 { index + length } else { index };
        if resolved < 0 || resolved >= length {
            return Err(pyo3::exceptions::PyIndexError::new_err(
                "frame index out of range",
            ));
        }
        Ok(PyBytes::new(py, &self.frames[resolved as usize]))
    }

    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.frames(py)
            .into_any()
            .try_iter()
            .map(|it| it.into_any())
    }

    /// Equal to another `Multipart`, and to any sequence of `bytes` with the
    /// same frames — which is what makes a test read as one.
    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        if let Ok(twin) = other.extract::<PyRef<'_, PyMultipart>>() {
            return self.frames == twin.frames;
        }
        match other.extract::<Vec<Vec<u8>>>() {
            Ok(frames) => self.frames == frames,
            Err(_) => false,
        }
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.frames.hash(&mut hasher);
        hasher.finish()
    }

    fn __repr__(&self) -> String {
        let sizes: Vec<String> = self
            .frames
            .iter()
            .map(|frame| frame.len().to_string())
            .collect();
        format!(
            "<weida_zmq.Multipart {} frame(s), bytes: [{}]>",
            self.frames.len(),
            sizes.join(", ")
        )
    }
}

/// Whether a value is one frame rather than a sequence of them.
fn is_frame(object: &Bound<'_, PyAny>) -> bool {
    object.is_instance_of::<PyBytes>() || object.is_instance_of::<PyByteArray>()
}

/// Takes a message from Python: a [`PyMultipart`], one `bytes` frame, or an
/// iterable of frames.
///
/// The copy here is the one an owning `Message` forces, and it is the only one
/// on this path — PyO3 lends the interpreter's buffer rather than copying it
/// first (`weida_py_core::bytes`).
pub fn message_from(object: &Bound<'_, PyAny>) -> PyResult<Multipart> {
    let py = object.py();
    if let Ok(message) = object.extract::<PyRef<'_, PyMultipart>>() {
        return message.to_library(py);
    }
    if is_frame(object) {
        return Ok(Multipart::single(Message::from(weida_py_core::payload_of(
            object,
        )?)));
    }
    let mut frames = Vec::new();
    for frame in object.try_iter()? {
        frames.push(Message::from(weida_py_core::payload_of(&frame?)?));
    }
    Multipart::new(frames).map_err(|error| to_py(py, &errno_of(error)))
}

/// What a send did on a socket type that drops rather than blocks.
#[pyclass(frozen, name = "Sent", module = "weida_zmq")]
pub struct PySent {
    /// Whether the message is in a peer's queue.
    #[pyo3(get)]
    queued: bool,
}

impl PySent {
    /// Wraps the library's report.
    pub fn of(sent: Sent) -> PySent {
        PySent {
            queued: matches!(sent, Sent::Queued),
        }
    }
}

#[pymethods]
impl PySent {
    /// Whether the message was thrown away because the queue was full.
    ///
    /// Nothing will retry it; that is what this socket type's mute action
    /// means.
    #[getter]
    fn dropped(&self) -> bool {
        !self.queued
    }

    fn __bool__(&self) -> bool {
        self.queued
    }

    fn __repr__(&self) -> &'static str {
        if self.queued {
            "<weida_zmq.Sent queued>"
        } else {
            "<weida_zmq.Sent dropped>"
        }
    }
}

/// What a publish reached.
#[pyclass(frozen, get_all, name = "Published", module = "weida_zmq")]
pub struct PyPublished {
    /// Subscribers whose queue took a copy.
    pub delivered: usize,
    /// Subscribers that matched but whose queue was full, so their copy was
    /// dropped — a publisher never blocks.
    pub dropped: usize,
}

impl PyPublished {
    /// Wraps the library's report.
    pub fn of(published: Published) -> PyPublished {
        PyPublished {
            delivered: published.delivered,
            dropped: published.dropped,
        }
    }
}

#[pymethods]
impl PyPublished {
    fn __repr__(&self) -> String {
        format!(
            "<weida_zmq.Published delivered={} dropped={}>",
            self.delivered, self.dropped
        )
    }
}
