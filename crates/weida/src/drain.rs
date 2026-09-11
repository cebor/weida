//! The bounded drain of [decision 0009](../../../docs/decisions/0009-drain.md).
//!
//! `Runtime::shutdown` is abortive: it closes, and a FIN that was queued but
//! not yet acknowledged never lands [0009 §4.1]. A drain is the counterpart —
//! stop admitting, give the finished transfers their chance, then perform the
//! same close — and what it can wait for is exactly one thing: the peer's
//! **transport** receipt, the condition `Delivery::delivered()` reports
//! [0009 §4.2]. Nothing here waits for a peer application to read anything,
//! and nothing here goes on the wire [0009 §4.8].
//!
//! The state below exists because a fire-and-forget sender drops its
//! `Delivery` and with it the only handle that can observe the
//! acknowledgement. Such a receipt is parked here instead of being dropped, so
//! that a later drain has something to wait on. A receipt the application
//! keeps is never parked: whoever holds it is doing the waiting.
//!
//! The parked set holds nothing new in the sense of
//! [`docs/INVARIANTS.md`](../../../docs/INVARIANTS.md) — a receipt is a
//! future over a stream that already exists — but it is a set, so it is
//! bounded: by the stream budgets already in `Limits`, and by reaping settled
//! receipts on the way in. Both are local quantities; a peer cannot grow this
//! by sending.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use weida_core::Limits;

/// One finished transfer's acknowledgement, as quinn reports it.
pub(crate) type Receipt =
    Pin<Box<dyn Future<Output = Result<Option<quinn::VarInt>, quinn::StoppedError>> + Send + Sync>>;

/// What a drain achieved, counted locally.
///
/// Both numbers are about *this* drain: the transfers that were still
/// unacknowledged when it started. Neither is a statement about the peer's
/// application — L0 has no such signal, which is why the drain reports counts
/// rather than a guarantee [0009 §4.6].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Drained {
    /// Transfers whose payload and FIN the peer's transport acknowledged
    /// before the deadline.
    pub delivered: u64,
    /// Transfers still unacknowledged when the deadline expired, plus any the
    /// peer refused with `STOP_SENDING` and any whose connection was lost.
    /// A non-zero count is not an error.
    pub outstanding: u64,
}

/// Admission flag plus the parked receipts of one runtime.
pub(crate) struct DrainState {
    /// Set for good when a drain starts: bindings stop accepting connections
    /// and a new inbound stream is refused with `SHUTDOWN` [0009 §4.5].
    draining: AtomicBool,
    /// Receipts nobody else is waiting on, oldest first.
    parked: Mutex<VecDeque<Receipt>>,
    /// Cap on the parked set, taken from the stream budgets.
    max_parked: usize,
    /// Receipts evicted at the cap before they settled. They can no longer be
    /// observed from here, so the next drain counts them as outstanding
    /// rather than assuming they landed.
    evicted: AtomicU64,
}

impl DrainState {
    pub(crate) fn new(limits: &Limits) -> DrainState {
        // The transfers that can plausibly be unacknowledged at once are
        // bounded by the concurrent streams this side deals in; reusing that
        // number adds no new knob (0009 §5 asks for none).
        let max_parked = limits.max_concurrent_uni_streams as usize
            + limits.max_concurrent_bidi_streams as usize;
        DrainState {
            draining: AtomicBool::new(false),
            parked: Mutex::new(VecDeque::new()),
            max_parked,
            evicted: AtomicU64::new(0),
        }
    }

    /// Whether admission has stopped.
    pub(crate) fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Relaxed)
    }

    /// Stops admission. Idempotent.
    pub(crate) fn begin(&self) {
        self.draining.store(true, Ordering::Relaxed);
    }

    /// Parks the receipt of a finished transfer nobody is waiting on.
    ///
    /// Settled receipts at the front are reaped on the way in, so a healthy
    /// connection keeps this set near empty without a task polling it.
    pub(crate) fn park(&self, receipt: Receipt) {
        let mut parked = self.parked.lock().expect("drain state poisoned");
        while let Some(front) = parked.front_mut() {
            if settled(front).is_none() {
                break;
            }
            parked.pop_front();
        }
        if parked.len() >= self.max_parked {
            parked.pop_front();
            self.evicted.fetch_add(1, Ordering::Relaxed);
        }
        parked.push_back(receipt);
    }

    /// Takes everything this drain has to wait for.
    pub(crate) fn take(&self) -> (Vec<Receipt>, u64) {
        let parked = {
            let mut parked = self.parked.lock().expect("drain state poisoned");
            std::mem::take(&mut *parked)
        };
        (parked.into(), self.evicted.swap(0, Ordering::Relaxed))
    }
}

/// Polls one parked receipt without a real waker: `Some` when it has settled.
///
/// A receipt only ever needs one look — either the stream is acknowledged or
/// it is not — so reaping costs a poll and never a wake-up registration.
fn settled(receipt: &mut Receipt) -> Option<Result<Option<quinn::VarInt>, quinn::StoppedError>> {
    let mut cx = Context::from_waker(Waker::noop());
    match receipt.as_mut().poll(&mut cx) {
        Poll::Ready(outcome) => Some(outcome),
        Poll::Pending => None,
    }
}

/// Awaits `receipts` until they settle or `deadline` elapses, counting both.
///
/// Polling the whole set on every wake is linear, which is the right trade
/// here: the set is bounded, and this runs once per process rather than per
/// message.
pub(crate) async fn wait_for(
    receipts: Vec<Receipt>,
    evicted: u64,
    deadline: impl Future<Output = ()>,
) -> Drained {
    let mut delivered = 0u64;
    let mut refused = 0u64;
    let mut pending: Vec<Option<Receipt>> = receipts.into_iter().map(Some).collect();

    {
        let settle = std::future::poll_fn(|cx: &mut Context<'_>| {
            let mut left = 0usize;
            for slot in pending.iter_mut() {
                let Some(receipt) = slot else { continue };
                match receipt.as_mut().poll(cx) {
                    Poll::Ready(Ok(None)) => {
                        delivered += 1;
                        *slot = None;
                    }
                    // The peer stopped the stream, or the connection went
                    // away: settled, but not delivered.
                    Poll::Ready(_) => {
                        refused += 1;
                        *slot = None;
                    }
                    Poll::Pending => left += 1,
                }
            }
            if left == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        });

        tokio::select! {
            () = settle => {}
            () = deadline => {}
        }
    }

    let unfinished = pending.iter().filter(|slot| slot.is_some()).count() as u64;
    Drained {
        delivered,
        outstanding: unfinished + refused + evicted,
    }
}
