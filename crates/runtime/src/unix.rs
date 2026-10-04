//! `AF_UNIX` bind hygiene and the kernel's answer to *who is on the other
//! end*.
//!
//! A filesystem endpoint is not a port: it has an owner, a mode, a path
//! length the kernel truncates at, and a node that outlives the process that
//! bound it. Every one of those is a hazard with a known answer, and the
//! answers are the same for any protocol that offers an `ipc://`-style
//! transport — weida's `weida+unix://` and ZeroMQ's `ipc://` differ in what
//! they write on the socket, not in how they bind it
//! ([decisions/0010](../../../docs/decisions/0010-local-transport.md) §4.5,
//! `docs/research/ipc.md` §1.2, §1.5, §7).

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use tokio::net::{UnixListener, UnixStream};
use weida_core::{Error, LocalPrincipal, MAX_SOCKET_PATH_BYTES};

/// Mode of a bound socket file.
///
/// Set explicitly after bind, never inherited: a new socket file gets every
/// permission bit `umask` does not mask, so the default is whatever the
/// process happened to inherit (`docs/research/ipc.md` §1.2, [0010 §4.5]).
const SOCKET_MODE: u32 = 0o600;

/// A bound `AF_UNIX` socket file: the node exists as long as this value
/// does.
///
/// Hold it beside the [`UnixListener`] it was bound with. Dropping it removes
/// the node, which is what keeps the *next* bind of the same path out of the
/// unlink-then-bind race below.
#[derive(Debug)]
pub struct BoundUnixSocket {
    path: PathBuf,
}

impl BoundUnixSocket {
    /// Binds `path` with the hygiene a filesystem endpoint needs, replacing a
    /// stale socket file left by a crash.
    ///
    /// Four things happen here, and each answers a documented hazard:
    ///
    /// 1. **The path budget.** `sun_path` is 108 bytes on Linux and 104 on
    ///    macOS *including* the terminator, and the kernel truncates rather
    ///    than failing, so an over-long path binds something other than what
    ///    was asked for. It is refused instead
    ///    (`docs/research/ipc.md` §1.1, §2.1). libzmq's `ipc://` publishes
    ///    the same limit as "113 characters including the prefix" and leaves
    ///    the rest to the caller (`docs/research/zeromq.md` §11).
    /// 2. **The socket-type check.** Closing a socket does not remove its
    ///    node, so a crash leaves one and `bind()` then fails with
    ///    `EADDRINUSE`. A stale node is a socket nobody is listening on, and
    ///    a connect probes for that: a live one is
    ///    [`Error::AddressInUse`] and left alone (the probe's connection is
    ///    accepted and closed by the incumbent). Anything else at that path is
    ///    not ours to remove, and removing it anyway is how a bind deletes a
    ///    caller's data.
    /// 3. **Unlink, then bind.** The usual answer to the stale node, and it
    ///    opens a substitution race that is closed only "unless directory
    ///    ownership and permissions prevent endpoint substitution"
    ///    (`docs/research/ipc.md` §1.2, §7). **The directory is therefore
    ///    load-bearing**: the caller MUST place the socket in a directory it
    ///    owns and that no other user may write. libzmq's `ipc://` has this
    ///    hazard too and answers none of it — a local process can steal a
    ///    bound endpoint.
    /// 4. **The mode, explicitly.** `0600` set *after* bind, because a socket
    ///    file is created with whatever `umask` allows, which is whatever the
    ///    process happened to inherit.
    pub fn bind(path: &Path) -> Result<(BoundUnixSocket, UnixListener), Error> {
        BoundUnixSocket::bind_with(path, false)
    }

    /// As [`BoundUnixSocket::bind`], with libzmq's `ipc://` semantics: a
    /// live node is replaced, and the listener that held it no longer
    /// receives connections.
    pub fn bind_stealing(path: &Path) -> Result<(BoundUnixSocket, UnixListener), Error> {
        BoundUnixSocket::bind_with(path, true)
    }

    fn bind_with(path: &Path, steal: bool) -> Result<(BoundUnixSocket, UnixListener), Error> {
        if path.as_os_str().len() > MAX_SOCKET_PATH_BYTES {
            return Err(Error::InvalidAddress(format!(
                "socket path exceeds this platform's {}-byte sun_path budget: {}",
                MAX_SOCKET_PATH_BYTES,
                path.display()
            )));
        }
        // A stale node is a socket nobody is listening on. Anything else at
        // that path is not ours to remove.
        match std::fs::metadata(path) {
            Ok(meta) if is_socket(&meta) => {
                if !steal {
                    match std::os::unix::net::UnixStream::connect(path) {
                        Ok(_) => return Err(Error::AddressInUse(path.display().to_string())),
                        Err(e)
                            if matches!(
                                e.kind(),
                                std::io::ErrorKind::ConnectionRefused
                                    | std::io::ErrorKind::NotFound
                            ) => {}
                        Err(e) => return Err(Error::Io(e)),
                    }
                }
                match std::fs::remove_file(path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(Error::Io(e)),
                }
            }
            Ok(_) => {
                return Err(Error::InvalidAddress(format!(
                    "{} exists and is not a socket",
                    path.display()
                )));
            }
            Err(_) => {}
        }
        let listener = UnixListener::bind(path).map_err(Error::Io)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(SOCKET_MODE))
            .map_err(Error::Io)?;
        Ok((
            BoundUnixSocket {
                path: path.to_path_buf(),
            },
            listener,
        ))
    }

    /// The path this socket is bound at.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for BoundUnixSocket {
    fn drop(&mut self) {
        // Leaving the node behind is what forces the next bind into
        // unlink-then-bind; removing it on the way out keeps the common case
        // free of that race.
        let _ = std::fs::remove_file(&self.path);
    }
}

fn is_socket(meta: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::FileTypeExt;
    meta.file_type().is_socket()
}

/// The credentials the kernel attributes to the peer of `stream`.
///
/// This is the whole of a local peer's identity: there is no key, no
/// certificate and nothing the peer asserts about itself — the kernel says
/// what it is, which is why it can be trusted at all
/// ([decisions/0010](../../../docs/decisions/0010-local-transport.md) §4.4,
/// §4.5).
///
/// Taken at accept or connect time, which is when `SO_PEERCRED` captures
/// them; they are **not** re-read per message, and re-reading would not help:
/// the values are a snapshot of the peer as it was when the socket was
/// created (`docs/research/ipc.md` §1.5). The pid is an `Option` because
/// macOS's `LOCAL_PEERCRED` reports none, and because a pid is an
/// observation — it may be reused — rather than an identity.
///
/// A protocol that uses this as an authorization input should say so
/// explicitly: libzmq's `ZMQ_IPC_FILTER_UID`/`_GID`/`_PID` did exactly this
/// and are deprecated in favour of ZAP, which is a decision about *where*
/// authorization lives and not about whether the kernel's answer is true
/// (`docs/research/zeromq.md` §10).
pub fn peer_credentials(stream: &UnixStream) -> Result<LocalPrincipal, Error> {
    let cred = stream.peer_cred().map_err(Error::Io)?;
    Ok(LocalPrincipal {
        uid: cred.uid(),
        gid: cred.gid(),
        // macOS reports no PID at all, and a PID is an observation even where
        // it exists [0010 §4.4].
        pid: cred.pid().map(|pid| pid as u32),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A private directory per test, which is also what the substitution race
    /// of `bind` requires of a caller.
    fn dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("weida-runtime-{}-{tag}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("test directory");
        dir
    }

    /// Claim: the mode is what this function sets, not what `umask` allowed.
    #[tokio::test]
    async fn a_bound_socket_is_private_to_its_owner() {
        let path = dir("mode").join("s");
        let (bound, _listener) = BoundUnixSocket::bind(&path).expect("bind");
        let mode = std::fs::metadata(&path)
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, SOCKET_MODE, "mode {mode:o}");
        assert_eq!(bound.path(), path.as_path());
    }

    /// Claim: the node goes away with the binding, so the next bind of the
    /// same path is not an unlink-then-bind at all.
    #[tokio::test]
    async fn the_node_is_removed_on_drop() {
        let path = dir("drop").join("s");
        let (bound, listener) = BoundUnixSocket::bind(&path).expect("bind");
        assert!(path.exists());
        drop(listener);
        drop(bound);
        assert!(!path.exists(), "the socket node outlived its binding");
    }

    /// Claim: a stale node — a socket file with nobody listening, which is
    /// what a crash leaves — is replaced rather than reported as
    /// `EADDRINUSE`.
    #[tokio::test]
    async fn a_stale_socket_is_replaced() {
        let path = dir("stale").join("s");
        {
            let (bound, listener) = BoundUnixSocket::bind(&path).expect("first bind");
            // Forget the guard the way a crash does: the node stays behind.
            std::mem::forget(bound);
            drop(listener);
        }
        assert!(path.exists(), "the stale node must still be there");
        let (_bound, _listener) = BoundUnixSocket::bind(&path).expect("bind over the stale node");
    }

    /// Claim: a socket someone listens on is theirs; a second bind refuses
    /// it and leaves it working.
    #[tokio::test]
    async fn a_live_socket_is_refused_and_left_alone() {
        let path = dir("live").join("s");
        let (_bound, _listener) = BoundUnixSocket::bind(&path).expect("first bind");
        let err = BoundUnixSocket::bind(&path).unwrap_err();
        assert!(matches!(err, Error::AddressInUse(_)), "{err:?}");
        std::os::unix::net::UnixStream::connect(&path).expect("the first listener still answers");
    }

    /// Claim: the stealing bind replaces a live socket, as libzmq's `ipc://`.
    #[tokio::test]
    async fn bind_stealing_replaces_a_live_socket() {
        let path = dir("steal").join("s");
        let (first, _old) = BoundUnixSocket::bind(&path).expect("first bind");
        std::mem::forget(first);
        let (_bound, new) = BoundUnixSocket::bind_stealing(&path).expect("stealing bind");
        let _client = tokio::net::UnixStream::connect(&path)
            .await
            .expect("connect");
        new.accept()
            .await
            .expect("the new listener takes the connection");
    }

    /// Claim: anything at that path that is not a socket is refused, because
    /// unlinking it would delete somebody's file.
    #[tokio::test]
    async fn a_regular_file_is_not_unlinked() {
        let path = dir("file").join("s");
        std::fs::write(&path, b"not a socket").expect("write");
        let err = BoundUnixSocket::bind(&path).unwrap_err();
        assert!(matches!(err, Error::InvalidAddress(_)), "{err:?}");
        assert_eq!(std::fs::read(&path).expect("read"), b"not a socket");
    }

    /// Claim: a path the kernel would truncate is refused before it binds
    /// something other than what was asked for.
    #[tokio::test]
    async fn an_over_long_path_is_refused() {
        let path = dir("long").join("x".repeat(MAX_SOCKET_PATH_BYTES + 1));
        let err = BoundUnixSocket::bind(&path).unwrap_err();
        assert!(matches!(err, Error::InvalidAddress(_)), "{err:?}");
    }

    /// Claim: the credentials are the kernel's, and on a socket between two
    /// halves of this process they are this process's own.
    #[tokio::test]
    async fn peer_credentials_are_the_kernels_answer() {
        let (a, _b) = UnixStream::pair().expect("socket pair");
        let principal = peer_credentials(&a).expect("credentials");
        assert_eq!(principal.uid, unsafe_free_uid());
        if let Some(pid) = principal.pid {
            assert_eq!(pid, std::process::id());
        }
    }

    /// This process's uid without `unsafe`: the effective uid of the owner of
    /// a file this process just created.
    fn unsafe_free_uid() -> u32 {
        use std::os::unix::fs::MetadataExt;
        let path = dir("uid").join("owned");
        std::fs::write(&path, b"").expect("write");
        let uid = std::fs::metadata(&path).expect("metadata").uid();
        let _ = std::fs::remove_file(&path);
        uid
    }
}
