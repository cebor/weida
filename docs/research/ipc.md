# Local transports: IPC and in-process

## 0. Scope and roles

This sheet describes the operating-system mechanisms for moving bytes between two peers on one
machine, in the vocabulary of the platforms themselves: sockets, pipes, ports, sections, mappings,
handles, descriptors. It is not about a messaging protocol, so it uses the section layout given
below instead of the protocol template of `README.md`. The sourcing rules are the same: every
claim carries a source with a version or date, and every sentence that is the author's own
extrapolation is marked `[inference]` or `[uncertain]`.

Two roles are distinguished throughout, because they have different requirements:

- **Role A — peer to peer.** A library user connects one process to another process on the same
  machine. Neither side is privileged, both are typically started by the same user, and the pair
  may be short-lived. The endpoint name is agreed between the two programs.
- **Role B — client to local runtime.** A client process attaches to a long-running local daemon
  that was started independently, possibly by a service manager, possibly as another user. The
  endpoint name is well known, the daemon outlives clients, and the daemon must decide whether a
  connecting peer is allowed to talk to it.

The difference matters in four places. Role B needs a stable, discoverable name; role A can use an
inherited, unnamed endpoint. Role B needs peer authentication; role A can often assume the peer.
Role B has to survive a daemon crash and restart, which is where stale endpoint state becomes a
problem. Role A can pass a descriptor or handle at spawn time, which is the cheapest and most
robust arrangement available on every platform.

Out of scope: network transports between machines, cross-VM transports (`AF_VSOCK`, Hyper-V
sockets), clipboard-like or window-message channels, and file-based mailbox polling as a primary
transport. Loopback TCP and UDP are in scope because they are the universal fallback.

## 1. Linux

### 1.1 Unix domain sockets: types and addressing

`AF_UNIX` (also `AF_LOCAL`) is the socket family for same-machine IPC; endpoints are created with
`socket(AF_UNIX, type, 0)` or as a connected pair with `socketpair(AF_UNIX, type, 0, sv)` [1].
Three socket types exist on Linux [1]:

- `SOCK_STREAM` — a byte stream with no message boundaries.
- `SOCK_DGRAM` — preserves message boundaries; unlike Internet UDP, `AF_UNIX` datagrams are always
  reliable and are never reordered [1].
- `SOCK_SEQPACKET` — connection-oriented, preserves message boundaries, delivers in send order;
  added in Linux 2.6.4 [1].

`SOCK_SEQPACKET` is the type that gives a local transport framing and connection semantics at
once, removing the need for a length-prefix framing layer. It is Linux-and-some-BSD only; see
sections 2 and 3.

The address is `struct sockaddr_un { sa_family_t sun_family; char sun_path[108]; }` [1]. Three
address kinds are distinguished [1]:

- **pathname** — a NUL-terminated filesystem path; the terminating NUL must fit inside `sun_path`,
  so the usable path is at most 107 bytes. The correct `addrlen` is
  `offsetof(struct sockaddr_un, sun_path) + strlen(path) + 1`; `sizeof(struct sockaddr_un)` is
  accepted but less portable [1]. Portable code cannot even assume 108 bytes: some implementations
  have `sun_path` as short as 92 bytes [1].
- **unnamed** — an unbound stream socket, and both ends of a `socketpair()`. The returned address
  length is `sizeof(sa_family_t)` and `sun_path` must not be inspected [1].
- **abstract** — `sun_path[0]` is a NUL byte; the name is the remaining bytes covered by `addrlen`,
  embedded NULs have no special meaning, and the name has no relation to any filesystem path [1].
  The abstract namespace is a nonportable Linux extension introduced in Linux 2.2 [1].

Two address-handling traps are documented as bugs in `unix(7)`. Linux appends a NUL terminator if
the bind address lacks one, so a 108-byte non-NUL pathname can be returned without a terminator
when the caller supplied only `sizeof(sockaddr_un)` of storage; a safe reader bounds `strnlen()`
by `addrlen - offsetof(sockaddr_un, sun_path)`, or supplies a zeroed buffer of
`sizeof(sockaddr_un) + 1` [1]. Autobind is the second: `bind()` with
`addrlen == sizeof(sa_family_t)`, or setting `SO_PASSCRED` on an unbound socket, assigns an
abstract address of one NUL plus five hexadecimal characters, so at most 2^20 autobind names
exist; it was 8 characters from Linux 2.1.15 and reduced to 5 in Linux 2.3.15 [1].

### 1.2 Filesystem sockets versus abstract sockets

**Permissions.** Creating a pathname socket requires write and search permission on the containing
directory [1]. On Linux, connecting to a pathname stream socket requires write permission on the
socket file, and sending a datagram to a pathname datagram socket likewise requires write
permission on it [1]. POSIX says nothing about socket-file permissions, and on some systems (older
BSDs) they are ignored, so `unix(7)` explicitly warns that portable programs must not rely on this
as a security mechanism [1]. A new socket file is owned by the creating process according to the
usual rules and gets all permission bits except those masked by the process `umask` [1]; `chmod()`
and `chown()` work afterwards [1]. Because the mode is umask-dependent, the robust arrangement is
a private directory with restrictive permissions holding a socket with permissive permissions —
the pattern NNG documents explicitly [119].

Abstract sockets have no permissions at all: `umask` has no effect on bind, and `fchown()`/
`fchmod()` do not change accessibility [1]. Their access control is the network namespace: a
network namespace isolates the abstract `AF_UNIX` namespace along with IP resources and ports [17].
So an abstract socket is reachable by every process in the same network namespace and by no
process outside it.

**Cleanup.** A pathname socket persists as a filesystem object until the caller `unlink()`s it;
closing the socket does not remove the name [1]. `bind()` fails `EADDRINUSE` both when the address
is already in use and when the filesystem object merely exists [1]. A `SIGKILL` or crash therefore
leaves a stale socket file that blocks the next `bind()`. The socket may be unlinked while open and
is finally removed when the last reference closes [1], which makes unlink-then-bind the usual
startup sequence — but that is a namespace race unless directory ownership and permissions prevent
another principal from substituting an endpoint at the same path (see section 7). `SO_REUSEADDR` is
an Internet-socket option and is not a remedy here: `unix(7)` documents pathname collision as
`EADDRINUSE`, not as reusable binding [1][3].

Abstract sockets automatically disappear when the last open reference is closed, including after
process death [1]. They leave no stale state and need no cleanup, which is exactly why they are
attractive for role B on Linux — and exactly why they are unusable in namespaced sandboxes
(section 6).

### 1.3 Buffering, datagram limits, readiness

On `AF_UNIX`, `SO_SNDBUF` has an effect and `SO_RCVBUF` does not [1]. For datagram sockets,
`SO_SNDBUF` imposes an upper limit on outgoing datagram size, computed as the doubled option value
less 32 bytes of overhead [1]; Linux doubles requested socket buffer accounting generally, and the
unprivileged maxima are `/proc/sys/net/core/wmem_max` and `rmem_max` [20]. `MSG_OOB` and `MSG_MORE`
are unsupported on `AF_UNIX`, and `MSG_TRUNC` on `recv()` was unsupported before Linux 3.4 [1]. A
nonblocking socket returns `EAGAIN` rather than blocking, and readiness is awaited with
`poll`/`select`/`epoll` [3].

`epoll` monitors readiness across many descriptors and supports level-triggered and edge-triggered
modes; edge-triggered users should set `O_NONBLOCK` and drain until `EAGAIN` [13]. `AF_UNIX`
descriptors are ordinary pollable file descriptors, so this applies unchanged [3][13].

io_uring is the completion-based alternative, introduced with `io_uring_setup()` in Linux 5.1 [15].
Each submitted SQE normally produces a CQE whose `res` carries the equivalent syscall result or
negative errno, and completion order is not guaranteed to match submission order, so overlapping
same-direction operations on one stream socket must be ordered explicitly, for example with
`IOSQE_IO_LINK` [14]. `IORING_SETUP_SQPOLL` runs a kernel submission-polling thread; before Linux
5.11 it required registered files, and Linux 5.13 removed its special privilege requirement [15].
io_uring is not universally available: Linux 6.6 added `/proc/sys/kernel/io_uring_disabled`, where
`1` forbids ring creation by unprivileged processes and `2` forbids it for everyone, changeable
only with `CAP_SYS_ADMIN` [27]; Moby's default seccomp profile does not include `io_uring_setup`,
`io_uring_enter` or `io_uring_register` in its allow list, so Docker's default profile blocks them
[175]; and Google reported in 2023 that ChromeOS disabled io_uring, Android made it unreachable to
apps via seccomp-BPF, and Google production servers disabled it [33]. Any local transport that uses
io_uring needs an epoll or blocking fallback.

### 1.4 Descriptor passing

Ancillary data is carried by `sendmsg()`/`recvmsg()` as `cmsghdr` records built with
`CMSG_FIRSTHDR`, `CMSG_NXTHDR`, `CMSG_SPACE`, `CMSG_LEN` and `CMSG_DATA` [2]. `SCM_RIGHTS` carries
an integer array of open file descriptors; semantically it transfers a reference to the open *file
description*, equivalent to `dup()` into the receiver, so the descriptor numbers usually differ [1].

Limits and failure modes [1]:

- `SCM_MAX_FD` is 253 (255 before Linux 2.6.38); exceeding it fails `sendmsg()` with `EINVAL`.
- A too-small or absent receive control buffer truncates or discards the ancillary data and
  automatically closes the excess descriptors; `MSG_CTRUNC` is set in `msg_flags`.
- Exceeding the receiver's `RLIMIT_NOFILE` likewise closes the excess descriptors.
- Since mainline Linux 4.5, sending fails `ETOOMANYREFS` if in-flight descriptors exceed the
  sender's `RLIMIT_NOFILE` without `CAP_SYS_RESOURCE`. Before that, an unlimited number could be
  put in flight by sending and then closing each descriptor.
- On a stream socket at least one byte of ordinary data must accompany the ancillary data. Linux
  permits zero-payload ancillary sends on datagram sockets, but portable code should send a byte
  anyway.
- Ancillary data acts as a barrier in the stream: the byte sent with the ancillary data is returned
  together with preceding payload, and later payload comes on a later receive.

In-flight descriptors can form reference cycles — queued file references keep `AF_UNIX` sockets
alive while those sockets keep the queued rights alive — so the kernel carries a dedicated garbage
collector for them in `net/unix/garbage.c` [30]. Kuniyuki Iwashima reworked the GC in 2024; the
rework restructures the scan and mark handling rather than removing the need for cycle collection
[30].

Descriptor passing is the single capability that has no equivalent on Windows (section 3), and it
is what makes memfd-based shared memory, socket activation and privilege separation work on Linux.

### 1.5 Peer credentials

- `SO_PEERCRED` is read-only and returns `struct ucred { pid, uid, gid }` [1]. The credentials are
  those in effect at the time of `connect()`, `listen()` or `socketpair()` — not at the time of a
  later send [1]. It is available only on connected `AF_UNIX` stream sockets and on stream or
  datagram pairs from `socketpair()` [1].
- `SO_PASSCRED` makes the kernel attach `SCM_CREDENTIALS` to each subsequently received message
  [1]. The default content is the sender's PID, real UID and real GID; a sender may specify other
  values only within kernel-checked bounds — another existing PID needs `CAP_SYS_ADMIN`, a foreign
  UID needs `CAP_SETUID`, a foreign GID needs `CAP_SETGID` [1].
- `SO_PASSSEC` enables `SCM_SECURITY`, a NUL-terminated SELinux context string for which the
  receiver should allocate at least `NAME_MAX` bytes; it exists for datagram sockets since Linux
  2.6.18 and for stream sockets since Linux 4.2 [1].
- `SO_PEERSEC` has existed since Linux 2.6.2 and returns the peer socket's security context as a
  string, normally an SELinux context [3].
- `SO_PEERGROUPS` was added in Linux 4.18 and returns the connected peer's supplementary GIDs,
  reporting `ERANGE` for an undersized buffer [28].
- `SO_PEERPIDFD` and `SCM_PIDFD` arrived in Linux 6.5, with `SO_PASSPIDFD` enabling the ancillary
  form [29]. A pidfd refers to one particular process rather than a number, so PID reuse cannot
  redirect it, and it becomes pollable on process exit [16][29].

### 1.6 Pipes and FIFOs

Pipes and FIFOs are unidirectional byte streams with distinct read and write ends; `pipe()` creates
an anonymous pair and `mkfifo()` creates a filesystem name opened with `open()`, after which the
I/O semantics are identical [4]. Capacity was one page before Linux 2.6.11 and has been 16 pages
since — 65,536 bytes with 4096-byte pages [4]. `F_GETPIPE_SZ`/`F_SETPIPE_SZ` query and change it
since Linux 2.6.35, bounded for unprivileged callers by `/proc/sys/fs/pipe-max-size`, whose default
is 1 MiB; since Linux 4.9 that limit also caps the default capacity of a new pipe or newly opened
FIFO [4]. `PIPE_BUF` is 4096 bytes on Linux and writes no larger than it are atomic with respect to
other writers; larger writes can be partial and interleaved [4].

Closing all write ends gives readers EOF; closing all read ends raises `SIGPIPE` in the writer, or
`EPIPE` if the signal is ignored [4]. A FIFO stores no data: the directory entry names one kernel
pipe object that exists only while at least one process has it open [5]. FIFO `open()` normally
blocks until both ends are opened; nonblocking `O_RDONLY` succeeds without a writer while
nonblocking `O_WRONLY` fails `ENXIO` without a reader [5]. Pipes carry no credentials and no
ancillary data, which rules them out for role B; they are the classic role-A mechanism for a
spawned child.

### 1.7 Shared memory

**memfd.** `memfd_create()` (Linux 3.17; glibc wrapper since glibc 2.27) creates an anonymous
RAM-backed file descriptor that is released when all references drop [6]. It starts at length zero
and is sized with `ftruncate()` [6]. `MFD_CLOEXEC` sets close-on-exec; `MFD_ALLOW_SEALING` permits
seals, without which the initial `F_SEAL_SEAL` prevents adding any; `MFD_HUGETLB` exists since
Linux 4.14, combinable with `MFD_ALLOW_SEALING` since Linux 4.16, and `MFD_HUGE_2MB`/`MFD_HUGE_1GB`
select page sizes [6].

Sealing is what makes shared memory safe against an untrusted peer. `F_ADD_SEALS` and `F_GET_SEALS`
arrived in Linux 3.17; seals are inode properties, immediately enforced, additive only, and never
removable [7]. `F_SEAL_SHRINK` prevents size reduction, `F_SEAL_GROW` prevents growth,
`F_SEAL_WRITE` blocks content modification and new shared writable mappings and fails `EBUSY` while
writable shared mappings exist, and `F_SEAL_FUTURE_WRITE` (Linux 5.1) leaves existing writable
mappings alone while failing new writable mappings and `write()` with `EPERM` [7]. Without seals an
untrusted peer can change bytes after they were validated, or shrink the object so the reader takes
`SIGBUS` [6]. The documented producer pattern is: create a sealing-enabled memfd, size and fill it,
apply seals, pass the descriptor over `SCM_RIGHTS`, and have the consumer check `F_GET_SEALS`
before mapping [6].

**POSIX shared memory.** `shm_open()` names an object that unrelated processes can map; Linux
implements it with a dedicated tmpfs normally mounted at `/dev/shm` [8]. Portable names are
`/name`: NUL-terminated, at most `NAME_MAX` (255), exactly one leading slash and no other slash
[8]. `O_CREAT|O_EXCL` makes existence-check and creation atomic [8]. `shm_unlink()` removes the name
while the contents survive until every mapping is gone, so a crash before `shm_unlink()` leaves a
discoverable object in `/dev/shm` [8]. `MAP_SHARED` is the mapping flag for cross-process state;
`MAP_SHARED_VALIDATE` (Linux 4.15) has the same sharing behaviour but rejects unknown flags with
`EOPNOTSUPP` [10].

**System V shared memory.** `shmget()` allocates or looks up a segment by `key_t`; `IPC_PRIVATE` is
a key value that always creates a new segment rather than a flag [9]. Size is rounded to the page
size; `SHM_HUGETLB` exists since Linux 2.6 and `SHM_HUGE_2MB`/`SHM_HUGE_1GB` since Linux 3.8 [9].
Limits are `SHMMAX`, `SHMALL` and `SHMMNI` under `/proc/sys/kernel/`; since Linux 3.16 the default
`SHMALL` and `SHMMAX` are `ULONG_MAX - 2^24`, effectively unlimited, and `SHMMNI` has defaulted to
4096 since Linux 2.4 [9]. Segments outlive process death until explicitly removed, which is the
operational reason modern designs prefer a passed memfd or a carefully unlinked POSIX object
[inference, from 6, 8, 9].

### 1.8 Cross-process synchronisation

A futex word is a 4-byte-aligned 32-bit value; for cross-process use it must live in genuinely
shared memory (`mmap(MAP_SHARED)` or `shmat()`), and the processes may map it at different virtual
addresses as long as it is the same physical word [11]. `FUTEX_WAIT` compares an expected value and
blocks atomically, `FUTEX_WAKE` wakes waiters, so the uncontended path stays in user space [11].
`FUTEX_PRIVATE_FLAG` (Linux 2.6.22) declares a futex process-private and must not be set for
interprocess use [11]. Priority-inheritance futexes can set `FUTEX_OWNER_DIED` when an owner dies
with waiters present, leaving the next owner to repair the protected state [11].
`PTHREAD_PROCESS_SHARED` mutexes are the libc interface over this machinery, and a crash while
holding one requires explicit robustness handling [inference, from 11].

`eventfd()` (Linux 2.6.22) is a kernel 64-bit counter used as a wait/notify descriptor;
`EFD_SEMAPHORE` (Linux 2.6.30) makes each successful read return 1 and decrement by 1, while an
ordinary read returns and clears the whole counter [12]. It is poll/epoll-readable when nonzero and
can replace a two-descriptor signalling pipe [12]. Passing an eventfd over `SCM_RIGHTS` alongside a
memfd separates bulk data from completion notification [inference, from 1, 6, 12].

### 1.9 Loopback as fallback

TCP gives reliable ordered full-duplex byte streams with no record boundaries, so a loopback
fallback needs its own framing [20]. UDP is unreliable and may reorder or duplicate, unlike
`AF_UNIX` datagrams [21]. An unbound UDP sender is auto-bound to a free port from
`/proc/sys/net/ipv4/ip_local_port_range`, which can exhaust under churn [21]. TCP adds connection
state and `TIME_WAIT` handling that `AF_UNIX` does not have [20].

Network namespaces isolate interfaces, protocol stacks, routes, firewall rules and port numbers
[17], so loopback does not cross a namespace boundary without provisioning, for example a veth
pair. This is the same boundary that hides abstract sockets, which means loopback TCP and abstract
`AF_UNIX` fail together in namespaced environments while a bind-mounted pathname socket still works
[inference, from 1, 17].

### 1.10 Security modules and unit sandboxing

`systemd.exec(5)` documents `RestrictAddressFamilies=` as a seccomp-based allow/deny list of
address families for the unit; excluding `AF_UNIX` blocks the corresponding `socket()` calls [32].
`PrivateNetwork=yes` creates a separate network namespace with loopback only, which makes host
abstract `AF_UNIX` names unreachable while filesystem `AF_UNIX` sockets remain accessible subject
to filesystem visibility and permissions [32].

AppArmor gained fine-grained `AF_UNIX` mediation including abstract-address matching (`addr="@…"`
policy notation) in a kernel patch series prepared against Linux 4.13 and shipped with the
AppArmor 2.12-era policy interface [31]; the project wiki lists abstract-socket support as a
development target as early as the 2.9 notes, so the patch series rather than that target is the
citable evidence of availability [204].

### 1.11 What a crash leaves behind (Linux)

| Mechanism | State after `SIGKILL` |
| --- | --- |
| Pathname `AF_UNIX` socket | Socket file remains; next `bind()` fails `EADDRINUSE` [1] |
| Abstract `AF_UNIX` socket | Disappears with the last reference [1] |
| Anonymous pipe / `socketpair` / eventfd / memfd | Freed when no descriptor survives [4][6][12] |
| FIFO | Pathname remains; kernel pipe object exists only while open [5] |
| POSIX shared memory | Name remains in `/dev/shm` until `shm_unlink()` [8] |
| System V segment | Segment remains until explicitly removed [9] |
| Loopback TCP | Connection state drains through normal TCP teardown [20] |

## 2. macOS

### 2.1 Unix domain sockets on Darwin

Darwin documents `PF_LOCAL` as the host-internal protocol family and marks `PF_UNIX` deprecated in
favour of it [34]. `unix(4)` describes the family as on-machine IPC through the ordinary socket
calls and supports `SOCK_STREAM` and `SOCK_DGRAM` [35]. `SOCK_STREAM` is a sequenced, reliable,
bidirectional, connection-based byte stream; `SOCK_DGRAM` is connectionless, unreliable and
fixed-maximum-length [34].

**`SOCK_SEQPACKET` is not available.** Darwin's public `socket.h` defines the constant with numeric
value 5, but that is a definition rather than support [38], and XNU's `AF_LOCAL` implementation
lists `SEQPACKET, RDM` under `TODO` [37]. `socket(2)` documents that an unsupported protocol/type
combination fails with `EPROTONOSUPPORT` or `EPROTOTYPE` [34]. Any design that wants message
framing from the kernel therefore cannot be portable to macOS.

`sockaddr_un.sun_path` is exactly `char sun_path[104]` in Darwin's public header, and `unix(4)`
describes addresses as filesystem paths of at most 104 characters [35][36]. The Darwin structure
also carries `sun_len` and `sun_family` before `sun_path`, and the `SUN_LEN(su)` macro computes the
initialised address length without the unused tail [36]. There is no abstract namespace: `unix(4)`
describes every address as a filesystem pathname and the public `sockaddr_un` provides only
`sun_path` [inference, from 35, 36].

Binding creates a socket file that closing does not remove; the listener must `unlink(2)` it and
startup code must handle stale paths [35]. Normal filesystem access control applies during path
resolution, and `unix(4)` states that `connect(2)`/`sendto(2)` require the destination socket to be
writable [35]. A broken stream send raises `SIGPIPE` by default; `SO_NOSIGPIPE` (option `0x1022`)
suppresses it in favour of `EPIPE`, and `F_SETNOSIGPIPE` does the same for a pipe [34][38][39].

### 2.2 Descriptor passing and peer identity on Darwin

Darwin documents descriptor passing for `AF_LOCAL` `SOCK_STREAM` using `sendmsg`/`recvmsg`
`msg_control` with `SCM_RIGHTS`, whose data is an integer array whose count is encoded in the
ancillary length [35]. Received descriptors are duplicates as if created by `dup(2)`, and
per-process descriptor flags set with `fcntl(2)` do not transfer [35]. The kernel closes
descriptors that are awaiting delivery or are deliberately not received when the destination socket
closes [35]. XNU sets `UIPC_MAX_CMSG_FD` to 512, the maximum descriptors in one `AF_LOCAL`
ancillary mbuf [37]. `[uncertain]` The current manual promises `SCM_RIGHTS` only for `SOCK_STREAM`;
no primary source was found establishing it for `SOCK_DGRAM` on Darwin, so portable code should not
rely on that [35].

Peer identity options, from `<sys/un.h>` and `unix(4)`:

- `LOCAL_PEERCRED` (`SOL_LOCAL`, value `0x001`) returns `struct xucred`, which carries no PID
  [35][36]. It is documented for `SOCK_STREAM`; the server sees the client's effective UID and
  group list as captured at the client's `connect(2)`, and the client sees the server's as captured
  at `listen(2)` [35]. `unix(4)` calls the mechanism reliable because a peer can only influence it
  by performing that call under different effective credentials [35].
- `getpeereid(3)` returns a Unix-domain peer's effective UID and GID; it requires a `SOCK_STREAM`
  socket on which `connect` or `listen` has happened and returns `EINVAL` otherwise [40].
- `LOCAL_PEERPID` is `0x002`, `LOCAL_PEEREPID` `0x003`, `LOCAL_PEERUUID` `0x004`,
  `LOCAL_PEEREUUID` `0x005`, and `LOCAL_PEERTOKEN` `0x006`, the last labelled "retrieve peer audit
  token" in the public header [36]. `[inference]` These are implementation interfaces exposed by the
  header rather than part of the `unix(4)` credential contract, so availability and payload layout
  should be treated as Darwin-version-sensitive [35][36].

Apple Developer Technical Support advises against PID as a security identity because the PID space
is small and PIDs are commonly reused, and identifies `audit_token_t` as the recommended
alternative, validatable with `kSecGuestAttributeAudit` [52]. For XPC specifically, Apple added
`xpc_connection_set_peer_code_signing_requirement` in macOS 12.0 [50]; DTS reports
`SecCodeCreateWithXPCMessage` as the macOS 11 route, `NSXPCConnection setCodeSigningRequirement:`
arriving in macOS 13, and `xpc_connection_set_peer_lightweight_code_requirement` in macOS 14.4
[51].

### 2.3 Mach ports

A Mach port is a secure simplex communication channel accessed through send and receive
capabilities called port rights, and a task's key Mach resources are mediated through its port-right
namespace [48]. Mach distinguishes send, receive and send-once rights; a typical message-queue port
has exactly one receive-right holder and possibly many send-right holders, and a reply needs a
second port because each endpoint is unidirectional [48]. Port-right ownership is Mach's
fundamental security mechanism: holding a send right is a capability, and rights can be copied or
moved through Mach IPC, which transfers that capability [48]. Port names are 32-bit indices local
to a task, not systemwide names [48]. A port set lets one receive operation wait on any member port
[48].

A Mach message may include pure data, memory-range copies, port rights, and kernel-supplied
implicit attributes such as the sender's security token; transfer is asynchronous and the receiver
sees a logical copy that may be copy-on-write optimised [48]. A named memory entry is a port
denoting a virtual-memory handle; possession permits mapping the backing VM object or passing on
the right to map it, and mapping one entry in two tasks creates a shared-memory window [48].
Darwin's copy-on-write means read-only sharing is protected until a writer faults and copies the
changed portion [48]. `[inference]` Memory-entry transfer is therefore the Apple-native
capability-based route to large shared buffers, avoiding a bytewise copy once the peer maps the
entry [48].

Service discovery goes through the bootstrap server: Apple assigns Mach bootstrap the job of
looking up requested Mach ports, and documents `launchd` as the kernel-launched process that
handles those lookups [48]. DTS describes a daemon checking in with
`bootstrap_check_in(..., name, ...)` and a client obtaining the port with `bootstrap_look_up` [53].
Apple's 2013 Mach overview identifies `mach_ipc` and `mach_msg` as raw port APIs and calls
`mach_msg` legacy [48]; `[uncertain]` that guidance predates `mach_msg2`, so it should not be read
as discouraging `mach_msg2` specifically [48]. MIG generates procedural interfaces over the message
APIs from interface descriptions [48].

`[uncertain]` No authoritative universal numeric limit on inline Mach message size was located;
budget dynamically and use out-of-line or named-memory transfer for bulk data rather than a
folklore constant [48].

### 2.4 XPC

Apple describes XPC Services as a lightweight IPC mechanism integrated with GCD and launchd [49].
They are launchd-managed: launched on demand, restarted after a crash, and `SIGKILL`ed while idle
[49]. By default an XPC service is sandboxed with minimal filesystem and network access [49]. A
bundled XPC service is a bundle in `Contents/XPCServices` of its main app bundle and Apple's
archived guide describes it as private to that containing app [49]. A C XPC service installs its
handler with `xpc_main`; the C API lives in libSystem and uses its own IPC-object container rather
than Core Foundation property lists, and unlike `CFPropertyList` it supports file descriptors in its
object graphs [49].

An XPC connection is a virtual endpoint independent of whether a service instance is running, and
the service binary launches on demand for that endpoint [49]. Connections themselves can be sent
inside XPC messages to introduce services to each other [49]. `NSXPCConnection` is Foundation's
Objective-C RPC API; both it and XPC Services have existed since OS X 10.8, methods must return
`void` with replies delivered through one reply block, permitted argument types are arithmetic
types, `BOOL`, C strings, C structures and arrays of permitted primitives, and `NSSecureCoding`
objects, and `NSXPCInterface` describes the expected methods and classes [49]. An `NSXPCListener`
delegate receives a new connection on its first message and may accept or reject it, and `resume`
is required before messages flow [49]. Interruption means the peer crashed or closed and the
connection can normally make future requests or relaunch a helper; invalidation means the local
connection is torn down and must be recreated [49]. Apple states that XPC's encoding and channel
are opaque and must not be interacted with directly [49].

DTS says the exact inline XPC cutoff is undocumented and estimated at just under 16 KiB, and
distinguishes implicit sharing by sending an XPC data object from explicit shared memory; it notes
`DispatchData` might avoid a copy across XPC as non-guaranteed advice [54].

`[inference]` An arbitrary unrelated process cannot simply open an XPC connection to a bundled
service: Apple defines bundled services as private to the containing app, and daemon-style
reachability is launchd Mach-service registration [49][53].

### 2.5 Shared memory on macOS

`shm_open()` opens a named POSIX shared-memory object and returns a new descriptor; reopening a
non-unlinked name yields descriptors to the same object, and `O_CREAT|O_EXCL` detects collisions
with `EEXIST` [41]. Darwin sets `FD_CLOEXEC` on the returned descriptor [41]. Darwin POSIX-SHM
objects have **no visible filesystem entry** [41], so `/dev/shm`-style discovery does not exist
[inference, from 41]. An object persists until unlinked with all references gone, and does not
survive reboot [41].

The name limit is severe: `PSHMNAMLEN` is exactly 31 in XNU, which stores names in
`pshm_name[PSHMNAMLEN + 1]`, and `shm_open` reports `ENAMETOOLONG` beyond it, with the man page
cautioning that the value may change [41][42]. `[uncertain]` The frequently repeated "`ftruncate`
may only be called once" rule was not established by the inspected man page or source; it must be
tested on the deployment OS [41].

System V shared memory exists through `shmget(2)`: it creates a segment for `IPC_PRIVATE` or for a
missing key with `IPC_CREAT`, initialises creator and owner IDs from the caller's effective IDs,
reports `EEXIST` for `IPC_CREAT|IPC_EXCL` on an existing key, and reports `ENOSPC` at the system
identifier limit [43]. `[uncertain]` No primary source was found for a stable default
`kern.sysv.shmmax`; operators must query the target release [43].

### 2.6 Cross-process synchronisation on macOS

Mach semaphores are counting semaphores that retain posts when no waiter exists and support wait,
post and post-all [48]. Apple documents `os_sync_wait_on_address` as an atomic compare-and-wait
primitive for building higher-level synchronisation [56]; `[uncertain]` the accessible API metadata
did not confirm the commonly cited macOS 14.4 introduction, so that version should not be stated as
sourced [56]. `[uncertain]` Apple's current documentation does not support a blanket claim that
process-shared pthread condition variables are unsupported, but an Apple forum report on macOS 11.6
describes problematic multi-waiter behaviour, so the exact primitive must be tested per OS [65].
`[uncertain]` `sem_open` was not shown to be formally deprecated by the accessible Apple material;
its sandbox restriction is documented but "deprecated" is not [62].

### 2.7 Readiness, pipes, loopback

Darwin provides `kqueue`/`kevent`; there is no `epoll` and no io_uring. The current XNU event header
defines `EVFILT_MACHPORT` as `-8` [47], and `kevent(2)` documents it as waiting for a queued message
on the named Mach port or port set [46]. GCD wraps this: `DispatchSourceRead` and
`DispatchSourceWrite` monitor readable and writable descriptors including Unix sockets and pipes,
`DispatchSourceMachReceive` monitors a Mach port for pending messages, and `DispatchSourceMachSend`
monitors dead-name notifications that signal a send right has lost its receive right [55].
`[inference]` The documented macOS event path is therefore kqueue plus GCD dispatch sources
[46][47][55].

`pipe(2)` creates a unidirectional descriptor pair that persists until all associated descriptors
close, and closing the write end is how a reader sees EOF after buffered bytes drain [39]. XNU's
normal pipe capacity macro `PIPE_SIZE` is 16384 and `BIG_PIPE_SIZE` is 65536, with `PIPE_MINDIRECT`
at 8192 required to sit between `PIPE_BUF` and `PIPE_SIZE` [44]. Darwin's `PIPE_BUF` is 512 bytes
[45] — an eighth of the Linux value, which matters for any design that relies on atomic small
writes. `[inference]` These macros describe kernel configuration capacities, not a promise that
every pipe auto-grows in every release [44].

On loopback, an Apple engineer's forum answer states that macOS local-network privacy does not
classify `127.0.0.1` or `::1` as local network, and that the restriction covers multicast,
broadcast and unicast to local-subnet addresses [59]. `[uncertain]` That is a forum answer rather
than a formal specification, and it is the best direct Apple evidence located that loopback does not
trigger the local-network prompt [59].

### 2.8 Crash and cleanup semantics (macOS)

A pathname socket file survives process death and must be unlinked [35]. A POSIX-SHM name survives
until `shm_unlink`, but is not discoverable through the filesystem [41]. A System V segment survives
until explicitly removed [43]. Pipes vanish when all descriptors close [39]. An XPC service is
restarted by launchd after a crash, and its clients observe interruption rather than invalidation
[49] — which makes XPC the only mechanism on any of the three platforms with built-in supervised
restart semantics.

## 3. Windows

### 3.1 Named pipes: API and namespace

`CreateNamedPipeW` creates one server-side instance; the first call establishes the pipe's basic
attributes and later calls create further instances of the same pipe [68]. Parameters are the name,
open mode, pipe mode, `nMaxInstances`, output and input buffer reservations, a default wait timeout,
and optional `SECURITY_ATTRIBUTES` [68].

The documented local spelling is `\\.\pipe\pipename` [68]. The `pipename` component may contain any
character except backslash, so the namespace is flat, and the whole name string is limited to 256
characters [68]; names are case-insensitive [68]. Remote access over SMB is the default
(`PIPE_ACCEPT_REMOTE_CLIENTS`, value 0) subject to the security descriptor;
`PIPE_REJECT_REMOTE_CLIENTS` (`0x8`) rejects remote clients automatically and is what a local-only
endpoint should set [68].

**Direction.** `PIPE_ACCESS_DUPLEX` (`0x3`) gives the server read/write access and lets a client
request read, write or both; `PIPE_ACCESS_INBOUND` (`0x1`) is client-to-server and
`PIPE_ACCESS_OUTBOUND` (`0x2`) server-to-client [68]. The access mode must be identical across all
instances of a pipe [68].

**Message versus byte mode.** `PIPE_TYPE_BYTE` (0) treats data as an undifferentiated byte stream
and cannot be combined with `PIPE_READMODE_MESSAGE`; `PIPE_TYPE_MESSAGE` (`0x4`) makes each
`WriteFile` a message unit [68][69]. The system always performs writes on message-type pipes as if
write-through were enabled [69]. A message-type pipe may be read in byte-read or message-read mode,
and the read mode may differ between server and client handles to the same instance [69]. In
message-read mode a read completes only when the whole message is read; an undersized buffer reads
as much as possible and returns with `GetLastError()` reporting `ERROR_MORE_DATA`, and the remainder
is read by another read [69]. A client handle from `CreateFile` always starts in byte-read mode and
changes with `SetNamedPipeHandleState`, for which the handle needs `FILE_WRITE_ATTRIBUTES` [69].
**This is the property that makes named pipes the closest Windows equivalent of `SOCK_SEQPACKET`.**

**Wait mode.** `PIPE_WAIT` (0) is blocking; `PIPE_NOWAIT` (`0x1`) returns immediately, giving
`ERROR_NO_DATA` on an empty read and `ERROR_PIPE_LISTENING` on a connect with nobody waiting
[68][69]. Microsoft states that `PIPE_NOWAIT` exists for LAN Manager 2.0 compatibility and must not
be used to obtain asynchronous pipe I/O; `FILE_FLAG_OVERLAPPED` is the mechanism for that [68][69].

**Instances and buffers.** `nMaxInstances` must be between 1 and `PIPE_UNLIMITED_INSTANCES` (255)
and must match across instances; a higher value fails `ERROR_INVALID_PARAMETER`, and
`PIPE_UNLIMITED_INSTANCES` means the count is bounded by system resources rather than a fixed cap
[68]. Buffer sizes are advisory: Windows applies a system default, minimum or maximum, or rounds up
to an allocation boundary; buffers and request bookkeeping consume nonpaged pool, and undersizing
can block a write while the system expands the buffer [68]. There is consequently no documented
fixed maximum message size in message mode — the bound is nonpaged pool and write quota, not a
protocol number [68]. `nDefaultTimeOut` applies when `WaitNamedPipe` requests
`NMPWAIT_USE_DEFAULT_WAIT`, and zero means 50 ms [68].

**Connection lifecycle.** With a blocking handle `ConnectNamedPipe` waits for a client [70]. With an
overlapped handle it requires a non-NULL valid `OVERLAPPED`, since NULL can incorrectly report
completion, and a pending connect returns false with `ERROR_IO_PENDING` [70]. A client can connect
between `CreateNamedPipe` and `ConnectNamedPipe`, in which case `ConnectNamedPipe` returns false
with `ERROR_PIPE_CONNECTED` and the connection is nevertheless good — a race every server loop must
handle [70]. A reused instance must be `DisconnectNamedPipe`d before reconnecting, otherwise
`ConnectNamedPipe` returns `ERROR_NO_DATA` or `ERROR_PIPE_CONNECTED` [70]. When all instances are
busy the client's `CreateFile` fails with `ERROR_PIPE_BUSY`, after which it can call
`WaitNamedPipe` — which waits for an instance with a pending `ConnectNamedPipe` — and retry
[71][72]. The documented accept pattern is to create enough instances, issue `ConnectNamedPipe` on
each, then process and recycle each instance [73].

**Operations and shutdown.** `PeekNamedPipe` reads available data without removing it and also
returns instance information, for both byte and message pipes [73]. `TransactNamedPipe` writes a
request and reads a reply in one operation on a message-type duplex pipe whose caller handle is in
message-read mode, documented as improving network performance for request/reply exchanges [73].
The documented orderly shutdown is `FlushFileBuffers` — which waits until the client has read all
queued bytes or messages — then `DisconnectNamedPipe`, then close or reuse; disconnect discards
unread data and invalidates the still-open client handle [73].

**Lifetime.** Named-pipe instances are deleted when their last handle closes, including when the
owning process terminates [68]. **Windows named pipes therefore have no stale-endpoint problem at
all** — the single most important operational difference from `AF_UNIX` pathname sockets.

`FILE_FLAG_FIRST_PIPE_INSTANCE` makes a second creation attempt with the flag fail
`ERROR_ACCESS_DENIED` [68]. `[inference]` Creating the first instance with that flag and checking
success before trusting the endpoint detects name squatting for that pipe name [68].

### 3.2 Named-pipe security

`lpSecurityAttributes` supplies the new pipe's security descriptor and controls inheritance of the
server handle; if NULL, the handle is not inheritable and the pipe gets the default descriptor [68].
That default grants full control to LocalSystem, Administrators and Creator Owner, **and read
access to Everyone and the anonymous account** — so it is not a same-user-only policy and a local
daemon must set an explicit DACL [68].

Client `CreateFile` and `CallNamedPipe` are access-checked against the pipe DACL, and a server
creating another instance needs `FILE_CREATE_PIPE_INSTANCE` as well as the applicable DACL rights
[74]. `FILE_GENERIC_WRITE` includes `FILE_APPEND_DATA`, which aliases `FILE_CREATE_PIPE_INSTANCE`,
so Microsoft recommends naming individual rights when that consequence is unwanted [74]. Microsoft
recommends putting the logon SID on the DACL to exclude remote users and users in other Terminal
Services sessions [74].

Two security-descriptor states must not be conflated: a NULL DACL — `pDacl == NULL` with a DACL
present — "allows all access", while an empty DACL has no ACEs, grants no rights and denies access
implicitly [77].

### 3.3 Impersonation risk

`ImpersonateNamedPipeClient` lets only the server end impersonate the security context of the
client that sent the last message read, and the server must call `RevertToSelf` afterwards [75][76].
If the call fails and a privileged server nevertheless services the request, that request runs under
the server's identity; Microsoft says to always check the return value and execute no client request
on failure [75]. Impersonation is permitted when the requested level is below
`SecurityImpersonation`, when the caller holds `SeImpersonatePrivilege`, when the token came from
explicit `LogonUser`/`LsaLogonUser` credentials in the caller's logon session, or when client and
caller are the same authenticated identity [75]. `SeImpersonatePrivilege` did not exist on Windows
XP SP1 and earlier [75].

The default server impersonation level is `SecurityImpersonation`, so a client can supply
`SECURITY_SQOS_PRESENT` at `CreateFile` and deliberately request `SECURITY_IDENTIFICATION` or
`SECURITY_ANONYMOUS` if it must prevent usable impersonation [76]. `[inference]` A privileged client
that connects to an attacker-controlled pipe name can be impersonated by that pipe server under the
documented default; unpredictable names, `FILE_FLAG_FIRST_PIPE_INSTANCE` and client SQOS are
complementary mitigations [68][76].

### 3.4 Identifying the client

`GetNamedPipeClientProcessId` retrieves a connected client's PID from a server-created handle, with
documented minimum Windows Vista and Windows Server 2008 [78]. `GetNamedPipeClientComputerName` and
`GetNamedPipeServerProcessId` have the same documented minima [79][80]. `[inference]` A PID is an
observation, not authentication, because Windows can reuse a PID after process exit; authorisation
should rest on the impersonation token and SID together with ACL policy [75][76]. `[inference]`
Re-opening a process by PID to inspect its image with `QueryFullProcessImageName` adds a
time-of-check/time-of-use race, so a real process handle should be retained where process identity
genuinely matters [78].

### 3.5 Overlapped I/O and completion ports

`FILE_FLAG_OVERLAPPED` allows read, write, connect and transaction operations to return while work
continues in the background; without it those operations are synchronous [68]. A pending `ReadFile`,
`WriteFile` or `ConnectNamedPipe` returns `ERROR_IO_PENDING` and the outcome is retrieved with
`GetOverlappedResult` [81]. Microsoft's own overlapped server example avoids simultaneous
operations on one instance when a single event serves read, write and connect, because the signalled
event cannot identify which operation completed [81].

`CreateIoCompletionPort` builds the completion port that `GetQueuedCompletionStatusEx` drains,
dequeuing multiple completed entries per call, each reporting bytes transferred, completion key and
the original `OVERLAPPED` address [82]. This is a **completion**-based model, not a readiness model:
the application posts an operation and receives an entry when it finishes [82] — the opposite of
`epoll`/`kqueue` and the reason a portable abstraction has to be written against completion rather
than readiness. `SetFileCompletionNotificationModes` supports
`FILE_SKIP_COMPLETION_PORT_ON_SUCCESS` since Windows Vista and Server 2008, which suppresses the
queued completion for an immediately successful request on a port-associated handle; notification
modes cannot be removed once set [84].

`CancelIoEx(handle, NULL)` requests cancellation of all outstanding I/O on that handle in the
current process, and a non-NULL `OVERLAPPED` selects specific requests [83]. The `OVERLAPPED` must
not be freed or reused until the operation actually completes, cancellation can race with normal
completion so the real result may be success, `ERROR_OPERATION_ABORTED` or another error, and
pending asynchronous cancellation on an IOCP-associated handle queues a completion packet [83].
`WaitForMultipleObjects` is limited to `MAXIMUM_WAIT_OBJECTS`, above which thread-pool waits or
grouped wait threads are required [85].

### 3.6 AF_UNIX on Windows

Microsoft announced native `AF_UNIX` support on 2017-12-19, beginning in Windows Insider Build
17063, enabling `AF_UNIX` communication between two Win32 processes through Winsock [86]. The
current Win32 IPC index still says only "Beginning in Windows Insider Build 17063" [87].
`[uncertain]` **No dated Microsoft statement was found that names Windows 10 version 1803 (build
17134) or Windows Server 2019 as the GA release**, although third-party documentation asserts it:
JEP 380 states that Windows 10 and Windows Server 2019 support Unix-domain sockets [152], and
Microsoft's ASP.NET Core gRPC guidance calls Unix sockets usable on Windows 10 and Windows Server
2019 and later [156].

What the 2017 announcement documents [86]:

- Only connection-oriented `SOCK_STREAM` is supported. `SOCK_DGRAM` and `SOCK_SEQPACKET` are listed
  as unavailable, with datagram support only a possible future consideration.
- Ancillary data is unsupported, explicitly including `SCM_RIGHTS` descriptor passing and credential
  ancillary data. Neither `SCM_RIGHTS` nor `SCM_CREDENTIALS` compatibility may be assumed.
- Winsock 2.0 `socketpair` is unsupported.
- Addresses use `sockaddr_un` from `afunix.h`, with `sun_path` a NUL-terminated UTF-8 Win32
  filesystem path. `UNIX_PATH_MAX` is exactly 108 in the SDK header, and `sun_path` is
  `char[UNIX_PATH_MAX]` — a header constant, unrelated to `MAX_PATH` [88].
- The node created by `bind` is a custom NTFS reparse point. Binding requires directory write
  permission and connecting to a stream socket requires write permission on the socket file. The
  socket file must be removed with `DeleteFile` before rebinding the same pathname — so Windows
  `AF_UNIX` reintroduces exactly the stale-endpoint problem that named pipes do not have.
- `AF_UNIX` and WSL Unix sockets did not interoperate at that time.
- Abstract addresses were accepted but auto-bind for them was unsupported; a later 2020 comment on
  the post points at a WSL issue reporting abstract sockets nonfunctional. Abstract-namespace
  portability on Windows must be treated as uncertain.

`[uncertain]` **No current primary source was found** establishing present-day `SOCK_DGRAM` support,
`SO_PEERCRED`/`getpeereid` support, or `SCM_RIGHTS` support, nor a newer dated Microsoft statement
that they remain unsupported [86][87]. `[uncertain]` No Microsoft support statement for `AF_UNIX`
inside UWP/AppContainer was found [86][103]. On Windows containers, Microsoft's AKS HostProcess
documentation states that named-pipe mounts and Unix-domain sockets are not directly supported
although host paths such as `\\.\pipe\*` can be accessed, so `npipe://` is a Docker/Moby named-pipe
bind-mount mechanism and there is **no primary source** for a Unix-socket equivalent [111].

### 3.7 Shared memory: file mappings

`CreateFileMappingW(INVALID_HANDLE_VALUE, ..., size, name)` creates a paging-file-backed section and
requires both maximum-size fields to specify a size [89]. A NULL `SECURITY_ATTRIBUTES` produces a
non-inheritable handle and a default descriptor derived from the creator's primary or impersonation
token [89].

Names may carry a `Global\` or `Local\` prefix and the remainder may not contain backslashes [89].
Windows provides a global namespace plus separate per-session namespaces for named events,
semaphores, mutexes, waitable timers, file mappings, jobs and symbolic links [90]. Processes in a
client session default to their session namespace while services default to global; `Global\`
selects the global namespace explicitly, `Local\` selects the caller's session namespace, and both
keywords are case-sensitive [90]. Creating a global file mapping outside session 0 requires enabled
`SeCreateGlobalPrivilege`, though opening an existing global mapping does not trigger that check
[89]; the privilege was introduced in Windows 2000 SP4, and without it users can still create
session-specific objects [104].

`CreateFileMapping` creates the section object without mapping it; `MapViewOfFile`/`MapViewOfFileEx`
map views [89]. A mapping can be shared through inherited handles, `DuplicateHandle`, or
`OpenFileMapping`, each subject to handle and object security [89]. Mapped views hold an internal
reference, so complete destruction requires unmapping every view and closing every handle [89].
`[inference]` Because process termination closes handles and tears down the address space, an
unnamed paging-file-backed section with no remaining handles or views is cleaned up automatically —
unlike a POSIX `shm_open` name, which persists [89]. `SEC_LARGE_PAGES` applies only to
paging-file-backed mappings, requires `SEC_COMMIT`, a multiple of `GetLargePageMinimum`, and enabled
`SeLockMemoryPrivilege` [89].

### 3.8 Cross-process synchronisation on Windows

Multiple processes can share a named event, mutex, semaphore or waitable timer through its name,
handle inheritance or duplication [91]. `WaitForMultipleObjects` accepts at most
`MAXIMUM_WAIT_OBJECTS` handles [85]. If a mutex is abandoned, the wait returns in the
`WAIT_ABANDONED_0` range and grants ownership to the waiter, and Microsoft states that persistent
state protected by it should be checked for consistency [85] — making abandonment a crash *signal*,
not proof that the shared state is valid [85].

**Correction to a common assumption.** Microsoft documents `WaitOnAddress`/`WakeByAddressSingle` as
waking only threads **in the same process** [92]. They are not a cross-process futex substitute;
named events or mutexes are the documented interprocess primitives [91].

### 3.9 Mailslots

A mailslot is an in-memory pseudo-file accessed through ordinary file functions, whose data is
temporary: closing all handles deletes the mailslot and its data [93]. Only the creator, or a
process that inherited or otherwise obtained the server handle, can read one; any process that knows
the name can write to it, so the name is not a credential [93]. Mailslots can broadcast within a
domain when recipients create the same name, and a network mailslot message cannot exceed 424 bytes,
for which Microsoft recommends named pipes or Windows Sockets instead [93]. Microsoft began
disabling the **Remote** Mailslot protocol by default as of Windows 11 Insider Preview Build 25314
and Windows Server Preview Build 25314, calling it "a precursor to deprecation and eventual
removal", announced 2023-03-15 [94]; that is not evidence that the local mailslot API has been
removed. Mailslots are unsuitable for either role here: unreliable, one-way, and without peer
identity.

### 3.10 ALPC and local RPC

Microsoft publicly supports local MS-RPC through the `ncalrpc` protocol sequence, registered with
`RpcServerUseProtseqEp` [95], and says servers expecting local calls should use `ncalrpc` [96].
`RpcServerUseProtseqEp` accepts a security descriptor only for `ncacn_np` and `ncalrpc`, and
Microsoft says endpoint-descriptor security alone is not a recommended way to secure a server [95].
Microsoft's archived debugger post says Vista introduced ALPC and redirects legacy LPC calls,
identifying RPC `ncalrpc` as the Win32 route to LPC/ALPC [97]. The `NtAlpc*` APIs are not covered by
the public Win32 SDK documentation consulted; direct use and reverse-engineered ABI claims must be
treated as undocumented [97].

### 3.11 Loopback on Windows

Windows Vista and Windows Server 2008 changed the default dynamic TCP client-port range to
49152–65535, replacing 1025–5000; it is inspectable with `netsh int ipv4 show dynamicport tcp` [98].
That range concerns ephemeral source ports, not a required firewall opening [98].
`SIO_LOOPBACK_FAST_PATH` is TCP-only and must be enabled on both ends of the loopback session [99].
`[uncertain]` No primary Microsoft source was obtained for a universal claim that loopback TCP never
requires firewall rules, nor for third-party WFP interference; host policy can still affect local
networking and must be tested in the target environment.

Packaged applications are the exception: they block loopback IPC by default for network isolation
[100]. Packaged peers require the `privateNetworkClientServer` capability and mutual loopback access
rules, and sideload or debug scenarios can use `CheckNetIsolation LoopbackExempt` [100].
Inbound-loopback support in `CheckNetIsolation ... -is` was introduced in Windows 10 version 1607
(build 14393) [100]. See section 6.

### 3.12 Crash and cleanup semantics (Windows)

Pipe instances disappear when the last handle closes [68]. Section objects disappear once all
handles and views are released [89]. Named kernel synchronisation objects disappear at last handle
close [91]. Mailslots disappear when all handles close [93]. A pathname `AF_UNIX` socket file created
by `bind` requires explicit `DeleteFile` before rebinding [86]. Windows is thus the only one of the
three platforms whose *native* local transport needs no crash cleanup at all.

## 4. Cross-platform comparison table

| Property | Linux | macOS | Windows |
| --- | --- | --- | --- |
| Default local transport | `AF_UNIX` `SOCK_STREAM` [1] | `AF_LOCAL` `SOCK_STREAM` [35] | Named pipe [68] |
| Message-framed connected type | `SOCK_SEQPACKET` since 2.6.4 [1] | none; XNU lists it `TODO` [37] | `PIPE_TYPE_MESSAGE` [69] |
| Reliable ordered datagram | `SOCK_DGRAM` [1] | `SOCK_DGRAM`, documented unreliable [34] | `AF_UNIX` datagram unsupported [86]; mailslots unreliable [93] |
| Path length | `sun_path[108]`, 107 usable [1] | `sun_path[104]` [36] | pipe name <=256 chars [68]; `UNIX_PATH_MAX` 108 [88] |
| Abstract / filesystem-free namespace | yes, Linux-only, netns-scoped [1][17] | no [inference, 35, 36] | accepted but auto-bind unsupported and reported nonfunctional [86] |
| Descriptor / handle passing | `SCM_RIGHTS`, <=253 fds [1] | `SCM_RIGHTS`, `UIPC_MAX_CMSG_FD` 512 [35][37] | no `SCM_RIGHTS` [86]; `DuplicateHandle` instead [89] |
| Peer credentials | `SO_PEERCRED`, `SO_PEERGROUPS` (4.18), `SO_PEERPIDFD` (6.5), `SO_PEERSEC` (2.6.2) [1][3][28][29] | `LOCAL_PEERCRED`/`getpeereid` (no PID), `LOCAL_PEERTOKEN` audit token [35][36][40] | `ImpersonateNamedPipeClient` token; `GetNamedPipeClientProcessId` (Vista) [75][78] |
| Endpoint permissions | socket-file mode + directory mode [1]; abstract has none [1] | socket-file mode + directory mode [35] | security descriptor / DACL [74] |
| Default endpoint permissions | umask-derived, all bits minus umask [1] | umask-derived [35] | NULL SD grants Everyone read [68] |
| Stale endpoint after crash | yes for pathname, no for abstract [1] | yes [35] | no for pipes [68]; yes for `AF_UNIX` [86] |
| Shared memory | memfd + seals [6][7], POSIX `/dev/shm` [8], SysV [9] | POSIX SHM, name <=31 chars, no fs entry [41][42]; Mach memory entries [48] | named/unnamed file mappings, `Global\` needs privilege [89][90] |
| Cross-process wake | futex on `MAP_SHARED` [11], eventfd [12] | Mach semaphores [48], `os_sync_wait_on_address` [56] | named events/mutexes [91]; **not** `WaitOnAddress` [92] |
| Readiness model | `epoll` readiness [13]; io_uring completion [14] | `kqueue` readiness, GCD sources [46][55] | IOCP completion [82] |
| Supervised on-demand start | systemd socket activation [129][130] | launchd XPC / Mach services [49] | no equivalent documented here |
| Loopback available by default | yes, netns-scoped [17] | yes; not treated as local network [59] | yes, except packaged apps [100] |

## 5. What established systems chose

**libzmq.** `zmq_ipc(7)` says the inter-process transport is "currently only implemented on
operating systems that provide UNIX domain sockets", addresses are pathnames, and a wild-card `*`
makes `zmq_bind()` generate a unique temporary pathname retrievable through `ZMQ_LAST_ENDPOINT`
[112]. Two behaviours are unusual: any existing binding to the same endpoint is overridden, so a
second process binding an already-bound endpoint succeeds and the first loses its binding —
explicitly inconsistent with `tcp` and `inproc`, and intended to let a process recover after a
crash [112][116]. On Linux only, an endpoint whose pathname starts with `@` uses the abstract
namespace, where a duplicate bind does fail [112]. The documented maximum is 113 characters
including the `ipc://` prefix, 107 for the real path [112]. Windows support arrived via `AF_UNIX`,
not named pipes: libzmq 4.3.3, released 2020-09-07, records "Fixed #3691 — added support for IPC on
Windows 10 via AF_UNIX" and "Fixed #1808 — use AF_UNIX instead of TCP for the internal socket on
Windows 10" [114]. The 4.3.4 release notes (2021-01-17) record "Fixed #4086 — excessive amount of
socket files left behind in Windows TMP directory", which is the stale-file consequence of that
choice [114].

**nanomsg / NNG.** `nng_ipc(7)` states plainly: POSIX platforms use Unix domain sockets, Windows
uses Windows Named Pipes [117]. On Windows all names are prefixed with `\\.\pipe\` and do not live
in the filesystem; on POSIX the path is literal and relative unless it begins with `/`, and absolute
paths are recommended because relative ones are interpreted per working directory [117]. The
`unix://` scheme is a POSIX-only alias for `ipc://`, reserved so that a future `AF_UNIX`-based
Windows transport can be distinguished from the named-pipe one [117]. Legacy nanomsg compatibility
caps pathnames at 122 bytes including the NUL, because legacy nanomsg cannot express URLs longer
than 128 bytes including the `ipc://` prefix [117]. On Linux, abstract sockets are supported through
URI-encoded names (`abstract://a%00b`), an empty listener name requests auto-bind, abstract sockets
ignore socket permissions but still permit `NNG_OPT_PEER_UID` and friends, and they are freed
automatically by the system [117]. `NNG_OPT_IPC_PERMISSIONS` is POSIX-only and NNG recommends a
server-writable, client-searchable directory rather than relying on socket mode alone;
`NNG_OPT_IPC_SECURITY_DESCRIPTOR` configures the Windows pipe's `PSECURITY_DESCRIPTOR` before
listener start; and NNG warns that peer PID can change if a descriptor is passed between processes
[119].

**gRPC.** `doc/naming.md` defines `unix:path` and `unix:///absolute_path` as Unix-only Unix-domain
targets, and `unix-abstract:abstract_path` as a Unix-only abstract socket for which the
implementation prepends the NUL byte, the caller must not, the name is unrelated to the filesystem,
**and no permissions apply so any user or process may access it** [120]. It also defines Linux-only
`vsock:cid:port` [120]. Windows `AF_UNIX` support in gRPC core is a request: issue #22285, opened
2020-03-10, asks for Windows `AF_UNIX`/create-from-fd support [121], and issue #13447, opened
2017-11-17, asks for a Windows named-pipe transport [122]; the continued existence of the latter is
primary evidence that core gRPC did not then provide that transport, which should not be generalised
to every gRPC language implementation [122].

**Chromium Mojo.** `PlatformChannel` is documented as an abstraction over a platform-specific IPC
FIFO primitive for Mojo Invitations; construction creates two transferable `PlatformHandle`s, one
intended for transfer to another process [123]. The platform API names Unix-domain sockets and
Windows named pipes as examples of the stable primitives it abstracts, and provides
`NamedPlatformChannel` for the case where a `PlatformHandle` cannot be transferred between
already-running processes, with a named pipe on Windows and a domain socket on POSIX [123]. Mojo
Core's overview says a Node normally corresponds to a process, transports messages between Ports,
can carry ports and platform handles including file descriptors, Mach ports and Windows handles, and
that Node communication uses platform channels such as `AF_UNIX` sockets, pipes or Fuchsia channels
[124]. `[uncertain]` No single primary Chromium document was found stating the exact per-platform
mapping (Linux `socketpair`, macOS Mach ports, Windows `DuplicateHandle` via a broker) or the
sandboxed-renderer broker requirement, so those must not be elevated from the generic docs
[123][124].

**D-Bus.** The specification documents `unix:tmpdir=` and `unix:dir=` as listen-only forms whose
connectable address is emitted as either `unix:path` or `unix:abstract`, and notes that a
connectable address such as `unixexec:` is not necessarily listenable [125]. `nonce-tcp` is TCP plus
a simple authentication step intended to ensure that only clients with read access to a filesystem
location can connect: the server publishes a nonce-file path, and after connecting the client must
read the nonce and send it over the socket [125] — the canonical workaround for a platform without
peer credentials. `dbus-daemon(1)` recommends allowing only `EXTERNAL` on non-Windows platforms and
says that is the default for the well-known system and session buses [126]. If
`DBUS_SESSION_BUS_ADDRESS` is unset, clients normally invoke `dbus-launch --autolaunch` to reuse a
session found from the X display or `~/.dbus/session-bus/`, or start one [127]. `[uncertain]` No
dated specification version enumerating every address family, and no authoritative dbus-daemon
Windows TCP/autolaunch document, was located [125].

**systemd.** `sd_listen_fds(3)` defines `SD_LISTEN_FDS_START` as 3 and returns received descriptors
beginning at descriptor 3 in sequence [129]. The socket-activation environment is `LISTEN_FDS`,
`LISTEN_PID`, `LISTEN_PIDFDID` and `LISTEN_FDNAMES`, removable via `unset_environment`;
`sd_listen_fds_with_names()` reads colon-separated names from `LISTEN_FDNAMES` and was added in
systemd 227 [129]. systemd recommends validating passed descriptors with `sd_is_fifo`,
`sd_is_socket`, `sd_is_socket_inet` or `sd_is_socket_unix`, with checks loose enough to permit
legitimate configuration [129]. `systemd.socket(5)` maps `ListenStream=`, `ListenDatagram=` and
`ListenSequentialPacket=` onto `SOCK_STREAM`, `SOCK_DGRAM` and `SOCK_SEQPACKET`; a `/` path is a
filesystem `AF_UNIX` socket and an `@` prefix is an abstract one with `@` replaced by NUL before
bind; `ListenSequentialPacket=` is `AF_UNIX`-only; and a socket unit may specify several addresses,
all of which are passed to the activated service [130]. `sd_notify(3)` says a `NOTIFY_SOCKET`
starting with `/` is an `AF_UNIX` socket and one beginning with `@` is abstract, and that the
datagram carries sender credentials via `SCM_CREDENTIALS` [128]. `[uncertain]` The systemd release
that introduced the varlink interfaces was not established from a primary source in this research.

**Docker, Podman, containerd, Kubernetes.** `dockerd`'s regular Linux default is a Unix socket at
`/var/run/docker.sock`, access to which requires root permission or docker-group membership [131].
The Windows client default endpoint is `npipe:////./pipe/docker_engine`, while Linux and macOS use
`unix:///var/run/docker.sock` [132]. Docker Desktop documents that non-privileged named pipes are
accessible only to the launching user, local Administrators and `LOCALSYSTEM` [134]. Docker Engine
29.5 introduced optional Windows `AF_UNIX` listening while keeping `npipe` as the Windows default; a
configured Unix socket without `--group` is limited to Administrators and `SYSTEM` [131]. Docker's
"Protect the Docker daemon socket" page notes that Docker normally runs through a non-networked Unix
socket and warns that anyone holding a client TLS key can instruct the daemon and thereby gets root
access to the daemon host [133]. Podman documents rootless systemd activation at
`ListenStream=%t/podman/podman.sock` where `%t` expands to `XDG_RUNTIME_DIR`, a direct rootless
default of `unix://$XDG_RUNTIME_DIR/podman/podman.sock`, and a rootful default of
`unix:///run/podman/podman.sock`; its security model rests on Unix-socket filesystem permissions and
it explicitly says the API grants full Podman functionality and hence arbitrary code execution as
the service user [135]. containerd's main gRPC listener defaults to `/run/containerd/containerd.sock`,
with the ttrpc plugin at `/run/containerd/containerd.sock.ttrpc` [136]. The kubelet's
`--container-runtime-endpoint` defaults to `unix:///run/containerd/containerd.sock`, and Kubernetes
documents that Linux supports Unix-domain sockets while Windows supports `npipe` and TCP endpoints,
giving `npipe:////./pipe/runtime` as the example [137]. Kubernetes device plugins register to the
kubelet over the host Unix socket in the hard-coded directory `/var/lib/kubelet/device-plugins/`
[138].

**Tailscale.** The `safesocket` package creates a Unix socket where possible and otherwise localhost
TCP; `ConnectContext` connects to tailscaled via Unix socket or named pipe, and `Listen` uses a Unix
path on Unix and a named-pipe path on Windows [139]. OS peer-credential authentication is enabled
only for `linux`, `darwin`, `freebsd`, `solaris` and `illumos`, and `PlatformUsesPeerCreds`
additionally requires the `HasUnixSocketIdentity` build feature [139] — a concrete example of a
production system that has peer credentials on Unix and something else on Windows. The Darwin
sandbox/App Store LocalAPI listener uses `tcp4` on `127.0.0.1:0`, generates a cryptographically
random 10-byte token rendered as hex, and distributes port and token through a `sameuserproof`
mechanism; a non-GUI CLI reads the same-user proof and otherwise falls back to tailscaled's default
Unix-socket mechanism, and Mac System Extension mode records a token file in `/Library/Tailscale`
owned `root/admin` with mode `0640` [140]. On Windows, issue #7730 (opened 2023-03-29) records that
the safesocket server was obtaining the client PID, opening the process and querying user
information, and proposes the idiomatic `ImpersonateNamedPipeClient` approach instead [141] — the
PID-is-not-identity problem of section 7, in production.

**Cap'n Proto RPC.** The C++ RPC documentation says that as of version 0.4 the only supported way to
communicate between threads is over pipes or socketpairs, and instructs users to set up RPC over
such a socketpair [142]. `[uncertain]` Whether `TwoPartyVatNetwork` lacks named-pipe support, and
the exact `setFdOnStream`/`FdPasser` semantics and release history, were not established from a
primary source in this research.

**iceoryx / iceoryx2.** Eclipse iceoryx describes itself as true zero-copy shared-memory IPC that
transfers data from publishers to subscribers without a single copy, ensuring constant latency
regardless of payload size, and lists support for Linux, macOS, QNX, FreeBSD and Windows 10 [143].
iceoryx2's README describes consistently low transmission latency regardless of payload size; that
is a project claim, not an independently established cross-platform guarantee [144]. `[uncertain]`
The iceoryx v1 RouDi daemon's exact role, the iceoryx2 versioned platform matrix, and the
per-platform primitives were not established from primary sources in this research.

**Wayland.** The wire protocol runs over a Unix-domain stream socket, usually named `wayland-0`,
whose name `WAYLAND_DISPLAY` can change; since Wayland 1.15 `WAYLAND_DISPLAY` may be an absolute
server-socket path, and `WAYLAND_SOCKET` indicates an inherited socket [145]. A file-descriptor
argument is carried as ancillary data in `msg_control` rather than in the message buffer; ordering
follows message and argument order but the exact byte position in the stream is not specified, and
both clients and compositors must queue received data until whole messages are available because
descriptors may arrive before or after their matching bytes [145]. Wayland is therefore a protocol
that structurally cannot run on a transport without `SCM_RIGHTS`.

**PostgreSQL.** `unix_socket_directories` selects the Unix-socket directories; an empty value
disables Unix sockets, the normal default is `/tmp` (build-configurable), and on Windows the default
is empty [146]. Linux-only abstract sockets are selected by an `@`-prefixed value, for which no lock
file exists [146]. Each filesystem socket is named `.s.PGSQL.nnnn` with lock file
`.s.PGSQL.nnnn.lock`, where `nnnn` is the port [146]. `unix_socket_permissions` defaults to `0777`,
and PostgreSQL explicitly states that only write permission matters for Unix sockets and that
abstract sockets have no file permissions [146]. `unix_socket_group` is unsupported on Windows, and
`unix_socket_permissions` is irrelevant where the OS ignores socket permissions [146]. Peer
authentication is available only where `getpeereid()`, `SO_PEERCRED` or a similar OS mechanism
exists, and is not a remote-connection mechanism [147]. PostgreSQL on Windows thus uses loopback TCP
rather than a native local transport.

**Redis.** `unixsocket` has no default, so Redis does not listen on a Unix socket unless configured,
and `unixsocketperm` sets its octal filesystem mode [148]. The benchmark documentation says local
clients can use loopback TCP or Unix sockets and that Unix sockets can deliver about 50% more
throughput than loopback TCP on Linux, that `redis-benchmark` defaults to TCP/IP loopback, and that
the Unix-socket advantage tends to decrease with long pipelines [149].

**SSH agent.** OpenBSD-current `ssh-agent(1)`, dated 2026-05-27, says the default agent endpoint is
a Unix-domain socket at a randomised `$HOME/.ssh/agent/s.*` path, selectable with `-a`, whose
pathname is written to `SSH_AUTH_SOCK` [150]. The endpoint is accessible only to the current user but
remains abusable by root or by another instance of the same user, agent socket files should be
readable only by their owner, and they are automatically removed when the agent exits [150] — the
manual does not state a literal `0600` mode. On Windows, the OpenSSH port sets
`SSH_AUTH_SOCK=\\.\pipe\openssh-ssh-agent` in `contrib/win32/win32compat/wmain_common.c` [151],
which is the same Unix-socket-versus-named-pipe split as NNG and Tailscale. `[uncertain]` No dated
PuTTY release note for Pageant's named-pipe support was located in this research.

**Java (JEP 380).** JEP 380 was created 2020-02-06, updated 2021-06-29 and delivered in Java 16,
adding `UnixDomainSocketAddress`, `StandardProtocolFamily.UNIX` and Unix-domain
`SocketChannel`/`ServerSocketChannel` factories [152]. Its stated goal is the intersection of
`AF_UNIX` features common to major Unix platforms and Windows, with TCP-like channel read/write,
connection, accept, selector and socket-option behaviour [152]. It **excludes** the Linux abstract
namespace and socket pairs because they are not common across those platforms, and names peer
credentials as an explicit potential JDK-specific socket-option exception [152]. It argues that
Unix-domain sockets are more secure and efficient than TCP loopback for local IPC because they are
local-only and subject to filesystem access control, and specifically identifies shared-volume Unix
sockets as useful between containers on one system [152]. It says Windows 10 and Windows Server 2019
support Unix-domain sockets [152]. The JEP text does not promise descriptor passing, so it must not
be claimed from the JEP alone [152].

**.NET.** `System.IO.Pipes` supplies `NamedPipeServerStream`/`NamedPipeClientStream` and
`AnonymousPipeServerStream`/`AnonymousPipeClientStream` [153]; anonymous pipes are documented as
local-only, lower-overhead, one-way IPC with limited services [153]. `UnixDomainSocketEndPoint`
represents a Unix-domain endpoint as a path [154]. Microsoft's gRPC IPC guidance calls Unix sockets
usable on Linux, macOS, Windows 10 and Windows Server 2019 and later, and says .NET has built-in
client and server support [156]. Crucially, .NET's named pipes on Unix are implemented over a Unix
socket file: a documented .NET 11 behaviour change says `NamedPipeServerStream` with
`PipeOptions.CurrentUserOnly` creates the underlying socket file as mode `0600` at bind time where it
formerly inherited the process umask [155].

**Go.** Go's `net` package documents `unix` and `unixpacket` among the legal network strings for
`Listen` [158]; Go 1.12 release notes state that `AF_UNIX` is supported on compatible Windows
versions [157]. `[uncertain]` No primary Go source was located proving `unixpacket` is
`SOCK_SEQPACKET` or documenting its macOS unavailability. Microsoft's `go-winio` focuses on Win32
I/O, named pipes and other handles, and using named pipes as a Go `net` transport; it uses I/O
completion ports and requires Windows Vista or later [159]. `[uncertain]` No pinned
`moby`/`containerd` module source was located to call it Docker's dependency.

**Node.js and libuv.** Node documents IPC as named pipes on Windows and Unix-domain sockets
elsewhere, with `connect`, `createConnection`, `listen` and `socket.connect` taking a `path` endpoint
[160]. It documents the Unix path limit as `sizeof(sockaddr_un.sun_path)`, typically 107 bytes on
Linux and 103 on macOS, and says Node-created pathname sockets are unlinked at `server.close` but
can persist after a crash [160]. Linux abstract sockets use a leading NUL, are invisible in the
filesystem and disappear when all references close, with abstract binding support recorded in
v20.8.0 [160]. Windows IPC paths must be in `\\?\pipe\` or `\\.\pipe\`, the namespace is flat, and
pipes disappear after the last reference closes or the owner exits [160]. libuv's `uv_pipe_t` is
documented as an abstraction over streaming Unix files including local-domain sockets, pipes and
FIFOs, and over named pipes on Windows [161]. `uv_pipe_bind`/`uv_pipe_connect` take Unix file paths
or Windows pipe names; the basic variants do not support Linux abstract sockets while
`uv_pipe_bind2`/`uv_pipe_connect2` do, were added in libuv 1.46.0, and require the leading NUL to be
counted in `namelen` [161]. Unix paths may be silently truncated to `sizeof(sockaddr_un.sun_path)`,
typically 92–108 bytes, unless `UV_PIPE_NO_TRUNCATE` makes `bind2`/`connect2` return `UV_EINVAL`
[161].

**Summary of the pattern.** Every system that supports all three platforms and needs a local
transport converges on the same shape: Unix-domain sockets on Unix, named pipes on Windows — NNG
[117], Node/libuv [160][161], .NET [153][155], Tailscale [139], Docker [132], the kubelet [137],
Chromium Mojo [123], go-winio [159], SSH agent [150][151]. The two systems that instead chose
Windows `AF_UNIX` are libzmq [114] and Java [152], and libzmq immediately acquired a
stale-socket-file bug from it [114].

## 6. Hostile environments and fallbacks

### 6.1 macOS App Sandbox

Apple DTS states that a sandboxed app cannot connect to arbitrary Unix-domain sockets [60], but also
stated in June 2020 that no special entitlement is needed to use Unix-domain sockets inside a
sandboxed app — evidence about `AF_UNIX` itself, not a grant covering arbitrary paths [63]. What is
restricted is the path. A sandboxed app can create a Unix-domain socket in its own container or in a
container reachable through an App Group [61], and two sandboxed apps from the same team can
communicate over a Unix-domain socket when both join an App Group and the listener creates the
socket in that group container [60]. Filesystem temporary exceptions such as
`com.apple.security.temporary-exception.files.absolute-path.read-write` apply to *files*, not to
Unix-domain sockets [61], and a dynamic sandbox extension obtained through an open panel likewise
applies to files and directories rather than sockets [61].

`com.apple.security.application-groups` is available on macOS 10.7.5 and 10.8.3 and later, permits
apps from one development team to share a group container intended for non-user-facing content, and
Apple states that App Groups permit members to share Mach and POSIX semaphores and certain other IPC
mechanisms [57]. Group containers live at `~/Library/Group Containers/<application-group-id>`, where
the group ID is the team ID, a period, and a developer-chosen name [57]. DTS explicitly recommends
POSIX shared memory (`shm_open` and related APIs) for sandboxed sharing through an App Group [64].

Two hard limits collide here. `sun_path` is 104 bytes on Darwin [36], and a Group Containers path
plus a team-ID-prefixed group name consumes most of it, so the socket basename must be kept very
short and the fully expanded pathname checked before `bind` [inference, from 36, 57].
`PSHMNAMLEN` is 31 [42], so a POSIX shared-memory name has even less room. `[uncertain]` The
frequently repeated rule that a sandboxed POSIX shared-memory name must carry an app-group prefix
was **not** located in accessible Apple primary documentation; the only concrete prefix grammar
found is for POSIX *semaphores*, where DTS says names must be `GGG/NNN` with `GGG` an app-group ID
[62], and that source does not establish the same grammar for `shm_open` [62][64].

`com.apple.security.network.client` means the app may open outgoing network connections and
`com.apple.security.network.server` that it may listen for incoming ones [57]. `[inference]` Taken
with the DTS `AF_UNIX` statement, those entitlements should not be presented as prerequisites for a
socket placed in an allowed container, and Apple's material does not state that they authorise
arbitrary `AF_UNIX` paths [57][61][63].

Mach lookup is separately gated. The documented temporary entitlement for looking up a global Mach
service is `com.apple.security.temporary-exception.mach-lookup.global-name`, whose value is an array
identifying the services to enable, and without it a sandboxed lookup of a global Mach service fails
[58]. DTS says Developer-ID distribution may use it while Mac App Store distribution requires App
Review approval and is difficult [53]. The App-Group alternative is to team-ID-prefix the service
name and grant the sandboxed client an app-group entitlement that is a prefix of that name [53]; DTS
reports XPC working among apps in one App Group with a service-name form of
`team_id.group.your_app_domain.suffix` [66]. `[inference]` XPC or Mach with an App Group is
therefore the Apple-sanctioned cross-sandbox route, and raw `AF_UNIX` is viable only within an
accessible container [57][58][60][66].

For completeness, the `sandbox-exec` profile language is a separate, Scheme-like mechanism with
operations such as `network*`, `network-outbound` and `file-write*`, and is not the App Sandbox
entitlement API [180]. `[uncertain]` Apple has published no complete stable profile-language
reference in the sources retrieved, so profile-language details are public reverse-engineering
rather than an API contract [180].

**iOS/iPadOS, for contrast.** Apple says App Groups enable communication and data sharing between
apps from the same developer [67], with shared preferences through
`NSUserDefaults initWithSuiteName:` and shared-container file coordination through Core Data, SQLite
or POSIX locks [67]. DTS says iOS cross-team IPC restrictions are intentional and warns that
bypasses are not durable, and in one cross-process scenario calls a shared file container the only
viable solution, suggesting a Darwin notification for change awareness [66]. `[inference]` Arbitrary
peer-daemon attachment is not a plan on iOS [66][67]. `[uncertain]` No retrieved Apple source claims
iOS loopback TCP is unrestricted, so loopback must not be presented as an iOS exemption [66][67].

### 6.2 Windows AppContainer, UWP and low integrity

AppContainers run at Low integrity level [101], and a Low-IL principal cannot write an object at
Medium IL even when the DACL would allow it, because the integrity SID sits in the object's SACL as
a `SYSTEM_MANDATORY_LABEL_ACE` [102]. Two grants are therefore needed for an AppContainer client to
reach a named pipe: a DACL entry for the package or capability SID (or `ALL APPLICATION PACKAGES`)
and a compatible mandatory label. A Microsoft Q&A answer for UWP-to-full-trust named pipes reports
that the connection fails without a DACL containing `ALL APPLICATION PACKAGES`, its working DACL
also containing the signed-in user SID [108]. `[uncertain]` The commonly cited SDDL string
`S:(ML;;NW;;;LW)` — a Low mandatory label with no-write-up — was **not** found in a retrieved
Microsoft named-pipe or AppContainer document; it is a conventional expression of the cited
low-integrity requirement and does not by itself grant a package access, since the DACL still
controls that [101][102]. Separately, Windows 10 version 1709 restricts named pipes in an app
container to processes in the same app and requires the `\\.\pipe\LOCAL\` name form [68][78].

Named objects are isolated by default: an AppContainer can access only named objects it created, and
sharing one requires permissions at creation time and qualified names on open, for which Microsoft
directs applications to `GetAppContainerNamedObjectPath` plus ACL grants [103]. `[inference]`
AppContainer code should therefore use its own qualified named-object namespace rather than assume it
can create `Global\` shared memory, since a global file mapping outside session 0 needs
`SeCreateGlobalPrivilege` [89][90][103][104].

Loopback is blocked by default: UWP default firewall block filters enforce network isolation by
dropping packets that lack the capabilities needed for the target resource [106], packaged
applications block loopback IPC by default [100], all packaged apps participating in a loopback
connection must declare `privateNetworkClientServer` [100], and the documented escapes are
`CheckNetIsolation.exe LoopbackExempt -a -n=<AppContainer-or-PackageFamily>` for an outbound packaged
client and `-is -n=<PACKAGEFAMILYNAME>` for an unpackaged client reaching a packaged listener, the
latter needing to remain running while the packaged app listens [100][105]. Microsoft's sanctioned
UWP app-to-app IPC facility is App Services, which exchange `ValueSet` property bags (rich objects
must be serialised), can run out-of-process as a background task or in-process, and are recommended
for small data where near-real-time latency is not required [107][100]. `[uncertain]` No Microsoft
support statement for `AF_UNIX` inside UWP/AppContainer was found, so it must be treated as
unverified [86][103].

### 6.3 Flatpak

A default Flatpak sandbox has no host-file access beyond the runtime, the app,
`~/.var/app/$FLATPAK_ID` and `$XDG_RUNTIME_DIR/app/$FLATPAK_ID`, of which only the last two are
writable [162]; `$XDG_RUNTIME_DIR/app/$FLATPAK_ID` is therefore the default writable directory for a
filesystem socket [162]. `--filesystem=` is the documented way to add access beyond that view [162].
The socket permission vocabulary includes `x11`, `wayland`, `system-bus` and `session-bus` [163].
D-Bus is filtered by default: the default session-bus policy permits the app's own `$FLATPAK_ID`
namespace, `org.mpris.MediaPlayer2.$FLATPAK_ID`, the bus itself, and `org.freedesktop.portal.*`;
`--socket=session-bus` or `--socket=system-bus` grants the entire bus, disables that filtering, and
Flatpak calls it a security risk to avoid except for development tools [162]. Portals are the
documented way for sandboxed apps to interact with host files, data and services without additional
static permissions [164]. `[inference]` A Flatpak whose sandbox has an unshared network namespace
cannot reach host abstract `AF_UNIX` addresses, since network namespaces isolate the abstract
namespace; sharing the network namespace removes that particular barrier while filesystem
permissions still govern pathname sockets [1][17][163]. `[uncertain]` No Flatpak primary text states
that consequence explicitly; it follows from the network-sharing flag plus the Linux namespace rule
[17][163].

### 6.4 Snap

Strictly confined snaps are isolated to a minimal access level and cannot reach files, network,
processes or other system resources without an interface, enforced with AppArmor, seccomp and
namespaces [165]. `SNAP_COMMON` is `/var/snap/<snap name>/common` and `SNAP_USER_COMMON` is
`/home/<username>/snap/<snap name>/common` [166]. The `content` interface shares a producer slot's
code or data with consumer plugs at filesystem level, and Snap explicitly says that sharing can
include sockets [167]. `[community report]` Snap community documentation states that supported
strictly-confined Unix-socket forms include `$SNAP_DATA/...`, `$SNAP_COMMON/...` and abstract
`@snap.<snap-name>.<suffix>`, sourced to a 2025 snapd forum discussion rather than normative
reference documentation, so it should be validated against snapd/AppArmor policy source before being
treated as a stable interface [168]. `[uncertain]` No primary source was found stating that **every**
abstract socket name must be prefixed with the snap name — searched snapd source, documentation and
AppArmor snap policy material — so that must not be claimed [168]. `[uncertain]` No primary source
proves the `network-bind` interface is required for a filesystem `AF_UNIX` bind; TCP/UDP service
binding must not be conflated with every Unix-socket bind [165][168].

### 6.5 bubblewrap and namespaces generally

Bubblewrap is an unprivileged sandboxing tool built on namespaces that lets the caller select which
filesystem parts are visible and whether they are read-only or writable, with `--ro-bind` making a
bind target read-only and `--dev-bind` bind-mounting the device tree from the parent namespace
[169]. `--unshare-net` creates a new network namespace [169]. Network namespaces isolate interfaces,
protocol stacks, routes, firewall rules, port numbers **and the abstract `AF_UNIX` namespace** [17],
so `[inference]` abstract socket pairs cannot cross an unshared network namespace [1][17]. Mount
namespaces isolate the mount view, so a file visible in one may be absent in another [19];
`[inference]` a pathname socket is connectable across mount namespaces only if the same inode is
deliberately visible through a shared or bind-mounted filesystem view and permissions allow it
[1][19]. A read-only root or mount prevents creating a filesystem socket there, so a daemon listener
needs a writable tmpfs or bind-mounted runtime directory such as `/run` [172][173].

IPC namespaces isolate System V IPC objects and POSIX message queues [18], and POSIX shared memory
uses the `/dev/shm` tmpfs mount in the common Linux implementation [8]. `[inference]` POSIX
shared-memory sharing therefore depends on both IPC-namespace compatibility and a mutually visible
`/dev/shm` mount, so independent mount namespaces need a shared volume or bind mount even where the
IPC namespace is shared [8][18].

### 6.6 Containers

Docker gives each default container its own network stack and no privileged access to another
container's sockets or interfaces [170]; host-network mode shares the host network namespace instead
of isolating it [171]. `[inference]` Container `127.0.0.1` is therefore container-local, so
host-to-container loopback attachment fails unless host networking, published ports or another
explicit arrangement is used [170][171], and because abstract `AF_UNIX` is network-namespace scoped
it does not cross normal container boundaries either [17][170][171]. A bind mount mounts a host file
or directory into a container, writable by default unless `ro`/`readonly` is used [172];
`[inference]` a writable bind-mounted directory containing a pathname Unix socket is the standard
host-to-container local-IPC bridge, and ownership and mode must be checked because it also grants
host filesystem authority [1][172]. `--read-only` makes the container root filesystem read-only
except for specified writable volumes [173]. Docker documents IPC namespaces as separating named
shared-memory segments, semaphores and message queues, with sharing available through
`--ipc=container:<donor>` after a `shareable` donor configuration [174]; the daemon's
`--default-shm-size` default is 64 MiB [131]. `[inference]` `--ipc=host` makes host IPC exposure an
explicit privileged choice, so shared memory should not be a cross-container default [174].

On Windows containers, the Docker CLI and engine communicate over the named pipe
`//./pipe/docker_engine` [132], and Microsoft's AKS HostProcess documentation says named-pipe mounts
and Unix-domain sockets are not directly supported although host paths such as `\\.\pipe\*` can be
accessed [111].

### 6.7 Anti-cheat and anti-tamper

This is the thinnest evidence in the sheet and is reported with explicit evidence grading.

- `[vendor statement]` Riot's VALORANT support says `VAN: INCOMPATIBLE SOFTWARE` means a driver file
  is incompatible with Vanguard, identifies the flagged file and recommends removing it; Riot does
  **not** publish a general IPC-primitive blocklist [176].
- `[vendor statement]` Riot documents that Vanguard can be disabled from the system-tray icon, but
  VALORANT will not run until it is re-enabled by restart [177].
- Microsoft documents `ObRegisterCallbacks` as registering callbacks for thread, process and desktop
  handle operations [109], and documents that an object pre-operation callback can modify
  `OB_PRE_CREATE_HANDLE_INFORMATION.DesiredAccess` only to **restrict** the granted access, never to
  add rights [110].
- `[community report]` Security research commonly reports kernel anti-cheat drivers using object
  callbacks to reduce process-handle rights. This is mechanically plausible from Microsoft's
  documented API, but vendor attribution is not established by Microsoft's documentation [109][110].
- `[uncertain]` **No public Easy Anti-Cheat or BattlEye vendor document was found** stating that it
  blocks named pipes, `AF_UNIX` sockets or shared memory as IPC categories. Such a policy must not be
  claimed [109][110].
- `[vendor statement]` Steam says its overlay injects itself into the game process and exposes
  overlay APIs including `ISteamFriends::ActivateGameOverlay` [178].
- Discord's archived official RPC guidance tells clients to probe local IPC endpoints `discord-ipc-0`
  through `discord-ipc-9` [179]. `[community implementation]` The conventional path renderings
  `\\.\pipe\discord-ipc-0` on Windows and `/run/user/{uid}/discord-ipc-0` on Unix come from community
  bridges; Discord's first-party guide establishes only the endpoint suffix range [179].
- `[uncertain]` No credible public source was located establishing a general anti-cheat failure of
  Discord, Steam or OBS specifically because of their named pipes or shared memory. Overlay and
  injection restrictions must be evaluated per game, vendor and version [176][177][178][179].
- `[uncertain]` No vendor source was located naming RenderDoc as blocked by Vanguard; Riot's public
  material concerns incompatible driver files [176].

The actionable conclusion is negative and should be stated as such: the evidence does not support
ranking pipes, sockets or shared memory by anti-cheat survivability. What the evidence does support
is that the documented kernel mechanism restricts **process handle rights** [109][110], not
named-object connections — so an integration that needs no `OpenProcess` handle and no code injection
has strictly fewer documented ways to fail than one that does [inference, from 109, 110].

### 6.8 Which fallback still works, per restriction

| Restriction | Blocked or restricted | Still works |
| --- | --- | --- |
| macOS App Sandbox | arbitrary `AF_UNIX` paths [60]; global Mach lookup without entitlement [58] | `AF_UNIX` inside own or App-Group container [61]; XPC/Mach with entitlement or App Group [53][66]; POSIX SHM via App Group [64] |
| Windows AppContainer / UWP | named pipe without package SID in DACL [108]; `Global\` sections without privilege [89][104]; loopback by default [100][106] | pipe with `ALL APPLICATION PACKAGES` DACL + low label [108]; `\\.\pipe\LOCAL\` in-app pipes [68]; App Services [107]; loopback with `LoopbackExempt` + capability [100] |
| Flatpak (default) | host paths; full session bus discouraged [162] | `$XDG_RUNTIME_DIR/app/$FLATPAK_ID` sockets [162]; portals [164]; `--filesystem=` grants [162] |
| Snap strict | unmediated paths and sockets [165] | `$SNAP_COMMON`/`$SNAP_DATA` sockets and `content` interface [166][167]; `[community report]` `@snap.<name>.*` abstract names [168] |
| Unshared network namespace | abstract `AF_UNIX` [1][17]; host loopback [17] | pathname socket on a bind-mounted path [1][19] |
| Read-only rootfs | creating a socket file [173] | writable tmpfs or volume for the socket directory [172][173] |
| Separate IPC + mount namespace | SysV IPC, POSIX message queues [18]; `/dev/shm` visibility [8] | shared volume or `--ipc=` sharing as an explicit choice [174] |
| Container to host | loopback [170][171]; abstract sockets [17] | bind-mounted pathname socket [172]; on Windows, host `\\.\pipe\*` access [111] |
| Anti-cheat (documented mechanism) | process handle rights via object callbacks [109][110] | `[inference]` designs needing no `OpenProcess` handle and no injection [109][110] |

Ranked by the collected evidence, `[inference]`: system-mediated capability-gated IPC (XPC/App
Groups, UWP App Services, Flatpak portals, Snap interfaces) is the most portable across these
regimes [57][66][107][164][167]; pathname sockets in an explicitly shared writable directory come
second and fail on mount visibility, writability or macOS container rights [19][61][162][167][172];
named pipes are strong for ordinary Windows desktop attachment but need deliberate DACL and
integrity setup in AppContainer [101][102][108]; loopback TCP is not a hostile-host default because
UWP blocks it and containers have separate loopback stacks [100][106][170]; abstract `AF_UNIX` is
least portable [1][17][86]; and shared memory is a performance tool rather than an attachment
default [18][57][103].

## 7. Peer identity on local transports

Without TLS, a local server learns who connected from the kernel, and each platform offers a
different quality of answer.

**Linux.** `SO_PEERCRED` gives PID, UID and GID as they were at `connect()`, `listen()` or
`socketpair()` [1]. Because those are a snapshot, they cannot be forged by a later `setuid()`, which
`unix(4)` on Darwin makes explicit for the equivalent option [35]. `SO_PEERGROUPS` (Linux 4.18) adds
supplementary GIDs [28], `SO_PEERSEC` (Linux 2.6.2) the SELinux context [3], and `SO_PEERPIDFD` with
`SCM_PIDFD` (Linux 6.5) a pidfd [29]. `SCM_CREDENTIALS` with `SO_PASSCRED` gives per-message
credentials that the kernel validates, so an unprivileged sender cannot claim a foreign PID, UID or
GID [1].

**macOS.** `LOCAL_PEERCRED` returns `struct xucred` with the effective UID and group list but **no
PID** [35], and `getpeereid(3)` returns effective UID and GID for a stream socket on which `connect`
or `listen` has happened [40]. `LOCAL_PEERPID`, `LOCAL_PEEREPID`, `LOCAL_PEERUUID`,
`LOCAL_PEEREUUID` and `LOCAL_PEERTOKEN` exist in the public header, the last being the peer audit
token [36]. Apple's own guidance is to bind authorisation to the audit token, validatable with
`kSecGuestAttributeAudit`, rather than to a PID [52]; for XPC the modern route is
`xpc_connection_set_peer_code_signing_requirement` (macOS 12.0) [50], with
`NSXPCConnection setCodeSigningRequirement:` in macOS 13 and
`xpc_connection_set_peer_lightweight_code_requirement` in macOS 14.4 [51].

**Windows.** There is no `SO_PEERCRED`. The documented mechanism is `ImpersonateNamedPipeClient`,
which impersonates the client that sent the last message read and must be paired with `RevertToSelf`
[75][76]; the resulting token yields the client's SID. `GetNamedPipeClientProcessId` gives a PID
[78], and `GetNamedPipeClientComputerName` a computer name [79]. For Windows `AF_UNIX`,
`[uncertain]` no primary source documents any peer-credential support [86][87], which is why systems
that need local identity on Windows use named pipes (Tailscale [139][141], .NET [155], NNG [119]) or
a nonce/token file (Tailscale on sandboxed Darwin [140], D-Bus `nonce-tcp` [125]).

**Pitfalls.**

- *PID reuse.* A PID is an observation, not an authentication: Windows can reuse a PID after process
  exit [inference, from 75, 78], Apple DTS says the PID space is small and PIDs are commonly reused
  [52], and NNG warns that a peer PID can change if a descriptor is passed between processes [119].
  `SO_PEERPIDFD` exists precisely because a numeric PID can exit and be reused while a pidfd refers
  to that one process [29]. Tailscale issue #7730 records exactly this pattern in production: the
  Windows server was taking the client PID, opening the process and querying user information, and
  the fix is to use the impersonation token instead [141].
- *Time-of-check/time-of-use on process identity.* `[inference]` Re-opening a process by PID to
  inspect its image with `QueryFullProcessImageName` races against process exit and reuse; a real
  process handle must be retained where process identity matters [78].
- *Impersonation.* On Windows the risk runs the other way too: the default server impersonation level
  is `SecurityImpersonation`, so a privileged *client* connecting to an attacker-controlled pipe name
  can be impersonated by that server, and the client's defence is `SECURITY_SQOS_PRESENT` with
  `SECURITY_IDENTIFICATION` or `SECURITY_ANONYMOUS` [76]. A server must also never service a request
  after `ImpersonateNamedPipeClient` has failed [75].
- *Name squatting.* `FILE_FLAG_FIRST_PIPE_INSTANCE` fails with `ERROR_ACCESS_DENIED` if an instance
  already exists, which is how a Windows server detects that its name was pre-created [68]. libzmq's
  `ipc://` has the opposite behaviour by design: a second process binding an already-bound endpoint
  succeeds and the first loses its binding [112].
- *Symlink and substitution races on socket paths.* A pathname socket may be unlinked at any time and
  is removed when the last reference closes [1], so unlink-then-bind at startup is standard — and is
  a race unless the containing directory's ownership and permissions prevent another principal from
  placing an endpoint at that path [inference, from 1]. This is why NNG recommends a server-writable,
  client-searchable directory rather than relying on the socket's own mode [119], why Microsoft
  recommends putting the logon SID on a pipe DACL to exclude other sessions [74], and why the
  `NULL`-security-descriptor default on Windows (Everyone read [68]) and the umask-derived default on
  Unix [1][35] both need to be overridden explicitly. PostgreSQL's default `unix_socket_permissions`
  of `0777` is only safe because only write permission matters and the directory is expected to be
  controlled [146].
- *Abstract sockets have no access control at all.* gRPC states it outright for `unix-abstract:`: no
  permissions apply, so any user or process may access it [120]. The only boundary is the network
  namespace [17].

## 8. In-process transport

### 8.1 What libzmq `inproc` guarantees

`inproc` passes messages via memory directly between threads sharing one context, and no I/O threads
are involved, so a context used only for in-process messaging can be initialised with zero I/O
threads [113]. A bind endpoint is an arbitrary string up to 256 characters that must be unique within
the context, with no other format restriction [113]. **Connect-before-bind:** before version 4.0 the
name had to have been created by binding at least one socket in the same context first; since 4.0 the
order of `zmq_bind()` and `zmq_connect()` does not matter, as with `tcp` [113]. The zguide states the
older rule as a specific limitation and records that it was fixed in ZeroMQ v4.0 [116], and its
troubleshooting section still says to keep both sockets in the same context — otherwise the
connecting side fails — and to bind before connect, because "inproc is not a disconnected transport
like tcp" [116].

Ordering and buffering: `inproc` is described as a connected signalling transport, much faster than
`tcp` or `ipc` [116]. High-water marks behave differently from other transports: HWM is a
per-connection limit on outstanding messages queued in memory for a single peer, zero meaning no
limit [115], but **over `inproc` the sender and receiver share the same buffers, so the real HWM is
the sum of the HWMs set by both sides** [116]. HWMs are approximate in any case: the guide notes the
real buffer size may be as little as half the configured value, and `zmq_setsockopt` warns that the
actual send limit may be up to 90% lower depending on message flow [115][116]. At the HWM, PUB and
ROUTER sockets drop while other types block [116]. The zguide's own architectural advice is to use
attached threads connected to their parent over `inproc` PAIR sockets when low latency is vital, and
detached threads with their own contexts over `tcp` when the threads should later be separable into
processes — noting explicitly that the `inproc` pattern "is not scalable out to processes" [116].

### 8.2 What NNG `inproc` guarantees

NNG's `inproc` connects sockets within the same process as an alternative to slower transports, and
"tries hard to avoid copying data, and thus is very light-weight" [118]. The URI is `inproc://` plus
an arbitrary NUL-terminated string; multiple URIs in one application do not interfere, and two
separate applications may use the same URI without interfering and will be unable to communicate
through it [118]. `inproc` has no special options, and although it accepts `NNG_OPT_RECVMAXSZ` for
compatibility the value is ignored with no enforcement, because "as `inproc` peers are in the same
address space, they are implicitly trusted, and thus it makes no sense to spend cycles protecting a
program from itself" [118]. That sentence is the cleanest statement in any of these documents of what
in-process transport gives up.

### 8.3 Rust in-process patterns

`std::sync::mpsc` provides multi-producer single-consumer channels in two flavours: `channel()`
returns a conceptually infinitely buffered channel whose sends never block, and `sync_channel(n)`
returns a bounded channel whose sends block until buffer space is available, with a bound of 0 making
it a rendezvous channel where each sender hands a message directly to a receiver [192]. Send and
receive return `Result`, and an error normally indicates that the other half was dropped in its
thread; once half a channel is deallocated most operations cannot progress and return `Err` [192].

`tokio::sync::mpsc` provides the same shape for asynchronous tasks: the bounded variant waits when
full, so it provides backpressure, while the unbounded variant always completes immediately and is
usable from synchronous code [191]. When all senders are dropped, remaining buffered values are
delivered and then `recv` returns `None`; if the receiver is dropped, further sends error and all
unread messages are drained and dropped [191]. For clean shutdown the receiver first calls `close`,
which prevents further sends, then consumes the channel to completion [191]. The channel is
runtime-agnostic and participates in cooperative scheduling when used inside Tokio [191]. Its
allocation behaviour is documented as blocks in a linked list holding 32 messages on 64-bit and 16 on
32-bit targets, independent of channel and message size, with 4 pointer-sized bookkeeping values per
block [191].

### 8.4 How in-process differs in failure semantics

The difference is crash isolation, and it is total.

- **No fault containment.** NNG says in-process peers are in the same address space and therefore
  implicitly trusted, which is why it does not enforce a receive-size limit there [118]. A malformed
  or hostile message from an in-process peer is a bug in the same program; over IPC it is an
  untrusted input that a receive-size limit must bound [118].
- **No independent death.** Over `AF_UNIX` a peer's death is observable: `ECONNRESET` or `EPIPE` on a
  stream, `SIGPIPE` unless suppressed [1]; on a named pipe it is `ERROR_BROKEN_PIPE`
  [inference, from 68, 73]. In-process, the corresponding event is a dropped channel half, which
  surfaces as `Err`/`None` [191][192] — but a panic or abort in one thread does not leave the other
  side running with a clean error; the process is gone.
- **No supervised restart.** launchd restarts a crashed XPC service and clients see interruption
  rather than invalidation [49]; systemd re-activates a socket-activated service while the listening
  descriptor is held by the manager [129][130]. There is no in-process analogue.
- **No credentials, and none needed.** Every peer-credential mechanism in section 7 answers a
  question that does not exist in-process.
- **Shared buffers change the flow-control arithmetic.** The libzmq HWM summing rule [116] is a
  concrete instance: the same configuration means something different in-process than over IPC.
- **What is gained.** No serialisation is required in principle — NNG says `inproc` tries hard to
  avoid copying [118] — and the measured latency gap is one to two orders of magnitude (section 9).

## 9. Performance characteristics

Every number below carries its hardware and methodology caveat. None of these are peer-reviewed;
they are cited because they are the published, reproducible measurements that exist.

**Unix sockets versus loopback TCP on Linux.** Eli Bendersky's Go ping-pong benchmark (2019-02-12)
used a 128-byte default packet, measured send-and-echo round trips and halved them, reporting roughly
2.3 µs average one-way Unix-socket latency against 3.6 µs for loopback TCP; CPU, kernel and Go
version are not reported, so this is directional only, and the author explicitly warns that socket
benchmarking is hard [181]. The same source measured large single-call sends to a discarding server:
at 512 KiB, Unix sockets reached 10 GB/s and loopback TCP 9.4 GB/s; both tapered at about 13 GB/s at
16–32 MiB; and at 64 KiB TCP won on that host, whose hardware is unspecified [181].

Goldsborough's `ipc-bench` is a reproducible CMake suite for Linux and macOS whose sequential
throughput procedure is a single message sent forth and back between two processes [182]. On its
stated Intel Core i5-4590S 3.00 GHz / Ubuntu 20.04.1 LTS host it measured 100-byte and 1-KiB
ping-pong rates of 70,221 / 67,901 msg/s for TCP and 130,372 / 127,582 msg/s for `AF_UNIX`, which is
about 14.24 / 14.73 µs and 7.67 / 7.84 µs per round trip respectively (reciprocals derived from the
published rates) [182]. The same run reported 4,702,557 / 1,659,291 msg/s for shared memory,
5,338,860 / 1,701,759 for mmap, 265,823 / 254,880 for FIFO, 162,441 / 155,404 for pipe, and
232,253 / 213,796 for SysV/POSIX message queues [182]. The project labels its own code "rather old",
says configurations may be suboptimal, and specifically says the shared-memory versus mmap difference
may be a lack of warm-up — a caveat that must travel with the figures [182].

**Shared memory versus Unix sockets.** A 2026 C benchmark compared
`socketpair(AF_UNIX, SOCK_STREAM)`, `socketpair(AF_UNIX, SOCK_DGRAM)` and an
`mmap(MAP_SHARED|MAP_ANONYMOUS)` lock-free SPSC ring, pinning producer and consumer to cores 0 and 1,
using `CLOCK_MONOTONIC_RAW`, 200 warm-ups and 5,000 round trips plus an 8 MB unidirectional transfer
with a one-byte acknowledgement; CPU, kernel, compiler and memory topology are absent [183]. Median
32-byte round-trip time was 270 ns for shared memory, 4,640 ns for the Unix datagram pair and
5,910 ns for the Unix stream pair; at 8 KiB it was 14,360 / 19,850 / 23,280 ns [183]. Unidirectional
throughput at 32 bytes was 269.7 / 29.5 / 23.7 MB/s and at 8 KiB 482.8 / 526.7 / 451.2 MB/s [183].
This is a recent personal benchmark with public source, not a peer-reviewed result [183].

`[inference]` Across these three Linux datasets the latency figures span sub-microsecond to several
microseconds because synchronisation method, CPU placement, scheduling, API framing, payload size and
clock all differ. Benchmarks must be selected by methodology, not averaged [181][182][183]. The one
robust ordinal result that all three agree on is: shared memory with busy-polling or a user-space
ring beats Unix sockets by roughly an order of magnitude at small sizes, and Unix sockets beat
loopback TCP by roughly a factor of two at small sizes.

Redis's own documentation provides an independent qualitative confirmation on throughput: Unix
sockets can deliver about 50% more throughput than loopback TCP on Linux, and the advantage tends to
decrease with long pipelines [149].

**Windows.** There is no strong, citable, contemporary three-way benchmark of named pipes, loopback
TCP and shared memory in the primary sources reviewed. A 2009 Windows IA-32 paper reported average
communication response times of 0.025022 ms for named pipes and 0.004145 ms for a shared-memory
section against a 0.002099 ms clean-application baseline; that was an application instrumentation
experiment rather than an apples-to-apples microbenchmark, and it supplies no TCP figure [185]. It is
usable only with that caveat, and no comparative TCP number should be invented [185].

**macOS.** A public benchmark on a 2017 MacBook Pro with a 2.9 GHz Intel i7 running macOS Monterey
12.2 measured reused-connection round trips of 11 µs Unix socket versus 12 µs XPC at 10 bytes, 11 µs
versus 15 µs at 1 KiB, and 17 ms versus 12 ms at 10 MiB — XPC winning at the largest size — plus
connect-plus-10-byte round trips of 32 µs versus 95 µs [184]. The source has one commit and reports
no percentiles, sample count, compiler flags or load controls, so it is reproducible material but low
confidence [184]. **No public numeric benchmark directly comparing Mach ports, XPC and Unix sockets
was located**; XPC latency must not be used as a proxy for raw Mach-port latency [184].

**Kernel zero-copy applicability.** This matters because it bounds how far a socket-based local
transport can go.

- `splice(2)` exists since Linux 2.6.17 and moves data between descriptors without copying through
  user space, but **one descriptor must be a pipe**; `SPLICE_F_MOVE` is only a hint and has been a
  no-op since Linux 2.6.21 [22].
- `vmsplice(2)` (Linux 2.6.17) maps user-memory iovecs into a **pipe** when writing; only the
  user-to-pipe direction is true splicing, since pipe-to-user copies, and `SPLICE_F_GIFT` requires
  page-aligned memory and length plus an irrevocable promise not to modify [23].
- `sendfile(2)` exists since Linux 2.2, avoids user-space copies, requires an input that supports
  mmap-like operations and therefore **cannot be a socket**; since Linux 2.6.33 the output can be any
  file, Linux 5.12 desugars pipe output to `splice`, and it transfers at most `0x7ffff000` bytes per
  call [24].
- `MSG_ZEROCOPY` is documented for TCP, UDP, raw and packet sockets and supported since Linux 4.14;
  **`AF_UNIX` is absent from that list** and it must not be advertised as an `AF_UNIX` zero-copy
  option [25].
- `io_uring_prep_send_zc` prepares an asynchronous zero-copy `send(2)`, normally producing a send CQE
  plus a notification CQE (`IORING_CQE_F_NOTIF`) that releases the buffer, with
  `IORING_SEND_ZC_REPORT_USAGE` reporting copied bytes and a per-call limit of `INT_MAX` [26].
  `[inference]` It changes the submission and completion pattern and can report fallback copying, but
  it does not create `AF_UNIX` support where the underlying socket zero-copy facility lacks it
  [25][26].

The conclusion is structural, not incidental: **there is no kernel zero-copy path for `AF_UNIX`**
[22][23][24][25]. A local transport that needs zero-copy for large payloads has to pass a memfd or
file mapping and let the peer map it — which is exactly what Wayland [145], iceoryx [143] and Mojo's
shared-memory path [123][124] do, and why descriptor and handle passing (sections 1.4, 3.7) is the
load-bearing capability rather than an optional extra.

## 10. Rust ecosystem

**Standard library.** `std::os::unix::net` has provided `UnixStream`, `UnixListener` and
`UnixDatagram` since Rust 1.10.0, and they are explicitly Unix-only [186]. There is **no** `AF_UNIX`
support in `std` on Windows; the tracking issue rust-lang/rust#56533, opened 2018-12-05, is still
open [188]. Ancillary-data support is unstable: the stable docs mark `SocketAncillary`, `ScmRights`
and `ScmCredentials` as experimental, and the `unix_socket_ancillary_data` tracking issue
rust-lang/rust#76915, opened 2020-09-19, remains open and lists an outstanding alignment bug
[186][187]. **Descriptor passing is therefore not available on stable Rust through `std`.**

**Tokio** (1.53.1 docs snapshot; maintained by the Tokio project). `tokio::net::UnixStream` is an
async connected Unix socket with `connect`, `pair`, `peer_cred`, readiness APIs, `AsFd`, and
conversion to and from the `std` type; it is Unix-only and requires the `net` feature [189].
`tokio::net::windows::named_pipe` exposes `NamedPipeServer`, `NamedPipeClient`, `ServerOptions`,
`ClientOptions`, `PipeInfo`, `PipeMode` and `PipeEnd`, and is Tokio's supported Windows local
primitive [190]. Tokio has no `AF_UNIX` on Windows [188][189][190] and exposes no `SOCK_SEQPACKET`
type — `UnixStream` is stream-only and `UnixDatagram` datagram-only, so Linux seqpacket needs a
lower-level socket crate integrated with the reactor [189]. There is no built-in `SCM_RIGHTS` API;
descriptor passing requires `sendmsg`/`recvmsg` through `nix` or `rustix` around the raw descriptor
while coordinating readiness with Tokio [187][189][197].

**interprocess** (2.4.3; crates.io result dated 2026-07-31). Provides a flagship `local_socket`
abstraction plus unnamed pipes, Unix FIFOs and Windows named pipes; Unix local sockets use the
standard library's Unix sockets while Windows local sockets use named pipes [193]. Platform coverage:
explicit CI and test guarantee for Windows, Linux and macOS; explicit but incomplete CI for FreeBSD
and Android; manual testing on OpenBSD and NetBSD; and association-level support for Dragonfly,
Redox, Fuchsia, iOS, tvOS and watchOS [193]. Only Tokio is supported for async, behind an opt-in
feature; smol support is desired but not being worked on, and the 2.0 migration removed the crate's
own Unix-socket module in favour of `std` [193]. Maintenance is self-declared passive but the release
and CI are current [193]. It does not claim `SCM_RIGHTS`, so it is a portable byte-stream abstraction
rather than a descriptor-passing API [193].

**uds_windows** (1.2.1). Provides Windows `UnixListener`, `UnixStream`, `SocketAddr`,
`AcceptAddrsBuf` and extension traits analogous to the Unix `std` types [194]. It remains relevant
only because `std` has no Windows Unix sockets [186][188], and it inherits Windows `AF_UNIX`'s
limits: stream only, no datagram or seqpacket, no `socketpair`, no ancillary data [86][194].

**ipc-channel** (0.23.0 docs snapshot; Servo origin). Implements serde-serialised interprocess
channels with serialisable `IpcSender`/`IpcReceiver` plus one-shot bootstrap servers [195]. Its
backend matrix is the same split as Mojo's: Unix variants use Unix-socket descriptor passing, macOS
uses Mach ports, Windows uses named pipes, with macOS, Linux and Windows covered by CI [195].
Limitations to note: payloads serialise and deserialise so it is not zero-copy, channels are always
unbounded and `send()` never blocks — so there is no backpressure — and the one-shot server accepts
only one client, which makes it unsuitable for a system service [195].

**iceoryx2** (crates.io result dated 2026-07-08; Eclipse project). Decentralised, lock-free,
service-oriented zero-copy IPC with no central daemon, which is the explicit design response to
iceoryx v1's RouDi [196]. The API creates a `Node`, then a `service_builder(ServiceName)`, then a
`publish_subscribe`, `event`, `request_response` or blackboard service; samples are loaned, written
and sent, which is what makes the zero-copy semantics visible to the application, and pipeline is
planned [196]. It is a messaging middleware with discovery, configuration and QoS semantics rather
than a drop-in byte stream, and `[uncertain]` its supported platform matrix and the exact release
that added request/response must be verified against the release notes before being recorded [196].

**Descriptor passing and shared-memory building blocks.**

- `nix` — `nix::sys::socket::ControlMessage::ScmRights(&[RawFd])` with `sendmsg` exposes
  `SCM_RIGHTS`; the docs warn against multiple `ScmRights` control messages in one call because the
  behaviour is platform-dependent [197].
- `rustix` — memory-safe and I/O-safe POSIX-like, Linux and Winsock-like syscall wrappers, including
  ancillary send/receive message types in `rustix::net` [198].
- `passfd` — a focused Unix `SCM_RIGHTS` helper for transferring descriptors over Unix sockets [199].
  `sendfd` serves the same narrow purpose; neither can bridge Windows `AF_UNIX`'s lack of ancillary
  data [86][199].
- `shared_memory` (elast0ny) — wraps native shared-memory APIs in an OS-agnostic interface and points
  users to the companion `raw_sync` for cross-process mutex, RwLock and events; the last indexed
  release signal is 2022-03-01, so it should be treated as likely stale and verified before adoption
  [200].
- `memfd` — a pure-Rust wrapper for Linux memfd objects and seals; version 0.3.0 was released
  2020-01-11, and because `memfd_create` is Linux-specific it is not a portable shared-memory layer
  [201].
- `memmap2` — mapping machinery only; synchronisation, ownership and lifetime, namespace and crash
  cleanup all have to be designed separately [inference, from 183].

**RPC and HTTP over local sockets.** hyperium/tonic PR #2218 (opened 2025-03-12, closed 2025-03-26)
adds direct Unix-socket URI support
to tonic, accepting `unix:relative_path` and `unix:///absolute_path`, for example
`GreeterClient::connect("unix:///tmp/tonic/helloworld")`; before it, the documented pattern used
`Endpoint::connect_with_connector` with a custom Tower connector, which the API still supports for
non-HTTP transports [203]. `hyperlocal::UnixConnector` builds a Hyper client that speaks to a Unix
socket [202]. `tarpc` and `remoc` are protocol layers over an I/O transport, so their local-IPC
properties come from the stream they are handed and neither supplies `SCM_RIGHTS` or cross-platform
local-socket selection by itself [189][190][203].

**Gaps to state rather than guess.** `[uncertain]` `parity-tokio-ipc`'s current status and version
were not verified from primary sources; it must not be described as maintained without checking its
repository state, and for current Tokio local IPC the available options are Tokio named pipes on
Windows, Tokio or `std` Unix sockets on Unix, or `interprocess` for a portable abstraction
[186][189][190][193]. `[uncertain]` The `zmq` bindings' and the pure-Rust `zeromq` crate's current
`ipc://` and `inproc://` support was not verified; support must not be inferred from libzmq's
transport vocabulary, since `zmq` links libzmq while pure-Rust `zeromq` has an independent transport
implementation [112][113]. `[uncertain]` The `windows`/`windows-sys` and `named_pipe` crates'
versions and maintenance were not verified in this research; they are bindings around named pipes and
file mappings rather than a transport policy.

## 11. Recommendation candidates (facts only, no decision)

Stated as candidates with their trade-offs, as the collected evidence supports them. No choice is
made here.

**Candidate default, Linux.** `AF_UNIX` `SOCK_STREAM` on a filesystem path in a directory the server
controls. Evidence: it is what libzmq [112], NNG [117], gRPC [120], D-Bus [125], Docker [131], Podman
[135], containerd [136], the kubelet [137], PostgreSQL [146], Redis [148], SSH agent [150], Wayland
[145] and Java [152] all use. Trade-offs: needs framing, since streams have no message boundaries
[1]; leaves a stale socket file after a crash, forcing unlink-then-bind and its race [1]; 107-byte
path budget [1]; default mode is umask-derived and must be overridden [1]. `SOCK_SEQPACKET` is the
candidate variant that removes the framing layer and is available since Linux 2.6.4 with systemd
support through `ListenSequentialPacket=` [1][130], at the cost of being unavailable on macOS [37]
and Windows [86] and unsupported by Tokio's own types [189]. The abstract namespace (`@`-prefixed) is
the candidate variant that removes stale state entirely [1] at the cost of having no access control
whatsoever [1][120] and being invisible across network namespaces [17], which breaks it in containers
and unshared sandboxes [17][170].

**Candidate default, macOS.** `AF_LOCAL` `SOCK_STREAM` on a filesystem path. Trade-offs: no
`SOCK_SEQPACKET` [37], so framing is mandatory; 104-byte path budget [36], which collides with App
Group container paths [57]; `LOCAL_PEERCRED` carries no PID [35] and Apple recommends audit tokens
over PIDs anyway [52]. XPC is the candidate for role B specifically, and the only mechanism surveyed
with launchd-supervised on-demand start and crash restart plus documented code-signing requirements
on the peer [49][50][51]; its costs are that it is macOS-only, that bundled services are private to
their containing app [49], that reachability for unrelated processes means Mach service registration
[53], and that its encoding and channel are opaque [49].

**Candidate default, Windows.** Named pipes in message mode. Evidence: it is what NNG [117],
libuv/Node [160][161], .NET [153], Mojo [123], go-winio [159], Docker [132], the kubelet [137],
Tailscale [139] and the Windows OpenSSH agent [151] use. Trade-offs and benefits: message mode gives
kernel framing [69]; instances vanish with the last handle so there is no stale endpoint [68]; peer
identity comes from `ImpersonateNamedPipeClient` [75]; the default security descriptor grants
Everyone read and must be replaced [68]; `PIPE_REJECT_REMOTE_CLIENTS` is required to keep the
endpoint local [68]; the accept loop must handle the `ERROR_PIPE_CONNECTED` race [70] and
`ERROR_PIPE_BUSY` retry through `WaitNamedPipe` [71][72]; the model is completion-based (IOCP), not
readiness-based [82]; and a client should consider `SECURITY_SQOS_PRESENT` to limit impersonation
[76].

Windows `AF_UNIX` is the alternative candidate. Its appeal is a single code path with Unix [86][152];
its costs are stream-only with no datagram or seqpacket [86], no ancillary data so no handle passing
[86], no documented peer credentials [86][87], a socket file that must be `DeleteFile`d before
rebinding — reintroducing stale state [86] — the abstract namespace reported nonfunctional [86], no
`std` or Tokio support in Rust [186][188][189], no documented AppContainer support [86][103], and no
Windows-container support [111]. libzmq took this route in 4.3.3 [114] and got a stale-socket-file
bug in 4.3.4 [114].

**Candidate fallback chains.**

- *Linux, role B:* filesystem `AF_UNIX` → abstract `AF_UNIX` where no namespace boundary is crossed
  [1] → loopback TCP with a nonce/token file where the filesystem path is unavailable, the D-Bus
  `nonce-tcp` pattern [125] and Tailscale's sandboxed-Darwin pattern [140]. Socket activation removes
  the startup race entirely where systemd is present, since the manager holds the listening
  descriptor [129][130].
- *macOS, role B:* `AF_LOCAL` in an accessible container → XPC or Mach service with entitlement or
  App Group when sandboxed [53][58][66] → loopback TCP on `127.0.0.1:0` with a random token and a
  same-user proof file, which is Tailscale's actual sandboxed implementation [140]. Loopback does not
  appear to trigger the local-network prompt [59], though that rests on a forum answer [59].
- *Windows, role B:* named pipe with an explicit DACL → loopback TCP with a token for packaged peers
  after `LoopbackExempt` and `privateNetworkClientServer` [100][105] → App Services where both peers
  are packaged [107].
- *Any platform, role A:* an inherited unnamed endpoint is strictly better than a named one —
  `socketpair()` on Unix [1], an inherited or duplicated handle on Windows [89] — because it needs no
  namespace, no permissions, no cleanup and no peer authentication. This is Cap'n Proto's documented
  approach [142] and Mojo's `PlatformChannel` design, with `NamedPlatformChannel` reserved for the
  case where a handle cannot be transferred [123].
- *Bulk payloads, any platform:* pass a memfd with seals on Linux [6][7], a Mach memory entry on
  macOS [48], or a file-mapping handle on Windows [89], and keep the socket or pipe for control and
  notification. There is no kernel zero-copy path for `AF_UNIX` [22][23][24][25], so this is the only
  route to zero-copy, and it is what Wayland [145], iceoryx [143] and Mojo [123][124] do.

**Trade-off axes to weigh.** Condensed from the evidence above; no axis is decisive alone.

| Axis | Linux | macOS | Windows |
| --- | --- | --- | --- |
| Kernel framing | `SOCK_SEQPACKET` [1] | none [37] | message-mode pipe [69] |
| Handle passing | `SCM_RIGHTS` [1] | `SCM_RIGHTS` [35] | `DuplicateHandle` only; not on `AF_UNIX` [86][89] |
| Peer identity | strongest [1][3][28][29] | no PID; audit token advised [35][52] | pipe token [75]; none on `AF_UNIX` [86][87] |
| Stale endpoint | pathname yes, abstract no [1] | yes [35] | pipe no [68]; `AF_UNIX` yes [86] |
| Sandbox survivability | pathname on shared writable path [19][172] | XPC/App Group [53][66] | pipe with explicit DACL [108] |
| Event model | readiness [13] | readiness [46] | completion [82] — does not unify |
| Rust support | first-class in Tokio [189] | first-class in Tokio [189] | pipes first-class [190]; `AF_UNIX` in neither `std` nor Tokio [186][188] |

Performance, all platforms: Unix sockets roughly twice loopback TCP at small sizes, shared memory
roughly an order of magnitude above Unix sockets, on benchmarks too methodologically varied to
average [149][181][182][183]. `SCM_RIGHTS` remains unstable in `std` [187].

## 12. Sources

1. `unix(7) — Linux manual page` — https://man7.org/linux/man-pages/man7/unix.7.html — Linux man-pages 6.18, 2026-02-08. AF_UNIX types, addressing, permissions, ancillary data, credentials, cleanup.
2. `cmsg(3) — Linux manual page` — https://man7.org/linux/man-pages/man3/cmsg.3.html — man-pages 6.18, 2026-02-08. Ancillary-data macros.
3. `socket(7) — Linux manual page` — https://man7.org/linux/man-pages/man7/socket.7.html — man-pages 6.18, 2026-02-08. Generic socket options, `SO_PEERSEC`, buffer maxima, nonblocking behaviour.
4. `pipe(7) — Linux manual page` — https://man7.org/linux/man-pages/man7/pipe.7.html — man-pages 6.18, 2026-02-08. Pipe capacity, `PIPE_BUF`, EOF/SIGPIPE.
5. `fifo(7) — Linux manual page` — https://man7.org/linux/man-pages/man7/fifo.7.html — man-pages 6.18, 2026-02-08. FIFO open semantics.
6. `memfd_create(2) — Linux manual page` — https://man7.org/linux/man-pages/man2/memfd_create.2.html — man-pages 6.18, 2026-02-08. memfd, flags, sealing producer pattern.
7. `F_ADD_SEALS(2const) — Linux manual page` — https://man7.org/linux/man-pages/man2/F_ADD_SEALS.2const.html — man-pages 6.18, 2025-07-20. File seals and kernel versions.
8. `shm_open(3) — Linux manual page` — https://man7.org/linux/man-pages/man3/shm_open.3.html — man-pages 6.18, 2026-02-08. POSIX shared memory, `/dev/shm`, naming, cleanup.
9. `shmget(2) — Linux manual page` — https://man7.org/linux/man-pages/man2/shmget.2.html — man-pages 6.18, 2026-02-08. System V shared memory and limits.
10. `mmap(2) — Linux manual page` — https://man7.org/linux/man-pages/man2/mmap.2.html — man-pages 6.18, 2026-02-08. `MAP_SHARED`, `MAP_SHARED_VALIDATE`.
11. `futex(2) — Linux manual page` — https://man7.org/linux/man-pages/man2/futex.2.html — man-pages 6.18, 2026-02-08. Cross-process futexes, `FUTEX_PRIVATE_FLAG`, `FUTEX_OWNER_DIED`.
12. `eventfd(2) — Linux manual page` — https://man7.org/linux/man-pages/man2/eventfd.2.html — man-pages 6.18, 2026-02-08. eventfd and `EFD_SEMAPHORE`.
13. `epoll(7) — Linux manual page` — https://man7.org/linux/man-pages/man7/epoll.7.html — man-pages 6.18, 2026-02-08. Readiness model.
14. `io_uring(7) — Linux manual page` — https://man7.org/linux/man-pages/man7/io_uring.7.html — man-pages 6.18, 2026-02-08. Completion semantics and ordering.
15. `io_uring_setup(2) — Linux manual page` — https://man7.org/linux/man-pages/man2/io_uring_setup.2.html — man-pages 6.18, 2026-02-08. Linux 5.1 introduction, SQPOLL history.
16. `pidfd_open(2) — Linux manual page` — https://man7.org/linux/man-pages/man2/pidfd_open.2.html — man-pages 6.18, 2026-02-08. pidfd semantics versus PID reuse.
17. `network_namespaces(7) — Linux manual page` — https://man7.org/linux/man-pages/man7/network_namespaces.7.html — man-pages 6.18, 2026-02-08. Isolation of ports and the abstract AF_UNIX namespace.
18. `ipc_namespaces(7) — Linux manual page` — https://man7.org/linux/man-pages/man7/ipc_namespaces.7.html — man-pages 6.15, 2025-05-17. SysV IPC and POSIX message-queue isolation.
19. `mount_namespaces(7) — Linux manual page` — https://man7.org/linux/man-pages/man7/mount_namespaces.7.html — man-pages 6.15, 2025-05-17. Mount-view isolation.
20. `tcp(7) — Linux manual page` — https://man7.org/linux/man-pages/man7/tcp.7.html — man-pages 6.18, 2026-02-08. Loopback TCP properties and buffer sysctls.
21. `udp(7) — Linux manual page` — https://man7.org/linux/man-pages/man7/udp.7.html — man-pages 6.18, 2026-02-08. UDP unreliability and ephemeral port range.
22. `splice(2) — Linux manual page` — https://man7.org/linux/man-pages/man2/splice.2.html — man-pages 6.18, 2026-02-08. Pipe requirement for splice.
23. `vmsplice(2) — Linux manual page` — https://man7.org/linux/man-pages/man2/vmsplice.2.html — man-pages 6.18, 2026-02-08. Direction asymmetry, `SPLICE_F_GIFT`.
24. `sendfile(2) — Linux manual page` — https://man7.org/linux/man-pages/man2/sendfile.2.html — man-pages 6.18, 2026-02-08. Input cannot be a socket; size limit.
25. `MSG_ZEROCOPY — Linux kernel documentation` — https://docs.kernel.org/networking/msg_zerocopy.html — accessed 2026-09-08; feature introduced Linux 4.14. Supported socket families exclude AF_UNIX.
26. `io_uring_prep_send_zc(3) — liburing manual` — https://man7.org/linux/man-pages/man3/io_uring_prep_send_zc.3.html — liburing 2.3, 2022-09-06; upstream snapshot 2026-05-24.
27. `io_uring_disabled — kernel sysctl documentation` — https://docs.kernel.org/admin-guide/sysctl/kernel.html#io-uring-disabled — Linux 6.6 (2023-10-29); accessed 2026-09-08.
28. `af_unix: add SO_PEERGROUPS socket option` — https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=f90497a16e434c2211c66e3de8e77b17868382b8 — upstream commit 2018-05-22, Linux 4.18 development.
29. `af_unix: Add SO_PEERPIDFD and SCM_PIDFD` — https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=24e0c8dce0f02bf4c4a89ae6436ad2a730391a1d — upstream 2023-05-23, Linux 6.5 development.
30. `net/unix/garbage.c` — https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/net/unix/garbage.c — Linux v6.11, 2024-09-15. In-flight descriptor cycle collection; Kuniyuki Iwashima 2024 GC rework.
31. `apparmor: af_unix mediation` — https://gitlab.com/apparmor/apparmor/-/blob/v2.11.95/kernel-patches/v4.13/0017-UBUNTU-SAUCE-apparmor-af_unix-mediation.patch — AppArmor project kernel patch, 2017-09-11. Abstract-address policy notation.
32. `systemd.exec(5)` — https://www.freedesktop.org/software/systemd/man/261/systemd.exec.html — systemd 261.2. `RestrictAddressFamilies=`, `PrivateNetwork=`.
33. `Our learnings from 42 Linux kernel exploits` — https://security.googleblog.com/2023/06/our-learnings-from-42-linux-system.html — Google Security Blog, 2023-06-15. io_uring disabled in ChromeOS, Android, Google production.
34. `SOCKET(2)` (Darwin) — https://manp.gs/mac/2/socket — man page dated 2015-03-18. Darwin socket types, `PF_LOCAL`, SIGPIPE.
35. `UNIX(4)` (Darwin) — https://manp.gs/mac/4/unix — man page dated 1993-06-09. AF_LOCAL types, 104-char paths, SCM_RIGHTS, `LOCAL_PEERCRED`, cleanup.
36. Apple XNU `bsd/sys/un.h` — https://raw.githubusercontent.com/apple-oss-distributions/xnu/main/bsd/sys/un.h — `main`, accessed 2026-09-08. `sun_path[104]`, `SOL_LOCAL` option numbers.
37. Apple XNU `bsd/kern/uipc_usrreq.c` — https://raw.githubusercontent.com/apple-oss-distributions/xnu/main/bsd/kern/uipc_usrreq.c — `main`, accessed 2026-09-08. `SEQPACKET, RDM` listed as TODO; `UIPC_MAX_CMSG_FD` 512.
38. Apple XNU `bsd/sys/socket.h` — https://raw.githubusercontent.com/apple-oss-distributions/xnu/main/bsd/sys/socket.h — `main`, accessed 2026-09-08. `SO_NOSIGPIPE` value, `SOCK_SEQPACKET` constant.
39. `PIPE(2)` (Darwin) — https://manp.gs/mac/2/pipe — man page dated 2011-02-17. Pipe lifetime, EOF, `F_SETNOSIGPIPE`.
40. `GETPEEREID(3)` (Darwin) — https://manp.gs/mac/3/getpeereid — man page dated 2001-07-15.
41. `SHM_OPEN(2)` (Darwin) — https://manp.gs/mac/2/shm_open — man page dated 2008-08-29. No filesystem entry, `ENAMETOOLONG`, `FD_CLOEXEC`, persistence.
42. Apple XNU `bsd/sys/posix_shm.h` — https://raw.githubusercontent.com/apple-oss-distributions/xnu/main/bsd/sys/posix_shm.h — `main`, accessed 2026-09-08. `PSHMNAMLEN` 31.
43. `SHMGET(2)` (Darwin) — https://manp.gs/mac/2/shmget — man page dated 1995-08-17.
44. Apple XNU `bsd/sys/pipe.h` — https://raw.githubusercontent.com/apple-oss-distributions/xnu/main/bsd/sys/pipe.h — `main`, accessed 2026-09-08. `PIPE_SIZE`, `BIG_PIPE_SIZE`, `PIPE_MINDIRECT`.
45. Apple XNU `bsd/sys/syslimits.h` — https://github.com/apple-oss-distributions/xnu/blob/main/bsd/sys/syslimits.h — `main`, accessed 2026-09-08. `PIPE_BUF` 512.
46. `kevent(2)` — https://leancrew.com/all-this/man/man2/kevent.html — macOS man-page mirror, accessed 2026-09-08. `EVFILT_MACHPORT` semantics.
47. Apple XNU `bsd/sys/event.h` — https://github.com/apple/darwin-xnu/blob/main/bsd/sys/event.h — `main`, accessed 2026-09-08. `EVFILT_MACHPORT` value.
48. `Mach Overview, Kernel Programming Guide` — https://developer.apple.com/library/archive/documentation/Darwin/Conceptual/KernelProgramming/Mach/Mach.html — Apple, updated 2013-08-08. Mach ports, rights, messages, memory entries, bootstrap, semaphores.
49. `Creating XPC Services, Daemons and Services Programming Guide` — https://developer.apple.com/library/archive/documentation/MacOSX/Conceptual/BPSystemStartup/Chapters/CreatingXPCServices.html — Apple, updated 2016-09-13.
50. `xpc_connection_set_peer_code_signing_requirement` — https://developer.apple.com/documentation/xpc/xpc_connection_set_peer_code_signing_requirement(_:_:) — Apple API reference, macOS 12.0+; accessed 2026-09-08.
51. `Validating Signature Of XPC Process` — https://developer.apple.com/forums/thread/681053 — Apple DTS forum post, updated 2023-12-16. macOS 11/13/14.4 code-signing APIs.
52. `XPC restricted to processes with the same code signing?` — https://developer.apple.com/forums/thread/72881 — Apple DTS forum post, updated 2022-01-31. PID reuse, audit tokens, `kSecGuestAttributeAudit`.
53. `mach ports and app hardening (on macOS)` — https://developer.apple.com/forums/thread/112427 — Apple DTS forum post, updated 2020-06-21. `bootstrap_check_in`/`bootstrap_look_up`, mach-lookup exception, App-Group service naming.
54. `Efficiently sending data from an XPC Service to a client` — https://developer.apple.com/forums/thread/126716 — Apple DTS forum post, accessed 2026-09-08. Inline cutoff estimate, `DispatchData`.
55. `DispatchSource` — https://developer.apple.com/documentation/dispatch/dispatchsource — Apple API reference, accessed 2026-09-08. Read/write/Mach-receive/Mach-send sources.
56. `os_sync_wait_on_address` — https://developer.apple.com/documentation/os/os_sync_wait_on_address — Apple API reference, accessed 2026-09-08.
57. `Enabling App Sandbox, Entitlement Key Reference` — https://developer.apple.com/library/archive/documentation/Miscellaneous/Reference/EntitlementKeyReference/Chapters/EnablingAppSandbox.html — Apple, updated 2017-03-27. App groups, group container path, network entitlements.
58. `App Sandbox Temporary Exception Entitlements` — https://developer.apple.com/library/archive/documentation/Miscellaneous/Reference/EntitlementKeyReference/Chapters/AppSandboxTemporaryExceptionEntitlements.html — Apple, 2017-03-27. `temporary-exception.mach-lookup.global-name`.
59. `Is the local networks privacy impacting 127.0.0.1?` — https://developer.apple.com/forums/thread/650810 — Apple engineer forum answer, 2020. Loopback not classified as local network.
60. `network extension, app groups, unix domain socket` — https://developer.apple.com/forums/thread/133543 — Apple DTS forum post, 2020-06-20. Sandbox cannot connect to arbitrary UDS; App-Group UDS works.
61. `Give sandboxed app access to /var …` — https://forums.developer.apple.com/forums/thread/712997 — Apple DTS forum post, 2022-08-30. Filesystem temporary exceptions do not cover sockets.
62. `sem_t in sandbox app` — https://developer.apple.com/forums/thread/756420 — Apple DTS forum post, 2024 (thread date precision limited to the year at the source); accessed 2026-09-08. POSIX semaphore `GGG/NNN` naming in a sandbox.
63. `How can I enable local socket under App Sandbox?` — https://developer.apple.com/forums/thread/126059 — Apple DTS forum post, 2020-06-21. No special entitlement needed for AF_UNIX itself.
64. `Is opening Shared memory allowed in …` — https://developer.apple.com/forums/thread/719897 — Apple DTS forum post, accessed 2026-09-08. POSIX shared memory recommended via App Group.
65. `Shared-memory pthread condition variable not working` — https://developer.apple.com/forums/thread/692476 — Apple Developer Forums, 2021-10-08. macOS 11.6 multi-waiter report.
66. `IPC within app group` — https://developer.apple.com/forums/thread/689642 — Apple DTS forum post, accessed 2026-09-08; with `IPC in iOS, two independant applications` — https://developer.apple.com/forums/thread/659531 — 2020-09-08, and `iOS Inter-process communication` — https://developer.apple.com/forums/thread/755758.
67. `Configuring app groups` — https://developer.apple.com/documentation/xcode/configuring-app-groups — Apple, accessed 2026-09-08; with `App Extension Programming Guide: Handling Common Scenarios` — https://developer.apple.com/library/archive/documentation/General/Conceptual/ExtensibilityPG/ExtensionScenarios.html — archived.
68. `CreateNamedPipeW function (namedpipeapi.h)` — https://learn.microsoft.com/en-us/windows/win32/api/namedpipeapi/nf-namedpipeapi-createnamedpipew — Microsoft Learn, updated 2024-11-20. Modes, namespace, instances, buffers, default SD, lifetime, `FILE_FLAG_FIRST_PIPE_INSTANCE`, 1709 app-container restriction.
69. `Named Pipe Type, Read, and Wait Modes` — https://learn.microsoft.com/en-us/windows/win32/ipc/named-pipe-type-read-and-wait-modes — Microsoft Learn, doc date 2018-05-31, updated 2025-04-15. Message versus byte mode, `ERROR_MORE_DATA`, wait modes.
70. `ConnectNamedPipe function (namedpipeapi.h)` — https://learn.microsoft.com/en-us/windows/win32/api/namedpipeapi/nf-namedpipeapi-connectnamedpipe — Microsoft Learn, updated 2024-11-20. `ERROR_PIPE_CONNECTED` race, overlapped requirements.
71. `Named Pipe Client` — https://learn.microsoft.com/en-us/windows/win32/ipc/named-pipe-client — Microsoft Learn, updated 2025-04-15. `ERROR_PIPE_BUSY` and retry.
72. `WaitNamedPipeW function (namedpipeapi.h)` — https://learn.microsoft.com/en-us/windows/win32/api/namedpipeapi/nf-namedpipeapi-waitnamedpipew — Microsoft Learn, 2023-02-01.
73. `Named Pipe Operations` — https://learn.microsoft.com/en-us/windows/win32/ipc/named-pipe-operations — Microsoft Learn, updated 2025-04-15. `PeekNamedPipe`, `TransactNamedPipe`, shutdown order.
74. `Named Pipe Security and Access Rights` — https://learn.microsoft.com/en-us/windows/win32/ipc/named-pipe-security-and-access-rights — Microsoft Learn, updated 2025-04-15. DACL checks, `FILE_CREATE_PIPE_INSTANCE`, logon SID recommendation.
75. `ImpersonateNamedPipeClient function (namedpipeapi.h)` — https://learn.microsoft.com/en-us/windows/win32/api/namedpipeapi/nf-namedpipeapi-impersonatenamedpipeclient — Microsoft Learn, updated 2025-07-01. Conditions, `SeImpersonatePrivilege`, failure handling.
76. `Impersonating a Named Pipe Client` — https://learn.microsoft.com/en-us/windows/win32/ipc/impersonating-a-named-pipe-client — Microsoft Learn, updated 2025-03-12. `RevertToSelf`, client SQOS, default level.
77. `SetSecurityDescriptorDacl function (securitybaseapi.h)` — https://learn.microsoft.com/en-us/windows/win32/api/securitybaseapi/nf-securitybaseapi-setsecuritydescriptordacl — Microsoft Learn, updated 2025-07-01. NULL DACL allows all access; empty DACL denies.
78. `GetNamedPipeClientProcessId function (winbase.h)` — https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-getnamedpipeclientprocessid — Microsoft Learn, updated 2025-07-01. Vista/Server 2008 minimum; 1709 app-container note.
79. `GetNamedPipeClientComputerNameA function (winbase.h)` — https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-getnamedpipeclientcomputernamea — Microsoft Learn, updated 2025-07-01.
80. `GetNamedPipeServerProcessId function (winbase.h)` — https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-getnamedpipeserverprocessid — Microsoft Learn, updated 2025-07-01.
81. `Named Pipe Server Using Overlapped I/O` — https://learn.microsoft.com/en-us/windows/win32/ipc/named-pipe-server-using-overlapped-i-o — Microsoft Learn, updated 2025-03-12. `ERROR_IO_PENDING`, `GetOverlappedResult`, one-event caveat.
82. `GetQueuedCompletionStatusEx function (ioapiset.h)` — https://learn.microsoft.com/en-us/windows/win32/api/ioapiset/nf-ioapiset-getqueuedcompletionstatusex — Microsoft Learn, updated 2024-08-22. Completion-based model.
83. `CancelIoEx function (IoAPI.h)` — https://learn.microsoft.com/en-us/windows/win32/fileio/cancelioex-func — Microsoft Learn, updated 2021-01-07. Cancellation races, `OVERLAPPED` lifetime.
84. `SetFileCompletionNotificationModes function (winbase.h)` — https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-setfilecompletionnotificationmodes — Microsoft Learn, updated 2024-02-22.
85. `WaitForMultipleObjects function (synchapi.h)` — https://learn.microsoft.com/en-us/windows/win32/api/synchapi/nf-synchapi-waitformultipleobjects — Microsoft Learn, updated 2025-07-01. `MAXIMUM_WAIT_OBJECTS`, `WAIT_ABANDONED`.
86. `AF_UNIX comes to Windows` — https://devblogs.microsoft.com/commandline/af_unix-comes-to-windows/ — Microsoft Windows Command Line blog, published 2017-12-19, modified 2019-02-18. Build 17063; stream-only; no ancillary data, no `socketpair`; reparse point; `DeleteFile` before rebind; abstract addresses.
87. `Interprocess communications - Win32 apps` — https://learn.microsoft.com/en-us/windows/win32/ipc/interprocess-communications — Microsoft Learn, updated 2025-04-15; AF_UNIX note dated 2024-02-13. Current AF_UNIX statement is still "Beginning in Windows Insider Build 17063".
88. `afunix.h` (Windows SDK 10.0.16299.0) — https://github.com/tpn/winsdk-10/blob/master/Include/10.0.16299.0/shared/afunix.h — SDK header snapshot, 2017-10. `UNIX_PATH_MAX` 108.
89. `CreateFileMappingW function (memoryapi.h)` — https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-createfilemappingw — Microsoft Learn, updated 2025-07-01. Pagefile-backed sections, `Global\` privilege, view lifetime, `SEC_LARGE_PAGES`.
90. `Kernel object namespaces` — https://learn.microsoft.com/en-us/windows/win32/termserv/kernel-object-namespaces — Microsoft Learn, updated 2024-05-07/2025-04-15. `Global\` and `Local\` semantics.
91. `Interprocess Synchronization` — https://learn.microsoft.com/en-us/windows/win32/sync/interprocess-synchronization — Microsoft Learn, updated 2025-04-15. Named events, mutexes, semaphores, timers.
92. `WaitOnAddress function (synchapi.h)` — https://learn.microsoft.com/en-us/windows/win32/api/synchapi/nf-synchapi-waitonaddress — Microsoft Learn, accessed 2026-09-08. Wakes only threads in the same process.
93. `About Mailslots` — https://learn.microsoft.com/en-us/windows/win32/ipc/about-mailslots — Microsoft Learn, updated 2025-03-12. Temporary data, write-by-name, 424-byte network limit.
94. `The beginning of the end of Remote Mailslots as part of Windows Insider` — https://techcommunity.microsoft.com/blog/filecab/the-beginning-of-the-end-of-remote-mailslots-as-part-of-windows-insider/3762048 — Microsoft, 2023-03-15. Build 25314 default-disable of the remote protocol.
95. `RpcServerUseProtseqEp function (rpcdce.h)` — https://learn.microsoft.com/en-us/windows/win32/api/rpcdce/nf-rpcdce-rpcserveruseprotseqep — Microsoft Learn, updated 2024-02-22. `ncalrpc`, security descriptor caveat.
96. `Making the Server Available on the Network` — https://learn.microsoft.com/en-us/windows/win32/rpc/making-the-server-available-on-the-network — Microsoft Learn, updated 2023-10-17. `ncalrpc` for local calls.
97. `LPC (Local procedure calls) Part 1 architecture` — https://learn.microsoft.com/en-us/archive/blogs/ntdebugging/lpc-local-procedure-calls-part-1-architecture — archived Microsoft NT Debugging blog, accessed 2026-09-08. ALPC introduced in Vista; `ncalrpc` as the Win32 route; `NtAlpc*` undocumented.
98. `The default dynamic port range for TCP/IP has changed in Windows Vista and Windows Server 2008` — https://learn.microsoft.com/en-us/troubleshoot/windows-server/networking/default-dynamic-port-range-tcpip-chang — Microsoft Learn, updated 2026-02-12.
99. `SIO_LOOPBACK_FAST_PATH Control Code` — https://learn.microsoft.com/en-us/windows/win32/winsock/sio-loopback-fast-path — Microsoft Learn, accessed 2026-09-08.
100. `Interprocess communication (IPC) - UWP applications` — https://learn.microsoft.com/en-us/windows/uwp/communication/interprocess-communication — Microsoft Learn, updated 2024-01-17. Loopback blocked by default, `LoopbackExempt`, `privateNetworkClientServer`, `-is` since 1607, App Services guidance.
101. `Launch an AppContainer` — https://learn.microsoft.com/en-us/windows/win32/secauthz/implementing-an-appcontainer — Microsoft Learn, accessed 2026-09-08. AppContainers run at Low IL.
102. `Mandatory Integrity Control` — https://learn.microsoft.com/en-us/windows/win32/secauthz/mandatory-integrity-control — Microsoft Learn, 2025-07-08. `SYSTEM_MANDATORY_LABEL_ACE`, no-write-up.
103. `Sharing named objects` — https://learn.microsoft.com/en-us/windows/apps/develop/communication/sharing-named-objects — Microsoft Learn, accessed 2026-09-08. AppContainer named-object isolation, `GetAppContainerNamedObjectPath`.
104. `SeImpersonatePrivilege and SeCreateGlobalPrivilege` — https://learn.microsoft.com/en-us/troubleshoot/windows-server/windows-security/seimpersonateprivilege-secreateglobalprivilege — Microsoft Learn, accessed 2026-09-08. Privilege introduced in Windows 2000 SP4.
105. `Communicating with Localhost` — https://learn.microsoft.com/en-us/windows/iot-core/develop-your-app/loopback — Microsoft Learn, 2023-04-03. `CheckNetIsolation LoopbackExempt -a`.
106. `Troubleshooting UWP App Connectivity Issues in Windows Firewall` — https://learn.microsoft.com/en-us/windows/security/operating-system-security/network-security/windows-firewall/troubleshooting-uwp-firewall — Microsoft Learn, accessed 2026-09-08. Default block filters.
107. `Windows.ApplicationModel.AppService Namespace` — https://learn.microsoft.com/en-us/uwp/api/windows.applicationmodel.appservice — Microsoft Learn, accessed 2026-09-08.
108. `UWP pipe client and C++ Fulltrustprocess pipe server - Access to the path is denied` — https://learn.microsoft.com/en-us/answers/questions/2120530/uwp-pipe-client-and-c-fulltrustprocess-pipe-server — Microsoft Q&A, 2024-11-18. `ALL APPLICATION PACKAGES` requirement in the pipe DACL.
109. `ObRegisterCallbacks function (wdm.h)` — https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/wdm/nf-wdm-obregistercallbacks — Microsoft Learn, 2022-02-24.
110. `_OB_PRE_CREATE_HANDLE_INFORMATION (wdm.h)` — https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/wdm/ns-wdm-_ob_pre_create_handle_information — Microsoft Learn, 2022-02-25. Callbacks may only restrict `DesiredAccess`.
111. `Use Windows HostProcess containers - Azure Kubernetes Service` — https://learn.microsoft.com/en-us/azure/aks/use-windows-hpc — Microsoft Learn, accessed 2026-09-08. Named-pipe mounts and Unix-domain sockets not directly supported; host `\\.\pipe\*` paths accessible.
112. `zmq_ipc(7)` — https://libzmq.readthedocs.io/en/latest/zmq_ipc.html — libzmq manual, last updated 2026-07-26. UDS-only note, bind override, `@` abstract on Linux, 113/107-character limit, wild-card bind.
113. `zmq_inproc(7)` — https://libzmq.readthedocs.io/en/latest/zmq_inproc.html — libzmq manual, last updated 2026-07-26. Zero I/O threads, 256-character names, connect-before-bind fixed in 4.0.
114. `libzmq NEWS` — https://raw.githubusercontent.com/zeromq/libzmq/master/NEWS — accessed 2026-09-08. 4.3.3, released 2020/09/07: "Fixed #3691 — added support for IPC on Windows 10 via AF_UNIX" and "Fixed #1808 — use AF_UNIX instead of TCP for the internal socket on Windows 10"; 4.3.4, released 2021/01/17: "Fixed #4086 — excessive amount of socket files left behind in Windows TMP directory".
115. `zmq_setsockopt(3)` — https://libzmq.readthedocs.io/en/latest/zmq_setsockopt.html — libzmq manual, accessed 2026-09-08. `ZMQ_SNDHWM`/`ZMQ_RCVHWM` definitions and approximation caveats.
116. `ZeroMQ Guide, Chapter 2: Sockets and Patterns` — https://zguide.zeromq.org/docs/chapter2/ — accessed 2026-09-08. inproc bind-before-connect and its 4.0 fix, HWM summing over inproc, ipc rebind-on-crash behaviour, attached/detached thread advice.
117. `nng_ipc(7)` — https://nng.nanomsg.org/man/v1.10.0/nng_ipc.7.html — NNG v1.10.0 (also v1.8.0). POSIX UDS / Windows Named Pipes, `\\.\pipe\` prefix, `unix://` alias reservation, 122-byte legacy cap, abstract names, auto-bind.
118. `nng_inproc(7)` — https://nng.nanomsg.org/man/v1.8.0/nng_inproc.7.html — NNG v1.8.0. Avoids copying; `NNG_OPT_RECVMAXSZ` ignored because peers are implicitly trusted.
119. `nng_ipc_options(5)` — https://nng.nanomsg.org/man/v1.1.0/nng_ipc_options.5.html — NNG v1.1.0. `NNG_OPT_IPC_PERMISSIONS`, `NNG_OPT_IPC_SECURITY_DESCRIPTOR`, peer UID/GID/PID and the fd-passing PID caveat.
120. `gRPC Name Resolution` — https://github.com/grpc/grpc/blob/master/doc/naming.md — grpc/grpc `master`, accessed 2026-09-08. `unix:`, `unix-abstract:` (no permissions apply), `vsock:`.
121. `Windows AF_UNIX/create_from_fd support` — https://github.com/grpc/grpc/issues/22285 — opened 2020-03-10.
122. `I could use a Windows named pipe transport` — https://github.com/grpc/grpc/issues/13447 — opened 2017-11-17.
123. `Mojo C++ Platform API` — https://chromium.googlesource.com/chromium/src/+/HEAD/mojo/public/cpp/platform/README.md — Chromium HEAD, accessed 2026-09-08. `PlatformChannel`, `NamedPlatformChannel`.
124. `Mojo Core Overview` — https://chromium.googlesource.com/chromium/src/+/master/mojo/core/README.md — Chromium master, accessed 2026-09-08. Nodes, ports, platform handles including Mach ports and Windows handles.
125. `D-Bus Specification` — https://dbus.freedesktop.org/doc/dbus-specification.html — current specification, accessed 2026-09-08. `unix:tmpdir=`/`unix:dir=` listen-only, `nonce-tcp` nonce file, `unixexec:`.
126. `dbus-daemon(1)` — https://dbus.freedesktop.org/doc/dbus-daemon.1.html — current manual, accessed 2026-09-08. `EXTERNAL`-only recommendation and default.
127. `dbus-launch(1)` — https://dbus.freedesktop.org/doc/dbus-launch.1.html — current manual, accessed 2026-09-08. `--autolaunch` behaviour.
128. `sd_notify(3)` — https://www.freedesktop.org/software/systemd/man/latest/sd_notify.html — systemd latest manual, accessed 2026-09-08. `NOTIFY_SOCKET` `/` versus `@`, `SCM_CREDENTIALS`.
129. `sd_listen_fds(3)` — https://www.freedesktop.org/software/systemd/man/latest/sd_listen_fds.html — systemd 261.2, accessed 2026-09-08. `SD_LISTEN_FDS_START` 3, environment variables, `sd_is_socket*`, names since systemd 227.
130. `systemd.socket(5)` — https://www.freedesktop.org/software/systemd/man/latest/systemd.socket.html — systemd 261.2, accessed 2026-09-08. `ListenStream=`/`ListenDatagram=`/`ListenSequentialPacket=`, `@` abstract form.
131. `Docker daemon configuration overview` — https://docs.docker.com/engine/daemon/ — Docker Docs, Docker Engine 29.5 statement; accessed 2026-09-08. `/var/run/docker.sock`, optional Windows AF_UNIX, `--default-shm-size` 64 MiB.
132. `docker CLI reference` — https://docs.docker.com/reference/cli/docker/ — Docker Docs, accessed 2026-09-08. `npipe:////./pipe/docker_engine` versus `unix:///var/run/docker.sock`.
133. `Protect the Docker daemon socket` — https://docs.docker.com/engine/security/protect-access/ — Docker Docs, accessed 2026-09-08. Non-networked Unix socket; client key equals root on the daemon host.
134. `Understand permission requirements for Windows` — https://docs.docker.com/desktop/setup/install/windows-permission-requirements/ — Docker Docs, accessed 2026-09-08. Non-privileged named pipes limited to the launching user, Administrators and `LOCALSYSTEM`.
135. `podman-system-service(1)` — https://docs.podman.io/en/latest/markdown/podman-system-service.1.html — current Podman documentation, accessed 2026-09-08; page history records 2020-01 and 2020-11. Rootless/rootful socket paths and the root-equivalence warning.
136. `containerd-config.toml(5)` — https://github.com/containerd/containerd/blob/main/docs/man/containerd-config.toml.5.md — dated 2022-04-05, config version 4. `/run/containerd/containerd.sock` and the ttrpc default.
137. `kubelet command reference` — https://kubernetes.io/docs/reference/command-line-tools-reference/kubelet/ — Kubernetes, modified 2026-08-26. `--container-runtime-endpoint` default; `npipe` and TCP on Windows.
138. `Device Plugins` — https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/ — Kubernetes, 2026-04-20; with `Local Files And Paths Used By The Kubelet` — https://kubernetes.io/docs/reference/node/kubelet-files/. Hard-coded `/var/lib/kubelet/device-plugins/`.
139. `safesocket.go` — https://github.com/tailscale/tailscale/blob/main/safesocket/safesocket.go — Tailscale `main`, accessed 2026-09-08. Unix socket or localhost TCP; named pipe on Windows; peer-credential platform list and `HasUnixSocketIdentity`.
140. `safesocket_darwin.go` — https://github.com/tailscale/tailscale/blob/main/safesocket/safesocket_darwin.go — Tailscale `main`, accessed 2026-09-08. `tcp4` on `127.0.0.1:0`, 10-byte hex token, `sameuserproof`, `/Library/Tailscale` token file mode `0640`.
141. `win: safesocket should use impersonation token for checking client access` — https://github.com/tailscale/tailscale/issues/7730 — opened 2023-03-29. PID-based check and the impersonation fix.
142. `Cap'n Proto: C++ RPC` — https://capnproto.org/cxxrpc.html — accessed 2026-09-08; documents version 0.4 behaviour. Pipes or socketpairs for inter-thread RPC.
143. `Eclipse iceoryx README` — https://github.com/eclipse-iceoryx/iceoryx/blob/main/README.md — `main`, accessed 2026-09-08. True zero-copy shared memory, constant latency claim, OS support list.
144. `Eclipse iceoryx2 README` — https://github.com/eclipse-iceoryx/iceoryx2/blob/main/README.md — `main`, accessed 2026-09-08. Latency claim as a project statement.
145. `Wayland Protocol and Model of Operation` — https://wayland.freedesktop.org/docs/book/Protocol.html — accessed 2026-09-08; documents the Wayland 1.15 change. `wayland-0`, `WAYLAND_DISPLAY`/`WAYLAND_SOCKET`, fd ancillary ordering and queuing requirement.
146. `PostgreSQL 18: 19.3 Connections and Authentication` — https://www.postgresql.org/docs/18/runtime-config-connection.html — PostgreSQL 18.6, published 2026-08-13. `unix_socket_directories`, `.s.PGSQL.nnnn`, `unix_socket_permissions` 0777, abstract `@` form, Windows caveats.
147. `PostgreSQL 18: 20.9 Peer Authentication` — https://www.postgresql.org/docs/18/auth-peer.html — PostgreSQL 18.6, published 2026-08-13.
148. `Redis configuration file example` — https://redis.io/docs/latest/operate/oss_and_stack/management/config-file/ — accessed 2026-09-08. `unixsocket`, `unixsocketperm`.
149. `Redis benchmark` — https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/ — accessed 2026-09-08. Roughly 50% more throughput on UDS than loopback TCP; advantage decreases with long pipelines.
150. `ssh-agent(1)` — https://man.openbsd.org/ssh-agent — OpenBSD-current, dated 2026-05-27. Randomised `$HOME/.ssh/agent/s.*` socket, `SSH_AUTH_SOCK`, owner-only readability, automatic removal, root/same-user caveat.
151. `wmain_common.c` — https://raw.githubusercontent.com/PowerShell/openssh-portable/latestw_all/contrib/win32/win32compat/wmain_common.c — Win32-OpenSSH `latestw_all`, accessed 2026-09-08. `SSH_AUTH_SOCK=\\.\pipe\openssh-ssh-agent`.
152. `JEP 380: Unix-Domain Socket Channels` — https://openjdk.org/jeps/380 — Java 16; created 2020-02-06, updated 2021-06-29. Feature intersection goal, abstract namespace and socket pairs excluded, peer credentials as a possible JDK-specific option, Windows 10 / Server 2019 statement.
153. `System.IO.Pipes Namespace` — https://learn.microsoft.com/en-us/dotnet/api/system.io.pipes — Microsoft Learn, accessed 2026-09-08; with `Pipe Operations in .NET` — https://learn.microsoft.com/en-us/dotnet/standard/io/pipe-operations.
154. `UnixDomainSocketEndPoint Class` — https://learn.microsoft.com/en-us/dotnet/api/system.net.sockets.unixdomainsocketendpoint — Microsoft Learn, updated 2023-02-02.
155. `How to: Use Named Pipes for Network Interprocess Communication` — https://learn.microsoft.com/en-us/dotnet/standard/io/how-to-use-named-pipes-for-network-interprocess-communication — Microsoft Learn, accessed 2026-09-08; documents .NET 11 behaviour. `CurrentUserOnly` creates the underlying Unix socket file as mode 0600.
156. `Inter-process communication with gRPC` — https://learn.microsoft.com/en-us/aspnet/core/grpc/interprocess — ASP.NET Core documentation, updated 2026-07-08. Unix sockets on Windows 10 / Server 2019 and later.
157. `Go 1.12 Release Notes` — https://go.dev/doc/go1.12 — Go 1.12, 2019-02 release series. AF_UNIX on compatible Windows versions.
158. `Package net` — https://pkg.go.dev/net — accessed 2026-09-08. `unix` and `unixpacket` network strings.
159. `go-winio README` — https://github.com/microsoft/go-winio/blob/main/README.md — Microsoft/go-winio `main`, accessed 2026-09-08. Named pipes as a Go `net` transport; IOCP; Vista+.
160. `Net | Node.js v26.8.1 Documentation` — https://nodejs.org/api/net.html — Node.js v26.8.1, accessed 2026-09-08. IPC = named pipes on Windows, UDS elsewhere; path limits 107/103; unlink at `server.close`; abstract support in v20.8.0; `\\?\pipe\` form.
161. `uv_pipe_t — Pipe handle` — https://docs.libuv.org/en/v1.x/pipe.html — libuv v1.x, notes versions 1.46.0, 1.16.0, 1.3.0, 1.2.1. Unix/Windows mapping, `bind2`/`connect2` abstract support, truncation and `UV_PIPE_NO_TRUNCATE`.
162. `Sandbox Permissions` — https://docs.flatpak.org/en/latest/sandbox-permissions.html — Flatpak documentation, accessed 2026-09-08. Default filesystem view, `$XDG_RUNTIME_DIR/app/$FLATPAK_ID`, `--filesystem=`, default D-Bus filtering, full-bus warning.
163. `Flatpak Command Reference` — https://docs.flatpak.org/en/latest/flatpak-command-reference.html — accessed 2026-09-08. `--socket=` values, `--share=network`.
164. `Basic concepts` — https://docs.flatpak.org/en/latest/basic-concepts.html — accessed 2026-09-08. Portals as the sanctioned route.
165. `Snap confinement` — https://snapcraft.io/docs/explanation/security/snap-confinement/ — Snap documentation, 2026-04-02. Strict confinement via AppArmor, seccomp, namespaces.
166. `Data locations` — https://snapcraft.io/docs/reference/administration/data-locations/ — Snap documentation, 2026-04-02. `SNAP_COMMON`, `SNAP_USER_COMMON`.
167. `content interface` — https://snapcraft.io/docs/reference/interfaces/content-interface/ — Snap documentation, accessed 2026-09-08. Filesystem-level sharing including sockets.
168. `Named sockets naming` — https://forum.snapcraft.io/t/named-sockets-naming/46742 — Snapcraft Forum, 2025-04-24. Community report of `$SNAP_DATA`/`$SNAP_COMMON` and `@snap.<snap-name>.<suffix>` forms.
169. `bubblewrap README` — https://github.com/containers/bubblewrap/blob/main/README.md — `main`, accessed 2026-09-08; with the `--unshare-net` example discussion at https://github.com/python-wheel-build/fromager/issues/472 — 2024-10-10.
170. `Docker Engine security` — https://docs.docker.com/engine/security/ — Docker Docs, accessed 2026-09-08. Per-container network stack; no privileged access to another container's sockets.
171. `Host network driver` — https://docs.docker.com/engine/network/drivers/host/ — Docker Docs, accessed 2026-09-08.
172. `Bind mounts` — https://docs.docker.com/engine/storage/bind-mounts/ — Docker Docs, accessed 2026-09-08; with `tmpfs mounts` — https://docs.docker.com/engine/storage/tmpfs/.
173. `Running containers` — https://docs.docker.com/engine/containers/run/ — Docker Docs, 2026-05-13. `--read-only`.
174. `docker container run` — https://docs.docker.com/reference/cli/docker/container/run/ — Docker Docs, accessed 2026-09-08. IPC namespace modes, `shareable`, `--ipc=`.
175. `profiles/seccomp/default.json` — https://github.com/moby/moby/blob/master/profiles/seccomp/default.json — Moby upstream default seccomp profile, accessed 2026-09-08. `io_uring_*` absent from the allow list; with `Seccomp security profiles for Docker` — https://docs.docker.com/engine/security/seccomp/.
176. `VAN: Incompatible Software – VALORANT Support` — https://support-valorant.riotgames.com/hc/en-us/articles/48441713812755-VAN-Incompatible-Software — Riot Games Support, accessed 2026-09-08.
177. `Uninstalling and Disabling Riot Vanguard` — https://support-valorant.riotgames.com/hc/en-us/articles/360044648213-Uninstalling-Riot-Vanguard — Riot Games Support, accessed 2026-09-08.
178. `Steam Overlay` — https://partner.steamgames.com/doc/features/overlay — Steamworks documentation, accessed 2026-09-08; with `ISteamUtils Interface` — https://partner.steamgames.com/doc/api/ISteamUtils.
179. `discord-rpc hard-mode documentation` — https://github.com/discord/discord-rpc/blob/master/documentation/hard-mode.md — repository archived 2025-10-01; accessed 2026-09-08. `discord-ipc-0` through `discord-ipc-9`; with the community path rendering at https://github.com/hitomi-team/discord-ipc-bridge/blob/master/README.md.
180. `A quick glance at macOS' sandbox-exec` — https://jmmv.dev/2019/11/macos-sandbox-exec.html — Julio Merino, 2019-11; with `Sandboxing a third-party macOS app to restrict writing to one folder` — https://7402.org/blog/2020/macos-sandboxing-of-folder.html — 2020-12-17. Public reverse-engineering of the profile language, not an Apple contract.
181. `Unix domain sockets in Go` — https://eli.thegreenplace.net/2019/unix-domain-sockets-in-go/ — Eli Bendersky, 2019-02-12. 128-byte ping-pong latency and large-transfer throughput; hardware unspecified.
182. `IPC-Bench` — https://github.com/goldsborough/ipc-bench — results identify Intel Core i5-4590S 3.00 GHz / Ubuntu 20.04.1 LTS; snapshot accessed 2026-09-08. Per-mechanism ping-pong rates and the project's own caveats.
183. `Linux IPC Shootout: Shared Memory vs Unix Domain Sockets` — https://victoranderssen.com/blog/linux-ipc-benchmark/ — published 2026-05-01, modified 2026-08-09. Pinned-core RTT and throughput for socketpair stream/datagram versus an mmap SPSC ring; CPU and kernel unspecified.
184. `steven-joruk/macos-ipc-benchmarks` — https://github.com/steven-joruk/macos-ipc-benchmarks — results specify macOS Monterey 12.2 on a 2017 MacBook Pro, 2.9 GHz Intel i7; snapshot accessed 2026-09-08. UDS versus XPC round trips.
185. Piotr Bania, `Dynamic Data Flow Analysis via Virtual Code Integration (aka The SpiderPig case)` — https://arxiv.org/abs/0906.0724 — 2009-06-03. Named-pipe versus shared-memory-section response times under application instrumentation; no TCP figure.
186. `std::os::unix::net — Rust` — https://doc.rust-lang.org/stable/std/os/unix/net/index.html — stable docs accessed 2026-09-08; API since Rust 1.10.0. Unix-only; `SocketAncillary`/`ScmRights` experimental.
187. `Tracking Issue for feature(unix_socket_ancillary_data), rust-lang/rust#76915` — https://github.com/rust-lang/rust/issues/76915 — opened 2020-09-19; open as of 2026-09-08.
188. `Unix domain sockets on Windows, rust-lang/rust#56533` — https://github.com/rust-lang/rust/issues/56533 — opened 2018-12-05; open as of 2026-09-08.
189. `tokio::net::UnixStream — docs.rs` — https://docs.rs/tokio/latest/tokio/net/struct.UnixStream.html — Tokio 1.53.1 docs snapshot, accessed 2026-09-08. `connect`, `pair`, `peer_cred`, Unix-only, no seqpacket, no built-in SCM_RIGHTS.
190. `tokio::net::windows::named_pipe — docs.rs` — https://docs.rs/tokio/latest/tokio/net/windows/named_pipe/index.html — Tokio 1.53.1 docs snapshot, accessed 2026-09-08.
191. `tokio::sync::mpsc — docs.rs` — https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html — Tokio 1.53.1 docs snapshot, accessed 2026-09-08. Bounded backpressure, disconnection, clean shutdown, block allocation behaviour.
192. `std::sync::mpsc — Rust` — https://doc.rust-lang.org/std/sync/mpsc/index.html — stable docs, API since Rust 1.0.0; accessed 2026-09-08. `channel` versus `sync_channel`, rendezvous at bound 0, disconnection semantics.
193. `interprocess — docs.rs` — https://docs.rs/interprocess/latest/interprocess/ — interprocess 2.4.3; crates.io result dated 2026-07-31; docs snapshot accessed 2026-09-08. `local_socket`, platform tiers, Tokio-only async, 2.0 migration, self-declared passive maintenance.
194. `uds_windows — docs.rs` — https://docs.rs/uds_windows/latest/uds_windows/ — uds_windows 1.2.1; docs snapshot accessed 2026-09-08.
195. `ipc_channel — docs.rs` — https://docs.rs/ipc-channel/latest/ipc_channel/ — ipc-channel 0.23.0; docs snapshot accessed 2026-09-08. Backend matrix, serde serialisation, unbounded channels, one-shot server limitation.
196. `iceoryx2 — docs.rs` — https://docs.rs/iceoryx2/latest/iceoryx2/ — docs snapshot accessed 2026-09-08; crates.io result dated 2026-07-08. Daemon-less design, `Node`/`service_builder`, loan-write-send samples.
197. `nix::sys::socket::ControlMessage — docs.rs` — https://docs.rs/nix/latest/nix/sys/socket/enum.ControlMessage.html — accessed 2026-09-08. `ScmRights`; warning about multiple control messages.
198. `rustix — docs.rs` — https://docs.rs/rustix/latest/rustix/ — accessed 2026-09-08.
199. `passfd — lib.rs` — https://lib.rs/crates/passfd — page dated 2023-02-24; accessed 2026-09-08.
200. `shared_memory — lib.rs` — https://lib.rs/crates/shared_memory — page dated 2022-03-01; accessed 2026-09-08. `raw_sync` companion.
201. `memfd 0.3.0 — docs.rs` — https://docs.rs/crate/memfd/0.3.0 — release 2020-01-11.
202. `hyperlocal::UnixConnector — docs.rs` — https://docs.rs/hyperlocal/latest/hyperlocal/struct.UnixConnector.html — accessed 2026-09-08.
203. `feat(transport): add support for uds, unix domain socket` — https://github.com/hyperium/tonic/pull/2218 — hyperium/tonic PR #2218, opened 2025-03-12, closed 2025-03-26. tonic `unix:relative_path` and `unix:///absolute_path` support; previously UDS required a custom connector.
204. `AppArmor versions 2.9` — https://gitlab.com/apparmor/apparmor/-/wikis/AppArmor_versions_2.9 — AppArmor project wiki, accessed 2026-09-08. Abstract-socket support listed as a development target.
