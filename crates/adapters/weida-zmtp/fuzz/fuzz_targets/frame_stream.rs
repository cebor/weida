//! The frame decoder driven as a stream, one frame after another.
//!
//! A ZMTP connection is "commands and messages intermixed", so the property
//! that matters is not one frame but the walk: every accepted frame advances
//! the cursor by at least two octets, so a reader can never be parked on the
//! same offset forever, and a command frame's body is fed to the command
//! decoder the way a real reader would feed it.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weida_zmtp::{Command, FrameKind, frame};

const CAP: u64 = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    let mut rest = data;
    while let Ok((header, body, used)) = frame::decode(rest, CAP) {
        assert!(used >= 2, "a frame always consumes its header");
        assert!(used <= rest.len());
        if header.kind == FrameKind::Command {
            // A command body is only ever as long as the frame allowed, so
            // whatever the decoder does with it is bounded by the cap above.
            if let Ok(command) = Command::decode(body) {
                let re = command.encode().expect("an accepted command re-encodes");
                let (_, body2, _) = frame::decode(&re, CAP).expect("its frame decodes");
                assert_eq!(
                    Command::decode(body2).expect("and its body decodes"),
                    command
                );
            }
        }
        rest = &rest[used..];
    }
});
