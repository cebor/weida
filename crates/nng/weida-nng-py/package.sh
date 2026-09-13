#!/bin/sh
# Builds the release wheel and proves it works with no Rust toolchain present.
#
# B-190. The four other bindings had this and this one did not, which made it
# the last wheel in the workspace that had never been built, installed
# anywhere clean, or run. Three things, in one command the loop can run:
#
#   1. `maturin build --release` produces an `abi3` wheel - one file for every
#      CPython from 3.9 on - carrying the metadata `pyproject.toml` declares
#      and the version `Cargo.toml` holds, which is the workspace version and
#      is not typed a second time anywhere.
#   2. A **fresh** virtualenv, in a temporary directory, installs that wheel
#      and nothing else. No `pynng`, no NNG C library: an SP implementation
#      that needs neither at install time is the claim being checked.
#   3. That virtualenv runs one REQ/REP round trip on each surface, asyncio
#      and synchronous, with `PATH` scrubbed of `cargo`, `rustc` and
#      `maturin`, so "the wheel needs no Rust toolchain" is a fact this script
#      establishes rather than an expectation. Both ends are this module, so
#      nothing but the wheel is under test.
#
# The temporary virtualenv is removed on the way out; the wheel is left in
# `target/wheels/` for a human to look at.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../../.." && pwd)
venv="$root/.venv"

CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$root/target}"
export CARGO_TARGET_DIR

uv venv --allow-existing "$venv"
uv pip install --python "$venv" maturin

cd "$here"
VIRTUAL_ENV="$venv" "$venv/bin/maturin" build --release --out "$CARGO_TARGET_DIR/wheels"

wheel=$(ls -1t "$CARGO_TARGET_DIR"/wheels/weida_nng-*.whl | head -n 1)
echo "built $wheel"
case "$wheel" in
    *-abi3-*) : ;;
    *) echo "the wheel is not abi3: $wheel" >&2; exit 1 ;;
esac

fresh=$(mktemp -d)
trap 'rm -rf "$fresh"' EXIT
uv venv "$fresh/venv"
uv pip install --python "$fresh/venv" "$wheel"

# No toolchain: the wheel is a compiled extension and a Python program that
# installs it must not need a compiler to run it.
PATH=/usr/bin:/bin "$fresh/venv/bin/python" "$here/smoke.py"
echo "the wheel runs REQ/REP on both surfaces with no Rust toolchain on PATH"
