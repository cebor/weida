#!/bin/sh
# Builds the Python extension from this checkout and runs its tests.
#
# One command, so that the loop (docs/LOOP.md) has one thing to run and a
# developer has one thing to remember. It:
#
#   1. creates or reuses a `uv`-managed virtualenv at the repository root,
#   2. installs `maturin` and `pytest` **into that virtualenv** — never into a
#      system interpreter, which LOOP.md 2 forbids and which would need root
#      on most machines anyway,
#   3. runs `maturin develop`, which builds this crate with
#      `pyo3/extension-module` (see pyproject.toml) and installs it into the
#      virtualenv,
#   4. runs the Python tests, passing any arguments straight to pytest.
#
# The tests need no external process at all: both halves of every exchange
# are this library, which is what makes weida's own binding the easiest of the
# six to test.
#
# `CARGO_TARGET_DIR` is honoured if it is already set, so a worktree that
# builds into its own target directory keeps doing so and two cargo
# invocations never share a build lock.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../../.." && pwd)
venv="$root/.venv"

CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$root/target}"
export CARGO_TARGET_DIR

uv venv --allow-existing "$venv"
uv pip install --python "$venv" maturin pytest

cd "$here"
VIRTUAL_ENV="$venv" "$venv/bin/maturin" develop
"$venv/bin/python" -m pytest tests "$@"
