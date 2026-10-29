"""Run a command on a pseudo-terminal and answer its y/N prompt, as a person at a terminal would.

Shared by the tests of every y/N prompt (scripts/approval.py, and the account
and release flows built on it). stdin, stdout and stderr are all the pty's
slave, so the command sees a real terminal; the test is the person typing.
"""

from __future__ import annotations

import os
import pty
import select
import signal
import subprocess
import time

PROMPT = "[y/N] "
CTRL_C = "\x03"  # what the terminal does for it: SIGINT to the foreground process group
CTRL_D = "\x04"  # at the start of a line: EOF


def pty_run(cmd, answer, env, cwd=None, timeout=300, prompt=PROMPT, before_answer=None):
    """Run ``cmd`` on a pty; type ``answer`` at the first ``prompt`` (None: never answer).

    ``before_answer``, if given, is called once the prompt is shown and before
    the answer is typed (something else happening while the person reads it).

    ``answer`` is followed by Enter, except CTRL_D, which is typed alone, and
    CTRL_C, which sends SIGINT to the command's process group (its own
    session, so nothing else is hit), as the terminal's Ctrl-C does.
    Returns (exit code, everything the terminal showed, CRLF as LF).
    """
    master, slave = pty.openpty()
    proc = subprocess.Popen(cmd, stdin=slave, stdout=slave, stderr=slave, env=env, cwd=cwd, close_fds=True,
                            start_new_session=True, preexec_fn=_default_sigint)
    os.close(slave)
    out, answered, deadline = b"", False, time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            ready, _, _ = select.select([master], [], [], 0.2)
            if ready:
                try:
                    chunk = os.read(master, 65536)
                except OSError:
                    break
                if not chunk:
                    break
                out += chunk
            if answer is not None and not answered and prompt.encode() in out:
                if before_answer is not None:
                    before_answer()
                if answer == CTRL_C:
                    os.killpg(proc.pid, signal.SIGINT)
                else:
                    os.write(master, answer.encode() + (b"" if answer == CTRL_D else b"\n"))
                answered = True
            if proc.poll() is not None and not ready:
                break
        else:
            proc.kill()
            raise AssertionError(f"timed out:\n{out.decode(errors='replace')}")
    finally:
        os.close(master)
    proc.wait(timeout=30)
    return proc.returncode, out.decode(errors="replace").replace("\r\n", "\n")


def _default_sigint():
    # A person's terminal delivers Ctrl-C; a test runner started in the background (bash `&`) has SIGINT
    # ignored, and an ignored signal is inherited, so put it back as a terminal session would have it.
    signal.signal(signal.SIGINT, signal.SIG_DFL)
