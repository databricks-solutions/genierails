"""Shared ANSI colour helpers for CLI output."""
from __future__ import annotations

import sys


def _green(s: object) -> str: return f"\033[32m{s}\033[0m"
def _red(s: object) -> str:   return f"\033[31m{s}\033[0m"
def _cyan(s: object) -> str:  return f"\033[36m{s}\033[0m"
def _yellow(s: object) -> str: return f"\033[33m{s}\033[0m"
def _step(msg: str) -> None: print(f"\n{_cyan('──')} {msg}")
def _ok(msg: str) -> None:   print(f"  {_green('✓')}  {msg}")
def _warn(msg: str) -> None: print(f"  {_yellow('⚠')}  {msg}", file=sys.stderr)
def _err(msg: str) -> None:  print(f"  {_red('✗')}  {msg}", file=sys.stderr)
