#!/usr/bin/env python3
"""Convenience entry point for setup_sample_env.py --teardown."""

import sys

from setup_sample_env import main

if __name__ == "__main__":
    raise SystemExit(main([*sys.argv[1:], "--teardown"]))
