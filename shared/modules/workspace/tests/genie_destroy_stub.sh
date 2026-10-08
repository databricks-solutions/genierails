#!/usr/bin/env bash
set -eu

case "${1:-}" in
  create)
    mkdir -p "$(dirname "$GENIE_ID_FILE")"
    printf 'test-space\n' > "$GENIE_ID_FILE"
    adopted_marker="$(dirname "$GENIE_ID_FILE")/.genie_adopted_$(basename "$GENIE_ID_FILE" | sed 's/^\.genie_space_id_//')"
    : > "$adopted_marker"
    : > handoff-revoke.log
    ;;
  set-acls)
    : > handoff-revoke.log
    ;;
  revoke-acls)
    printf 'revoke-acls space=%s id=%s groups=%s\n' \
      "${GENIE_SPACE_OBJECT_ID:-}" "${GENIE_ID_BASENAME:-}" "${GENIE_REVOKE_GROUPS_CSV:-}" >> handoff-revoke.log
    ;;
  trash)
    ;;
esac
