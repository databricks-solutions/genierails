"""Dependency-free never-raw treatment facts."""

NEVER_RAW_TREATMENTS = frozenset({
    "card_security_code", "card_pin", "card_track_data", "secret",
})
NEVER_RAW_OVERRIDE_UNAVAILABLE = "is never shown raw to anyone and no override exists"


def raw_override_warning(column: str) -> str:
    return (
        f'column_overrides["{column}"].partial = "raw" has no effect: column {column} '
        f"{NEVER_RAW_OVERRIDE_UNAVAILABLE}; remove that column_overrides entry from the source "
        "env.auto.tfvars, run make generate there, then make promote-to"
    )


def raw_override_error(column: str) -> str:
    return f'column_overrides["{column}"].partial may not be raw; column {column} {NEVER_RAW_OVERRIDE_UNAVAILABLE}'
