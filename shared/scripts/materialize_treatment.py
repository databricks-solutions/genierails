#!/usr/bin/env python3
"""Add one treatment's mask policy and UDF to an env's generated rules.

Generation only emits masks for treatments some column there carries, so a
treatment needed only in prod (a class the dev classifier never saw) has no
dev rule to promote. This writes, for every governed catalog of the env, the
treatment's column-mask policy (the same shape derivation gives a fallback
mask) and its configured UDF, without any tag_assignments. Policies and
functions already in the rules are kept exactly as reviewed, so re-running
changes nothing.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import hcl2

SHARED_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SHARED_ROOT))

from generate_abac import (  # noqa: E402
    _find_bracket_section,
    _render_fgac_policy_block,
    _render_tag_policy_block,
    _replace_bracket_section,
)
from treatment_derivation import (  # noqa: E402
    ACL_NEUTRAL_FALLBACK_COMMENT,
    load_treatment_config,
)
from scripts.footprint import FootprintError, resolve_footprint  # noqa: E402
from scripts.merge_space_configs import _function_blocks_by_key, _uc_identifier  # noqa: E402

COLUMN_MASK = "POLICY_TYPE_COLUMN_MASK"
POLICY_LIMIT = 100  # Unity Catalog policies per catalog (validate_abac's coverage check)
TAG_VALUE_RE = re.compile(r"hasTagValue\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)")


def governed_catalogs(cfg: dict, env_dir: Path | None) -> list[str]:
    """Catalogs the env governs: its policies, column assignments and footprint."""
    catalogs = {p.get("catalog") for p in cfg.get("fgac_policies") or [] if p.get("catalog")}
    catalogs |= {
        str(a.get("entity_name", "")).split(".", 1)[0]
        for a in cfg.get("tag_assignments") or []
        if a.get("entity_type") == "columns" and a.get("entity_name")
    }
    if env_dir is not None and (env_dir / "env.auto.tfvars").is_file():
        try:
            tables = resolve_footprint(env_dir)
        except FootprintError as exc:
            raise ValueError(str(exc)) from exc
        catalogs |= {t.split(".")[0] for t in tables if len(t.split(".")) == 3}
    return sorted(c for c in catalogs if c)


def _definition(treatment, sql_text: str) -> str | None:
    """The CREATE FUNCTION statement for the treatment's UDF, without context."""
    if treatment.udf_signature and treatment.udf_body:
        return (
            f"CREATE OR REPLACE FUNCTION {treatment.udf_signature}\n"
            f"RETURN {treatment.udf_body};"
        )
    for (_catalog, _schema, name), block in _function_blocks_by_key(sql_text).items():
        if name == treatment.masking_function.lower():
            start = re.search(r"CREATE\s", block, re.IGNORECASE)
            return block[start.start():].strip() if start else None
    return None


def materialize(
    tfvars_path: Path,
    sql_path: Path,
    treatment_value: str,
    *,
    config_path: Path | None = None,
    env_dir: Path | None = None,
) -> dict[str, list[str]]:
    """Add the treatment's masks and UDFs; return what was added and kept."""
    config = load_treatment_config(config_path) if config_path else load_treatment_config()
    treatment = next((t for t in config.treatments if t.value == treatment_value), None)
    if treatment is None:
        raise ValueError(
            f"TREATMENT={treatment_value} is not a treatment in shared/treatment_config.json. "
            f"Existing treatments: {', '.join(config.values)}. Add it first: "
            "make scaffold-treatments ENV=<the env that found the class>"
        )
    if not tfvars_path.is_file():
        raise FileNotFoundError(
            f"Generated ABAC config not found: {tfvars_path}. Run make generate first"
        )
    text = tfvars_path.read_text()
    cfg = hcl2.loads(text)
    sql_text = sql_path.read_text() if sql_path.is_file() else ""
    catalogs = governed_catalogs(cfg, env_dir)
    if not catalogs:
        raise ValueError(f"{tfvars_path} governs no catalog yet; run make generate first")

    policies = list(cfg.get("fgac_policies") or [])
    masks = [p for p in policies if p.get("policy_type") == COLUMN_MASK]
    names = {p.get("name") for p in policies}
    principals = sorted({p for m in masks for p in (m.get("to_principals") or [])}) or [
        "account users"
    ]
    functions = _function_blocks_by_key(sql_text)
    definition = _definition(treatment, sql_text)

    new_policies: list[dict] = []
    new_blocks: list[str] = []
    added_functions: list[str] = []
    kept: list[str] = []
    for catalog in catalogs:
        existing = [
            m for m in masks
            if m.get("catalog") == catalog
            and (config.tag_key, treatment.value) in TAG_VALUE_RE.findall(m.get("match_condition", "") or "")
        ]
        if existing:
            kept.extend(f"policy {m.get('name')} (catalog {catalog})" for m in existing)
            policy = existing[0]
        else:
            name = f"gr_mask_{catalog}_{treatment.value}"
            if name in names:
                raise ValueError(
                    f"A policy named {name} already exists but does not mask "
                    f"gr_treatment={treatment.value}; rename or remove it first"
                )
            template = next((m for m in masks if m.get("catalog") == catalog), None) or (
                masks[0] if masks else {}
            )
            policy = {
                "name": name,
                "policy_type": COLUMN_MASK,
                "catalog": catalog,
                "to_principals": principals,
                "comment": ACL_NEUTRAL_FALLBACK_COMMENT,
                "match_condition": f"hasTagValue('{config.tag_key}', '{treatment.value}')",
                "match_alias": f"gr_treatment_{treatment.value}",
                "function_name": treatment.masking_function,
                "function_catalog": catalog,
                "function_schema": template.get("function_schema") or "default",
            }
            new_policies.append(policy)
            names.add(name)
        fn_catalog = _uc_identifier(policy.get("function_catalog") or catalog)
        fn_schema = _uc_identifier(policy.get("function_schema"))
        fn_name = _uc_identifier(policy.get("function_name"))
        if (fn_catalog, fn_schema, fn_name) in functions:
            if not existing:
                kept.append(f"function {fn_catalog}.{fn_schema}.{fn_name}")
            continue
        if definition is None:
            raise ValueError(
                f"Treatment {treatment.value} has no udf_signature/udf_body in "
                f"shared/treatment_config.json and {sql_path.name} does not define "
                f"{treatment.masking_function}; add its definition first"
            )
        new_blocks.append(
            f"-- Treatment {treatment.value} (make materialize-treatment)\n"
            f"USE CATALOG {policy.get('function_catalog') or catalog};\n"
            f"USE SCHEMA {policy.get('function_schema')};\n{definition}"
        )
        functions[(fn_catalog, fn_schema, fn_name)] = definition
        added_functions.append(f"{fn_catalog}.{fn_schema}.{fn_name}")

    counts: dict[str, int] = {}
    for policy in policies + new_policies:
        catalog = policy.get("catalog", "") or policy.get("function_catalog", "")
        counts[catalog] = counts.get(catalog, 0) + 1
    over = [f"{c} ({n} policies)" for c, n in sorted(counts.items()) if n > POLICY_LIMIT]
    if over and new_policies:
        raise ValueError(
            f"Refusing to add {treatment.value}: Unity Catalog allows {POLICY_LIMIT} policies "
            f"per catalog and this would exceed it in {', '.join(over)}. Reuse an existing "
            "treatment instead (make scaffold-treatments TREATMENT=<name>)"
        )

    # The gr_treatment vocabulary must declare the value, as derivation does.
    tag_policies = [dict(p) for p in cfg.get("tag_policies") or []]
    vocab = next((p for p in tag_policies if p.get("key") == config.tag_key), None)
    vocab_changed = vocab is None or treatment.value not in (vocab.get("values") or [])
    if vocab is None:
        tag_policies.append({"key": config.tag_key, "description": config.description,
                             "values": config.values})
    elif vocab_changed:
        vocab["values"] = list(vocab.get("values") or []) + [treatment.value]

    updated = text
    if vocab_changed:
        rendered = [_render_tag_policy_block(p) for p in tag_policies]
        if _find_bracket_section(updated, "tag_policies") is None:
            updated = "tag_policies = [\n" + ",\n".join(rendered) + "\n]\n\n" + updated
        else:
            updated = _replace_bracket_section(updated, "tag_policies", rendered)
    if new_policies:
        blocks = [_render_fgac_policy_block(p) for p in new_policies]
        section = _find_bracket_section(updated, "fgac_policies")
        if section is None:
            updated = updated.rstrip() + "\n\nfgac_policies = [\n" + ",\n".join(blocks) + "\n]\n"
        else:
            start, end = section
            body = updated[start:end].rstrip()
            joiner = ",\n" if body.strip() and not body.endswith(",") else "\n"
            updated = updated[:start] + body + joiner + ",\n".join(blocks) + "\n" + updated[end:]
    hcl2.loads(updated)

    if updated != text:
        tfvars_path.write_text(updated)
    if new_blocks:
        sql_path.write_text(sql_text.rstrip() + "\n\n" + "\n\n".join(new_blocks) + "\n")
    return {
        "catalogs": catalogs,
        "policies": [f"{p['name']} (catalog {p['catalog']})" for p in new_policies],
        "functions": added_functions,
        "kept": kept,
        "vocabulary": [treatment.value] if vocab_changed else [],
    }


def source_env_dir(envs_dir: Path, env: str, env_dir: Path) -> Path:
    """The env to write: exactly a real envs/<env>, and never a promotion target.

    Rules reach a promoted env (prod, or anything with promote_from) only by
    promotion, so materialize writes the env they are promoted from. ENV_DIR
    can be overridden, so the check is on the resolved directory, not ENV.
    """
    from scripts.coverage_fix import promote_source
    from scripts.saved_settings import _env_name, destination_env_dir

    _env_name("ENV", env)
    path = destination_env_dir(envs_dir, env, env_dir)
    if not path.is_dir():
        raise ValueError(f"envs/{env} does not exist; run make setup ENV={env} first")
    source = promote_source(path)
    if env == "prod" or source:
        from_env = source or "dev"
        raise ValueError(
            f"ENV={env} is not allowed: {env}'s rules change only by promotion"
            + (f" (its promote_from is {source})" if source else "")
            + f". Run it in the env {env} is promoted from: make materialize-treatment "
            f"ENV={from_env} TREATMENT=<t>, then make rehearse ENV={from_env}, "
            f"make promote-to ENV={env} and make release ENV={env}"
        )
    return path


def promoted_to(env_dir: Path, env: str) -> list[str]:
    """Sibling envs whose env.auto.tfvars says promote_from = env."""
    from scripts.coverage_fix import promote_source

    return sorted(
        path.name for path in env_dir.parent.iterdir()
        if path.is_dir() and path != env_dir and promote_source(path) == env
    ) if env_dir.parent.is_dir() else []


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--tfvars", type=Path, required=True)
    parser.add_argument("--sql", type=Path, required=True)
    parser.add_argument("--treatment", required=True)
    parser.add_argument("--treatment-config", type=Path)
    parser.add_argument("--env-dir", type=Path)
    parser.add_argument("--env-name", default="")
    parser.add_argument("--envs-dir", type=Path,
                        help="the cloud's envs/; with it, --env-dir must be exactly "
                             "envs/<env-name> and not a promoted env")
    parser.add_argument("--check-env", action="store_true",
                        help="only check --env-dir against --envs-dir, then exit")
    args = parser.parse_args(argv)
    if not args.treatment.strip():
        print("ERROR: set TREATMENT=<treatment> (a value in shared/treatment_config.json)",
              file=sys.stderr)
        return 1
    try:
        if args.envs_dir is not None:
            if args.env_dir is None:
                raise ValueError("--envs-dir needs --env-dir")
            args.env_dir = source_env_dir(args.envs_dir, args.env_name, args.env_dir)
            args.tfvars = args.env_dir / "generated" / "abac.auto.tfvars"
            args.sql = args.env_dir / "generated" / "masking_functions.sql"
        if args.check_env:
            if args.envs_dir is None:
                raise ValueError("--check-env needs --envs-dir")
            return 0
        result = materialize(
            args.tfvars, args.sql, args.treatment.strip(),
            config_path=args.treatment_config, env_dir=args.env_dir,
        )
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    env = args.env_name or (args.env_dir.name if args.env_dir else "<env>")
    added = result["policies"] or result["functions"] or result["vocabulary"]
    print(f"Treatment {args.treatment.strip()} in {env} (catalogs: {', '.join(result['catalogs'])}):")
    for policy in result["policies"]:
        print(f"  added mask policy {policy}  -> generated/abac.auto.tfvars")
    for function in result["functions"]:
        print(f"  added function {function}  -> generated/masking_functions.sql")
    for value in result["vocabulary"]:
        print(f"  added gr_treatment value {value} to tag_policies")
    for item in result["kept"]:
        print(f"  kept reviewed {item}")
    if not added:
        print("  already present; nothing changed.")
    print("  No tag_assignments were added; the mask applies where a column gets this treatment.")
    targets = promoted_to(args.env_dir, env) if args.env_dir else []
    dest = targets[0] if len(targets) == 1 else "<prod env>"
    changed = []
    if result["policies"] or result["vocabulary"]:
        changed.append(f"envs/{env}/generated/abac.auto.tfvars")
    if result["functions"]:
        changed.append(f"envs/{env}/generated/masking_functions.sql")
    print(f"Next: make rehearse ENV={env}, make promote-to ENV={dest}, make release ENV={dest}."
          + (f" Commit {', '.join(changed)}." if changed else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
