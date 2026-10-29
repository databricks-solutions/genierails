"""REFERENCE.md lists every make target and every root setting, and only real ones.

The root REFERENCE.md is the one "every command and setting" page (design
18.5). It drifts silently when a target is added or renamed, so this checks it
against shared/Makefile.shared and the Terraform roots: every public target
(not test-*, not _-prefixed) and every ##-documented target has a command row,
every `make <target>` the page names exists, and every root variable is named.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
SHARED = ROOT / "shared"
REFERENCE = ROOT / "REFERENCE.md"
STAGES = ("setup/local", "PR check", "dev rehearse", "promotion PR",
          "prod release-plan/release", "maintain", "operations", "none")

_CODE = re.compile(r"`([^`\n]+)`")
_LINK = re.compile(r"\]\(([^)\s]+)\)")

# Make directives that never define a target, and the ones that read files.
_DIRECTIVES = {"ifeq", "ifneq", "ifdef", "ifndef", "else", "endif", "export",
               "unexport", "override", "undefine", "vpath", "private"}
_INCLUDES = {"include": True, "-include": False, "sinclude": False}
# A variable assignment: NAME =, :=, ::=, :::=, ?=, += or != (after any override/export).
_ASSIGNMENT = re.compile(r"^(?:(?:override|export|private)\s+)*[^\s:#=]+\s*(?:::?=|:::=|[?+!]?=)")
# A rule: targets, then ':' or '::' (not part of an assignment), then the rest.
_RULE = re.compile(r"^([^:#=]+?)\s*(::?)(?!=)(.*)$")
_VAR_REF = re.compile(r"\$[({]([A-Za-z0-9_]+)[)}]")


class MakefileParseError(Exception):
    pass


def parse_makefile(path, variables=None, _seen=None):
    """Explicit targets of a makefile and its includes: name -> has a ## description.

    Handles multi-target and :: rules and follows include directives
    ($(VAR) expanded from variables, or a failure). Skips recipes, define
    blocks, conditionals, assignments, target-specific variables, pattern
    rules and special targets (.PHONY, .SUFFIXES, ...). A target name make
    computes ($(...)) fails loudly, since it can't be checked.
    """
    path = Path(path)
    variables = dict(variables or {})
    seen = _seen if _seen is not None else set()
    if path.resolve() in seen:
        return {}
    seen.add(path.resolve())
    rules = {}
    in_define = False
    for number, line in enumerate(path.read_text().splitlines(), 1):
        where = f"{path}:{number}"
        if line.startswith("\t"):
            continue  # a recipe line
        stripped = line.strip()
        words = stripped.split()
        if not words or stripped.startswith("#"):
            continue
        if in_define:
            in_define = words[0] != "endef"
            continue
        if words[0] == "define" or words[:2] in (["override", "define"], ["export", "define"]):
            in_define = True
            continue
        if words[0] in _INCLUDES:
            for name in words[1:]:
                if name.startswith("#"):
                    break
                rules.update(_include(name, path, variables, seen, _INCLUDES[words[0]], where))
            continue
        if _ASSIGNMENT.match(stripped):
            continue
        if words[0] in _DIRECTIVES:
            continue
        if stripped.startswith(("$(error ", "$(warning ", "$(info ")):
            continue  # a parse-time message (e.g. the -i/-k refusal), even one with a colon in it
        match = _RULE.match(stripped)
        if not match:
            continue
        rest = match.group(3)
        if _ASSIGNMENT.match(rest.strip()):
            continue  # a target-specific variable, not a new rule
        for target in match.group(1).split():
            if "$" in target:
                raise MakefileParseError(f"{where}: can't check computed target {target!r}")
            if target.startswith(".") or "%" in target:
                continue  # special target or pattern rule
            rules[target] = rules.get(target, False) or "## " in rest
    return rules


def _include(name, including, variables, seen, required, where):
    def expand(ref):
        if ref.group(1) not in variables:
            raise MakefileParseError(f"{where}: can't resolve include {name!r}: ${ref.group(1)} is unknown")
        return str(variables[ref.group(1)])
    resolved = _VAR_REF.sub(expand, name)
    if "$" in resolved:
        raise MakefileParseError(f"{where}: can't resolve include {name!r}")
    target = Path(resolved)
    if not target.is_absolute():
        target = including.parent / target
    if not target.is_file():
        if required:
            raise MakefileParseError(f"{where}: included makefile {resolved!r} does not exist")
        return {}
    return parse_makefile(target, variables, seen)


def _rules():
    return parse_makefile(SHARED / "Makefile.shared", {"SHARED_ROOT": SHARED, "CLOUD_ROOT": ROOT / "aws"})


def _cells(line):
    return [cell.strip() for cell in re.split(r"(?<!\\)\|", line.strip())[1:-1]]


def _command_rows():
    rows = {}
    for line in REFERENCE.read_text().splitlines():
        if not line.startswith("| `make "):
            continue
        cells = _cells(line)
        target = cells[0].strip("`").split()[1]
        assert target not in rows, f"REFERENCE.md has two rows for make {target}"
        rows[target] = cells
    return rows


def _named_targets(text):
    named = set()
    for span in _CODE.findall(text):
        words = span.split()
        if words[0] == "make" and len(words) > 1 and re.fullmatch(r"[a-z_][a-z0-9_-]*", words[1]):
            named.add(words[1])
    return named


def _missing(required, rows):
    return sorted(set(required) - set(rows))


def test_every_public_and_documented_target_has_a_row():
    rules = _rules()
    public = {name for name in rules if not name.startswith(("_", "test-"))}
    documented = {name for name, has_doc in rules.items() if has_doc}
    # The parse itself: these are known to exist and must be found.
    assert {"release", "release-plan", "certify", "clean-all", "test-custom-mask"} <= public | documented
    missing = _missing(public | documented, _command_rows())
    assert not missing, "REFERENCE.md has no command row for: " + ", ".join(missing)


def test_reference_names_no_target_that_does_not_exist():
    rules = _rules()
    unknown = sorted(_named_targets(REFERENCE.read_text()) - set(rules))
    assert not unknown, "REFERENCE.md names make targets that don't exist: " + ", ".join(unknown)
    private = sorted(name for name in _command_rows() if name.startswith("_"))
    assert not private, "REFERENCE.md documents internal targets: " + ", ".join(private)


def test_every_command_row_has_parameters_purpose_and_a_section_17_stage():
    for target, cells in _command_rows().items():
        assert len(cells) == 4, f"make {target}: expected Command | Parameters | Purpose | Stage"
        assert cells[1] and cells[2], f"make {target}: empty parameters or purpose"
        assert cells[3].startswith(STAGES), f"make {target}: unknown stage {cells[3]!r}"


def test_every_root_variable_is_named():
    declared = set()
    for tf in (SHARED / "roots").glob("*/*.tf"):
        declared |= set(re.findall(r'^variable "([a-z0-9_]+)"', tf.read_text(), re.M))
    assert {"governance_mode", "databricks_client_secret", "table_readers"} <= declared
    spans = set(_CODE.findall(REFERENCE.read_text()))
    missing = sorted(declared - spans)
    assert not missing, "REFERENCE.md doesn't name these root variables: " + ", ".join(missing)


def test_every_setting_row_is_a_root_variable_or_read_by_make():
    declared = set()
    for tf in (SHARED / "roots").glob("*/*.tf"):
        declared |= set(re.findall(r'^variable "([a-z0-9_]+)"', tf.read_text(), re.M))
    sources = "\n".join(path.read_text() for path in SHARED.rglob("*.py") if "tests" not in path.parts)
    text = REFERENCE.read_text()
    settings = text[text.index("\n## Settings"):]
    for line in settings.splitlines():
        if not line.startswith("| `"):
            continue
        name = _cells(line)[0].strip("`")
        assert name in declared or f'"{name}"' in sources, f"setting {name} is neither a root variable nor read by make"


def test_reference_links_resolve_and_readme_links_to_it():
    for link in _LINK.findall(REFERENCE.read_text()):
        if re.match(r"[a-z]+:", link) or link.startswith("#"):
            continue
        target = (ROOT / link.split("#")[0]).resolve()
        assert target.exists(), f"REFERENCE.md links to a missing path: {link}"
    assert "](REFERENCE.md)" in (ROOT / "README.md").read_text()


@pytest.mark.parametrize("text, named", [
    ("run `make release ENV=prod MANIFEST=<m>`", {"release"}),
    ("Run `make` from `aws/`; `make ENV=prod` is not a target", set()),
    ("`make test-unit` and `make promote-to ENV=prod CHECK=1`", {"test-unit", "promote-to"}),
])
def test_named_targets_reads_make_code_spans(text, named):
    assert _named_targets(text) == named


def _parse(tmp_path, text, name="Makefile", **variables):
    path = tmp_path / name
    path.write_text(text)
    return parse_makefile(path, variables)


def test_parser_reads_single_multi_target_and_double_colon_rules(tmp_path):
    rules = _parse(tmp_path, (
        "plain: dep ## Documented\n"
        "\t@echo plain\n"
        "one two three: ## Shared description\n"
        "four  five :\n"
        "again:: ## First double-colon rule\n"
        "again::\n"
        "indented-rule: x\n"
    ).replace("indented-rule", "  indented-rule"))
    assert rules == {"plain": True, "one": True, "two": True, "three": True,
                     "four": False, "five": False, "again": True, "indented-rule": False}


def test_parser_ignores_assignments_patterns_special_targets_and_recipes(tmp_path):
    rules = _parse(tmp_path, (
        "A := 1\nB ::= 2\nC ?= 3\nD += 4\nE != echo 5\nF = a:b\n"
        "override G := 6\nexport H := 7\nexport I\n"
        ".PHONY: real\n.DEFAULT_GOAL := real\n.SUFFIXES:\n"
        "%.o: %.c\nbuild/%: src/%\n"
        "real: ## Real\n"
        "real: VERBOSE = 1\n"
        "\tnot-a-rule: inside a recipe\n"
        "# commented: rule\n"
        "define BLOCK\nfake: rule\nendef\n"
        "ifneq ($(X),y)\nelse\nendif\n"
    ))
    assert rules == {"real": True}


def test_parser_follows_includes_and_expands_known_variables(tmp_path):
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "extra.mk").write_text("from-include: ## Included\n")
    (tmp_path / "other.mk").write_text("relative: \n")
    rules = _parse(tmp_path, (
        "include $(SUB)/extra.mk other.mk\n"
        "-include missing-optional.mk\n"
        "sinclude also-missing.mk\n"
        "top:\n"
    ), SUB=tmp_path / "sub")
    assert rules == {"from-include": True, "relative": False, "top": False}


@pytest.mark.parametrize("text, message", [
    ("include missing.mk\n", "does not exist"),
    ("include $(NOPE)/x.mk\n", "$NOPE is unknown"),
    ("$(NAME)-target:\n", "computed target"),
])
def test_parser_fails_loudly_on_what_it_cannot_check(tmp_path, text, message):
    with pytest.raises(MakefileParseError, match=re.escape(message)):
        _parse(tmp_path, text)


def test_parser_skips_parse_time_messages_with_a_colon(tmp_path):
    text = "ifneq ($(X),)\n$(error make $(GOALS) refused: -i/-k are not supported)\nendif\n$(info a: b)\ntop:\n"
    assert _parse(tmp_path, text) == {"top": False}


def test_parser_survives_an_include_cycle(tmp_path):
    (tmp_path / "b.mk").write_text("include Makefile\nb-target:\n")
    assert _parse(tmp_path, "include b.mk\na-target:\n") == {"a-target": False, "b-target": False}
