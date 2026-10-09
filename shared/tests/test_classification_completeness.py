import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import classification_completeness as cc


def test_manifest_write_remaps_catalog_and_keeps_class_tags(tmp_path):
    source = tmp_path / "abac.auto.tfvars"
    source.write_text('''tag_assignments = [
      { entity_type = "columns", entity_name = "dev.s.t.email", tag_key = "class.email_address", tag_value = "true" },
      { entity_type = "columns", entity_name = "dev.s.t.email", tag_key = "gr_treatment", tag_value = "email" },
    ]\n''')
    destination = tmp_path / "generated/expected_classification.json"
    cc.write_manifest(source, destination, "dev=prod")
    assert json.loads(destination.read_text()) == {"prod.s.t.email": ["class.email_address"]}


def test_compare_and_ack_parsing_are_case_insensitive_and_trimmed():
    expected = {"prod.s.t.email": ["class.email_address"], "prod.s.t.ssn": ["class.us_ssn"]}
    assert cc.missing_classification(expected, ["PROD.s.t.email"]) == ["prod.s.t.ssn"]
    ack = cc.parse_ack(" prod.s.t.ssn, ,prod.s.t.phone ")
    assert cc.missing_classification(expected, ["prod.s.t.email"], ack) == []


def test_legacy_warns_but_deterministic_blocks(tmp_path, monkeypatch, capsys):
    env = tmp_path / "prod"; (env / "generated").mkdir(parents=True)
    (env / "generated/expected_classification.json").write_text('{"prod.s.t.email": ["class.email_address"]}\n')
    monkeypatch.setattr(cc, "live_classified_columns", lambda _env: set())
    assert cc.main(["check", "--env-dir", str(env), "--mode", "legacy"]) == 0
    assert "WARNING" in capsys.readouterr().err
    assert cc.main(["check", "--env-dir", str(env), "--mode", "deterministic"]) == 1
    assert "ERROR" in capsys.readouterr().err
