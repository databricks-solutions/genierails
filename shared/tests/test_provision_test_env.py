import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))
from scripts import provision_test_env


class _BudgetPolicyAPI:
    def __init__(self, response=None):
        self.response = response or SimpleNamespace(
            policy_id="policy-123", policy_name="genierails-ci-run123"
        )
        self.create_calls = []
        self.delete_calls = []

    def create(self, **kwargs):
        self.create_calls.append(kwargs)
        return self.response

    def delete(self, **kwargs):
        self.delete_calls.append(kwargs)


def _client(api):
    return SimpleNamespace(budget_policy=api)


def test_aws_provision_creates_workspace_bound_serverless_budget_policy():
    api = _BudgetPolicyAPI()
    state = {
        "cloud_provider": "aws",
        "workspace_id": 12345,
        "run_id": "run123",
    }

    provision_test_env._provision_aws_serverless_budget_policy(_client(api), state)

    assert len(api.create_calls) == 1
    call = api.create_calls[0]
    assert call["request_id"] == "genierails-ci-run123"
    assert call["policy"].policy_name == "genierails-ci-run123"
    assert call["policy"].binding_workspace_ids == [12345]
    assert [(tag.key, tag.value) for tag in call["policy"].custom_tags] == [
        ("genierails_ci", "run123")
    ]
    assert state["serverless_budget_policy_id"] == "policy-123"
    assert state["serverless_budget_policy_name"] == "genierails-ci-run123"


def test_azure_provision_does_not_touch_serverless_budget_policies():
    api = _BudgetPolicyAPI()
    state = {
        "cloud_provider": "azure",
        "workspace_id": 12345,
        "run_id": "run123",
    }

    provision_test_env._provision_aws_serverless_budget_policy(_client(api), state)

    assert api.create_calls == []
    assert "serverless_budget_policy_id" not in state


def test_aws_provision_fails_if_policy_creation_returns_no_id():
    api = _BudgetPolicyAPI(
        response=SimpleNamespace(policy_id=None, policy_name="genierails-ci-run123")
    )
    state = {
        "cloud_provider": "aws",
        "workspace_id": 12345,
        "run_id": "run123",
    }

    with pytest.raises(RuntimeError, match="no serverless usage policy ID"):
        provision_test_env._provision_aws_serverless_budget_policy(_client(api), state)


def test_aws_teardown_deletes_only_policy_recorded_in_state():
    api = _BudgetPolicyAPI()
    state = {
        "cloud_provider": "aws",
        "serverless_budget_policy_id": "policy-123",
    }

    provision_test_env._teardown_aws_serverless_budget_policy(_client(api), state)

    assert api.delete_calls == [{"policy_id": "policy-123"}]


def test_azure_teardown_does_not_touch_serverless_budget_policies():
    api = _BudgetPolicyAPI()

    provision_test_env._teardown_aws_serverless_budget_policy(
        _client(api),
        {"cloud_provider": "azure", "serverless_budget_policy_id": "policy-123"},
    )

    assert api.delete_calls == []
