import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))
from scripts import provision_test_env


class _AccountAPI:
    def __init__(self, response=None):
        self.response = response or {
            "policy": {
                "policy_id": "policy-123",
                "policy_name": "genierails-ci-run123",
            }
        }
        self.calls = []

    def do(self, method, path, **kwargs):
        self.calls.append((method, path, kwargs))
        return self.response


def _client(api):
    return SimpleNamespace(
        api_client=api,
        config=SimpleNamespace(account_id="account-123"),
    )


def test_aws_provision_creates_workspace_bound_serverless_budget_policy():
    api = _AccountAPI()
    state = {
        "cloud_provider": "aws",
        "workspace_id": 12345,
        "run_id": "run123",
    }

    provision_test_env._provision_aws_serverless_budget_policy(_client(api), state)

    assert len(api.calls) == 1
    method, path, call = api.calls[0]
    assert method == "POST"
    assert path == "/api/2.1/accounts/account-123/budget-policies"
    assert call["headers"] == {"X-Databricks-Org-Id": "12345"}
    assert call["body"] == {
        "policy": {
            "policy_name": "genierails-ci-run123",
            "binding_workspace_ids": [12345],
            "custom_tags": [{"key": "genierails_ci", "value": "run123"}],
        },
        "request_id": "genierails-ci-run123",
    }
    assert state["serverless_budget_policy_id"] == "policy-123"
    assert state["serverless_budget_policy_name"] == "genierails-ci-run123"


def test_azure_provision_does_not_touch_serverless_budget_policies():
    api = _AccountAPI()
    state = {
        "cloud_provider": "azure",
        "workspace_id": 12345,
        "run_id": "run123",
    }

    provision_test_env._provision_aws_serverless_budget_policy(_client(api), state)

    assert api.calls == []
    assert "serverless_budget_policy_id" not in state


def test_aws_provision_fails_if_policy_creation_returns_no_id():
    api = _AccountAPI(response={"policy": {"policy_name": "genierails-ci-run123"}})
    state = {
        "cloud_provider": "aws",
        "workspace_id": 12345,
        "run_id": "run123",
    }

    with pytest.raises(RuntimeError, match="no serverless usage policy ID"):
        provision_test_env._provision_aws_serverless_budget_policy(_client(api), state)


def test_aws_teardown_deletes_only_policy_recorded_in_state():
    api = _AccountAPI()
    state = {
        "cloud_provider": "aws",
        "workspace_id": 12345,
        "serverless_budget_policy_id": "policy-123",
    }

    provision_test_env._teardown_aws_serverless_budget_policy(_client(api), state)

    assert api.calls == [
        (
            "DELETE",
            "/api/2.1/accounts/account-123/budget-policies/policy-123",
            {"headers": {"X-Databricks-Org-Id": "12345"}},
        )
    ]


def test_azure_teardown_does_not_touch_serverless_budget_policies():
    api = _AccountAPI()

    provision_test_env._teardown_aws_serverless_budget_policy(
        _client(api),
        {"cloud_provider": "azure", "serverless_budget_policy_id": "policy-123"},
    )

    assert api.calls == []


def test_policy_tfvar_is_written_only_for_aws_test_environments():
    assert provision_test_env._serverless_usage_policy_tfvar(
        {
            "cloud_provider": "aws",
            "serverless_budget_policy_id": "policy-123",
        }
    ) == 'serverless_usage_policy_id = "policy-123"\n'
    assert provision_test_env._serverless_usage_policy_tfvar(
        {
            "cloud_provider": "azure",
            "serverless_budget_policy_id": "policy-123",
        }
    ) == ""
