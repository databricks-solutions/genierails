from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))
from auth_configured import auth_configured  # noqa: E402


def test_auth_configured_requires_non_placeholder_client_and_host(tmp_path):
    auth = tmp_path / "auth.auto.tfvars"
    auth.write_text('databricks_client_id = ""\ndatabricks_workspace_host = "<host>"\n')
    assert not auth_configured(auth)
    auth.write_text('databricks_client_id = "client"\n'
                    'databricks_workspace_host = "https://dbc.example.com"\n')
    assert auth_configured(auth)
