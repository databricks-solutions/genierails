"""Azure cloud provider for provision_test_env.py."""
from __future__ import annotations

import sys
import time
from typing import Any

from .base import CloudProvider, StorageResult, CredentialResult

# ANSI helpers
from ._ansi import _green, _red, _cyan, _yellow, _step, _ok, _warn, _err


def _ensure_azure_deps():
    """Auto-install Azure SDK packages if missing."""
    needed = {
        "azure-identity": "azure.identity",
        "azure-mgmt-storage": "azure.mgmt.storage",
        "azure-mgmt-authorization": "azure.mgmt.authorization",
        "azure-mgmt-databricks": "azure.mgmt.databricks",
    }
    missing = []
    for pip_name, import_path in needed.items():
        try:
            __import__(import_path)
        except ImportError:
            missing.append(pip_name)
    if missing:
        import subprocess
        _warn(f"Installing Azure SDKs: {', '.join(missing)}")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", *missing])


class AzureProvider(CloudProvider):
    """Azure cloud provider — uses ADLS Gen2 + Access Connector for Unity Catalog."""

    @property
    def account_host(self) -> str:
        return "https://accounts.azuredatabricks.net"

    def validate_config(self, cfg: dict[str, str]) -> None:
        required = ["AZURE_SUBSCRIPTION_ID", "AZURE_RESOURCE_GROUP", "AZURE_REGION"]
        missing = [k for k in required if not cfg.get(k)]
        if missing:
            _err(f"Missing Azure config: {', '.join(missing)}")
            sys.exit(1)

    def get_region(self, cfg: dict[str, str]) -> str:
        return cfg["AZURE_REGION"]

    def setup_storage(self, cfg: dict[str, str], run_id: str, region: str, account_id: str) -> StorageResult:
        _ensure_azure_deps()
        from azure.identity import ClientSecretCredential
        from azure.mgmt.storage import StorageManagementClient
        from azure.mgmt.storage.models import (
            StorageAccountCreateParameters, Sku, Kind,
            BlobServiceProperties, CorsRules,
        )

        subscription_id = cfg["AZURE_SUBSCRIPTION_ID"]
        resource_group = cfg["AZURE_RESOURCE_GROUP"]

        # Azure credentials: use SP credentials if available, else DefaultAzureCredential
        if cfg.get("AZURE_TENANT_ID") and cfg.get("AZURE_CLIENT_ID") and cfg.get("AZURE_CLIENT_SECRET"):
            credential = ClientSecretCredential(
                tenant_id=cfg["AZURE_TENANT_ID"],
                client_id=cfg["AZURE_CLIENT_ID"],
                client_secret=cfg["AZURE_CLIENT_SECRET"],
            )
        else:
            from azure.identity import DefaultAzureCredential
            credential = DefaultAzureCredential()

        # Storage account name: must be globally unique, 3-24 lowercase alphanumeric
        sa_name = f"genietest{run_id}"[:24].lower()
        container_name = "genie-test"

        _step(f"Creating Azure Storage Account: {sa_name}")
        storage_client = StorageManagementClient(credential, subscription_id)

        poller = storage_client.storage_accounts.begin_create(
            resource_group,
            sa_name,
            StorageAccountCreateParameters(
                sku=Sku(name="Standard_LRS"),
                kind=Kind.STORAGE_V2,
                location=region,
                is_hns_enabled=True,  # ADLS Gen2 (hierarchical namespace)
                tags={"ManagedBy": "provision_test_env", "RunId": run_id},
            ),
        )
        poller.result()
        _ok(f"Storage account created: {sa_name}")

        # Create blob container
        _step(f"Creating container: {container_name}")
        storage_client.blob_containers.create(
            resource_group, sa_name, container_name, {}
        )
        _ok(f"Container created: {container_name}")

        # Create Access Connector for Databricks
        _step("Creating Databricks Access Connector")
        from azure.mgmt.databricks import AzureDatabricksManagementClient
        from azure.mgmt.databricks.models import AccessConnector, ManagedServiceIdentity

        ac_name = f"genie-test-ac-{run_id}"
        db_client = AzureDatabricksManagementClient(credential, subscription_id)

        poller = db_client.access_connectors.begin_create_or_update(
            resource_group,
            ac_name,
            AccessConnector(
                location=region,
                identity=ManagedServiceIdentity(type="SystemAssigned"),
                tags={"ManagedBy": "provision_test_env", "RunId": run_id},
            ),
        )
        ac = poller.result()
        ac_id = ac.id
        ac_principal_id = ac.identity.principal_id
        _ok(f"Access Connector created: {ac_name} (principal: {ac_principal_id})")

        # Assign Storage Blob Data Contributor to the Access Connector's managed identity
        _step("Assigning Storage Blob Data Contributor role")
        from azure.mgmt.authorization import AuthorizationManagementClient
        from azure.mgmt.authorization.models import RoleAssignmentCreateParameters
        import uuid

        # Storage Blob Data Contributor built-in role ID
        role_def_id = f"/subscriptions/{subscription_id}/providers/Microsoft.Authorization/roleDefinitions/ba92f5b4-2d11-453d-a403-e96b0029c9fe"
        scope = f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Storage/storageAccounts/{sa_name}"
        assignment_params = RoleAssignmentCreateParameters(
            role_definition_id=role_def_id,
            principal_id=ac_principal_id,
            principal_type="ServicePrincipal",
        )
        assignment_id = str(uuid.uuid4())

        # Try with the SP credential first; if it lacks User Access Administrator,
        # fall back to DefaultAzureCredential (e.g. az CLI login) which may have
        # the permission via an inherited role.
        auth_client = AuthorizationManagementClient(credential, subscription_id)
        try:
            auth_client.role_assignments.create(scope, assignment_id, assignment_params)
        except Exception as first_err:
            if "AuthorizationFailed" in str(first_err):
                _warn("SP lacks roleAssignments/write — retrying with DefaultAzureCredential (az CLI)...")
                from azure.identity import DefaultAzureCredential
                fallback_cred = DefaultAzureCredential()
                fallback_client = AuthorizationManagementClient(fallback_cred, subscription_id)
                assignment_id = str(uuid.uuid4())
                fallback_client.role_assignments.create(scope, assignment_id, assignment_params)
            else:
                raise
        _ok("Role assignment created")

        # Wait for role assignment propagation
        _warn("Waiting 30s for Azure role assignment propagation...")
        time.sleep(30)

        storage_url = f"abfss://{container_name}@{sa_name}.dfs.core.windows.net/genie-test-{run_id}"

        return StorageResult(
            storage_url=storage_url,
            credential_name="test-ext-loc-cred",
            access_connector_id=ac_id,
            storage_account_name=sa_name,
            container_name=container_name,
            resource_group=resource_group,
        )

    def register_storage_credential(self, account_client: Any, metastore_id: str, storage_result: StorageResult) -> CredentialResult:
        from databricks.sdk.service.catalog import (
            CreateAccountsStorageCredential,
            AzureManagedIdentityRequest,
        )

        _step("Registering Azure storage credential")
        try:
            resp = account_client.storage_credentials.create(
                metastore_id=metastore_id,
                credential_info=CreateAccountsStorageCredential(
                    name=storage_result.credential_name,
                    azure_managed_identity=AzureManagedIdentityRequest(
                        access_connector_id=storage_result.access_connector_id,
                    ),
                    comment="Storage credential for test External Location — provision_test_env.py",
                ),
            )
            cred_info = getattr(resp, "credential_info", None) or resp
            _ok(f"Storage credential registered: id={cred_info.id!r}")
            return CredentialResult(credential_id=cred_info.id)
        except Exception as exc:
            _warn(f"Could not register storage credential: {exc}")
            return CredentialResult()

    def post_credential_setup(self, cfg: dict[str, str], storage_result: StorageResult, credential_result: CredentialResult, account_id: str, region: str) -> None:
        # Azure doesn't need post-registration trust policy tightening
        pass

    def teardown_storage(self, cfg: dict[str, str], state: dict) -> None:
        _ensure_azure_deps()
        from azure.identity import ClientSecretCredential, DefaultAzureCredential

        subscription_id = cfg.get("AZURE_SUBSCRIPTION_ID", state.get("azure_subscription_id", ""))
        resource_group = state.get("azure_resource_group", cfg.get("AZURE_RESOURCE_GROUP", ""))

        if cfg.get("AZURE_TENANT_ID") and cfg.get("AZURE_CLIENT_ID") and cfg.get("AZURE_CLIENT_SECRET"):
            credential = ClientSecretCredential(
                tenant_id=cfg["AZURE_TENANT_ID"],
                client_id=cfg["AZURE_CLIENT_ID"],
                client_secret=cfg["AZURE_CLIENT_SECRET"],
            )
        else:
            credential = DefaultAzureCredential()

        # Delete Access Connector
        ac_name = state.get("azure_access_connector_name")
        if ac_name and resource_group:
            _step(f"Deleting Access Connector: {ac_name}")
            try:
                from azure.mgmt.databricks import AzureDatabricksManagementClient
                db_client = AzureDatabricksManagementClient(credential, subscription_id)
                poller = db_client.access_connectors.begin_delete(resource_group, ac_name)
                poller.result()
                _ok(f"Access Connector deleted: {ac_name}")
            except Exception as exc:
                _warn(f"Could not delete Access Connector: {exc}")

        # Delete Storage Account (includes container and all data)
        sa_name = state.get("azure_storage_account_name")
        if sa_name and resource_group:
            _step(f"Deleting Storage Account: {sa_name}")
            try:
                from azure.mgmt.storage import StorageManagementClient
                storage_client = StorageManagementClient(credential, subscription_id)
                storage_client.storage_accounts.delete(resource_group, sa_name)
                _ok(f"Storage Account deleted: {sa_name}")
            except Exception as exc:
                _warn(f"Could not delete Storage Account: {exc}")

    def workspace_create_kwargs(self, region: str) -> dict:
        return {"location": region}

    # ARM preview API version that supports computeMode=Serverless
    _ARM_API_VERSION = "2025-10-01-preview"

    def _azure_credential(self, cfg: dict[str, str]):
        """Return an Azure credential from config (SP preferred, else DefaultAzureCredential)."""
        _ensure_azure_deps()
        from azure.identity import ClientSecretCredential, DefaultAzureCredential
        if cfg.get("AZURE_TENANT_ID") and cfg.get("AZURE_CLIENT_ID") and cfg.get("AZURE_CLIENT_SECRET"):
            return ClientSecretCredential(
                tenant_id=cfg["AZURE_TENANT_ID"],
                client_id=cfg["AZURE_CLIENT_ID"],
                client_secret=cfg["AZURE_CLIENT_SECRET"],
            )
        return DefaultAzureCredential()

    def _arm_token(self, cfg: dict[str, str]) -> str:
        """Get an Azure management bearer token."""
        cred = self._azure_credential(cfg)
        return cred.get_token("https://management.azure.com/.default").token

    def create_workspace(self, cfg: dict[str, str], ws_name: str, region: str, account_client: Any) -> tuple[int, str]:
        """Create a serverless Azure Databricks workspace via ARM REST API.

        Uses the preview API to set computeMode=Serverless, which skips the
        managed resource group and provisions in ~90 seconds instead of 3-5 min.
        """
        import json
        import urllib.request
        import urllib.error

        subscription_id = cfg["AZURE_SUBSCRIPTION_ID"]
        resource_group = cfg["AZURE_RESOURCE_GROUP"]
        token = self._arm_token(cfg)

        url = (
            f"https://management.azure.com/subscriptions/{subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.Databricks/workspaces/{ws_name}"
            f"?api-version={self._ARM_API_VERSION}"
        )
        body = json.dumps({
            "location": region,
            "sku": {"name": "premium"},
            "properties": {"computeMode": "Serverless"},
            "tags": {"ManagedBy": "provision_test_env"},
        }).encode()

        _step(f"Creating serverless Azure Databricks workspace: {ws_name}")
        print("  (Serverless workspaces typically provision in ~90 seconds…)")

        req = urllib.request.Request(url, data=body, method="PUT", headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })
        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace")
            raise RuntimeError(f"ARM PUT {e.code}: {detail}") from e

        # Poll until Succeeded
        deadline = time.time() + 600
        poll_interval = 15
        while time.time() < deadline:
            time.sleep(poll_interval)
            get_req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
            with urllib.request.urlopen(get_req) as resp:
                data = json.loads(resp.read())
            props = data.get("properties", {})
            state = props.get("provisioningState", "Unknown")
            elapsed = int(time.time() - (deadline - 600))
            print(f"  [{elapsed}s]  {state}")
            if state == "Succeeded":
                ws_id = int(props["workspaceId"])
                ws_url = props["workspaceUrl"]
                ws_host = f"https://{ws_url}" if not ws_url.startswith("https://") else ws_url
                _ok(f"Workspace created: id={ws_id}  host={ws_host}")
                return ws_id, ws_host
            if state in ("Failed", "Canceled"):
                raise RuntimeError(f"Workspace creation {state}: {props.get('provisioningState')}")

        raise TimeoutError("Workspace did not reach Succeeded within 10 minutes")

    def teardown_workspace(self, cfg: dict[str, str], state: dict) -> None:
        """Delete an Azure Databricks workspace via ARM REST API."""
        ws_name = state.get("workspace_name")
        if not ws_name:
            return

        import json
        import urllib.request
        import urllib.error

        subscription_id = cfg.get("AZURE_SUBSCRIPTION_ID", "")
        resource_group = state.get("azure_resource_group", cfg.get("AZURE_RESOURCE_GROUP", ""))
        token = self._arm_token(cfg)

        url = (
            f"https://management.azure.com/subscriptions/{subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.Databricks/workspaces/{ws_name}"
            f"?api-version={self._ARM_API_VERSION}"
        )

        _step(f"Deleting Azure Databricks workspace: {ws_name}")
        try:
            req = urllib.request.Request(url, method="DELETE", headers={
                "Authorization": f"Bearer {token}",
            })
            with urllib.request.urlopen(req) as resp:
                pass
            # Poll until gone (202 means async deletion)
            deadline = time.time() + 600
            while time.time() < deadline:
                time.sleep(15)
                try:
                    get_req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
                    with urllib.request.urlopen(get_req) as resp:
                        data = json.loads(resp.read())
                    state_val = data.get("properties", {}).get("provisioningState", "")
                    if state_val == "Deleting":
                        continue
                except urllib.error.HTTPError as e:
                    if e.code == 404:
                        break
                    raise
            _ok(f"Workspace deleted: {ws_name}")
        except Exception as exc:
            _warn(f"Could not delete workspace via ARM: {exc}")

    def state_extras(self, storage_result: StorageResult) -> dict:
        return {
            "azure_storage_account_name": storage_result.storage_account_name,
            "azure_container_name": storage_result.container_name,
            "azure_access_connector_id": storage_result.access_connector_id,
            "azure_resource_group": storage_result.resource_group,
            "azure_access_connector_name": (
                storage_result.access_connector_id.split("/")[-1]
                if storage_result.access_connector_id else None
            ),
        }

    def storage_url_for_ext_location(self, storage_result: StorageResult, run_id: str) -> str:
        return storage_result.storage_url
