"""Abstract base class for cloud provider backends."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class StorageResult:
    """Result from setup_storage() — holds cloud-specific resource identifiers."""
    # Common
    storage_url: str             # e.g. s3://bucket/prefix or abfss://container@account.dfs.core.windows.net/prefix
    credential_name: str         # name used when registering the Databricks storage credential

    # AWS-specific
    iam_role_arn: str | None = None
    bucket_name: str | None = None
    bucket_created: bool = False

    # Azure-specific
    access_connector_id: str | None = None
    storage_account_name: str | None = None
    container_name: str | None = None
    resource_group: str | None = None


@dataclass
class CredentialResult:
    """Result from register_storage_credential()."""
    credential_id: str | None = None
    # AWS: unity_catalog_iam_arn returned by the API (used to tighten trust policy)
    unity_catalog_iam_arn: str | None = None


class CloudProvider(ABC):
    """Abstract interface for cloud-specific operations in provision_test_env.py."""

    @property
    @abstractmethod
    def account_host(self) -> str:
        """Databricks account console URL.

        AWS:   https://accounts.cloud.databricks.com
        Azure: https://accounts.azuredatabricks.net
        """
        ...

    @abstractmethod
    def validate_config(self, cfg: dict[str, str]) -> None:
        """Validate that all required cloud-specific config keys are present."""
        ...

    @abstractmethod
    def get_region(self, cfg: dict[str, str]) -> str:
        """Return the cloud region from config."""
        ...

    @abstractmethod
    def setup_storage(self, cfg: dict[str, str], run_id: str, region: str, account_id: str) -> StorageResult:
        """Create cloud storage resources (bucket/container, IAM role/access connector).

        Returns a StorageResult with all identifiers needed for credential registration
        and teardown.
        """
        ...

    @abstractmethod
    def register_storage_credential(
        self,
        account_client: Any,
        metastore_id: str,
        storage_result: StorageResult,
    ) -> CredentialResult:
        """Register a storage credential in the Databricks metastore.

        Returns a CredentialResult with the credential ID and any cloud-specific
        identifiers (e.g. unity_catalog_iam_arn for AWS).
        """
        ...

    @abstractmethod
    def post_credential_setup(
        self,
        cfg: dict[str, str],
        storage_result: StorageResult,
        credential_result: CredentialResult,
        account_id: str,
        region: str,
    ) -> None:
        """Perform any post-credential-registration steps.

        For AWS: tighten the IAM trust policy to the specific UC principal ARN.
        For Azure: no-op.
        """
        ...

    @abstractmethod
    def teardown_storage(self, cfg: dict[str, str], state: dict) -> None:
        """Delete all cloud storage resources created by setup_storage().

        The state dict contains the fields saved during provisioning.
        """
        ...

    @abstractmethod
    def workspace_create_kwargs(self, region: str) -> dict:
        """Return cloud-specific kwargs for workspace creation via Databricks Account API.

        For AWS: {"aws_region": region}
        """
        ...

    def create_workspace(
        self,
        cfg: dict[str, str],
        ws_name: str,
        region: str,
        account_client: Any,
    ) -> tuple[int, str]:
        """Create a Databricks workspace and return (workspace_id, workspace_host).

        Default raises NotImplementedError, which causes provision_test_env.py
        to fall through to _create_workspace_via_account_api() — creating a
        fast serverless workspace via the Databricks Account REST API.
        This works for both AWS and Azure.
        """
        raise NotImplementedError("Subclass must implement create_workspace or use default Account API path")

    @abstractmethod
    def state_extras(self, storage_result: StorageResult) -> dict:
        """Return cloud-specific fields to save in the provision state file."""
        ...

    @abstractmethod
    def storage_url_for_ext_location(self, storage_result: StorageResult, run_id: str) -> str:
        """Return the URL to use when creating the External Location."""
        ...
