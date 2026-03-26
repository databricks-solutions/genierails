"""AWS cloud provider for provision_test_env.py."""
from __future__ import annotations

import json
import sys
import time
from typing import Any

from .base import CloudProvider, StorageResult, CredentialResult

# ---------------------------------------------------------------------------
# ANSI helpers
# ---------------------------------------------------------------------------

from ._ansi import _green, _red, _cyan, _yellow, _step, _ok, _warn, _err

# ---------------------------------------------------------------------------
# AWS / IAM helpers  (extracted from provision_test_env.py)
# ---------------------------------------------------------------------------

def _ensure_boto3() -> None:
    try:
        import boto3  # noqa: F401
    except ImportError:
        import subprocess
        _warn("boto3 not found — installing…")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "boto3"])


def _aws_session(cfg: dict[str, str], region: str):
    """Return a boto3 Session using explicit keys from cfg or the default chain."""
    import boto3

    kwargs: dict = {}
    if cfg.get("AWS_ACCESS_KEY_ID"):
        kwargs["aws_access_key_id"]     = cfg["AWS_ACCESS_KEY_ID"]
        kwargs["aws_secret_access_key"] = cfg.get("AWS_SECRET_ACCESS_KEY", "")
        if cfg.get("AWS_SESSION_TOKEN"):
            kwargs["aws_session_token"] = cfg["AWS_SESSION_TOKEN"]
    if cfg.get("AWS_PROFILE") and "aws_access_key_id" not in kwargs:
        kwargs["profile_name"] = cfg["AWS_PROFILE"]
    return boto3.Session(region_name=region, **kwargs)


def _create_uc_iam_role(
    cfg: dict,
    role_name: str,
    bucket: str,          # just the bucket name, no s3:// prefix
    account_id: str,      # Databricks account UUID (used as ExternalId)
    region: str,
) -> str:
    """Create an IAM role suitable for a UC External Location. Returns the role ARN.

    Initial trust policy allows the generic Databricks UC root principal
    (414351767826:root) to assume the role.  After the Databricks storage
    credential is created, call _update_uc_trust_policy() to tighten it to
    the specific unity_catalog_iam_arn returned by the API.
    """
    _ensure_boto3()
    session = _aws_session(cfg, region)
    iam = session.client("iam")

    # Determine the caller's AWS account ID for the ARN we're about to create.
    sts       = session.client("sts", region_name=region)
    aws_acct  = sts.get_caller_identity()["Account"]
    role_arn  = f"arn:aws:iam::{aws_acct}:role/{role_name}"

    # Initial trust: only the generic Databricks root principal.
    # We cannot include the role's own ARN here because the role doesn't exist
    # yet (circular reference).  The self-assume + specific UC principal ARN
    # are both added by _update_uc_trust_policy() after the role is created.
    initial_trust = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": "arn:aws:iam::414351767826:root"},
                "Action": "sts:AssumeRole",
                "Condition": {"StringEquals": {"sts:ExternalId": account_id}},
            },
        ],
    }

    iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(initial_trust),
        Description="Unity Catalog External Location role - provision_test_env.py",
        Tags=[
            {"Key": "ManagedBy",  "Value": "provision_test_env"},
            {"Key": "DatabricksAccountId", "Value": account_id},
        ],
    )

    # Inline S3 permission policy — scoped to the test bucket.
    s3_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                    "s3:GetBucketLocation", "s3:ListBucket",
                    "s3:ListBucketMultipartUploads", "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts",
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket}",
                    f"arn:aws:s3:::{bucket}/*",
                ],
            },
        ],
    }
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName=f"{role_name}-s3",
        PolicyDocument=json.dumps(s3_policy),
    )

    # IAM changes can take a few seconds to propagate globally.
    _warn("Waiting 15 s for IAM role to propagate…")
    time.sleep(15)
    return role_arn


def _update_uc_trust_policy(
    cfg: dict,
    role_name: str,
    role_arn: str,
    unity_catalog_iam_arn: str,
    account_id: str,
    region: str,
) -> None:
    """Narrow the trust policy to the specific Databricks UC principal."""
    _ensure_boto3()
    session = _aws_session(cfg, region)
    iam     = session.client("iam")

    trust = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": unity_catalog_iam_arn},
                "Action": "sts:AssumeRole",
                "Condition": {"StringEquals": {"sts:ExternalId": account_id}},
            },
            {
                "Effect": "Allow",
                "Principal": {"AWS": role_arn},
                "Action": "sts:AssumeRole",
            },
        ],
    }
    iam.update_assume_role_policy(
        RoleName=role_name,
        PolicyDocument=json.dumps(trust),
    )


def _ensure_s3_bucket(cfg: dict, bucket_name: str, region: str) -> bool:
    """Create the S3 bucket if it does not already exist.

    Returns True if the bucket was created by this call (and should therefore
    be deleted on teardown), False if it already existed.
    """
    _ensure_boto3()
    from botocore.exceptions import ClientError

    session = _aws_session(cfg, region)
    s3 = session.client("s3", region_name=region)

    try:
        s3.head_bucket(Bucket=bucket_name)
        _ok(f"S3 bucket already exists: s3://{bucket_name}")
        return False
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code not in ("404", "NoSuchBucket", "403"):
            raise

    if code == "403":
        _err(f"S3 bucket s3://{bucket_name} exists but is owned by another AWS account.")
        raise RuntimeError(f"Bucket s3://{bucket_name} is owned by another account (HTTP 403).")

    _step(f"Creating S3 bucket: s3://{bucket_name}  (region={region})")
    try:
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        s3.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )
        _ok(f"S3 bucket created: s3://{bucket_name}")
        return True
    except ClientError as exc:
        _err(f"Could not create S3 bucket s3://{bucket_name}: {exc}")
        raise


def _delete_s3_prefix(cfg: dict, bucket_name: str, prefix: str, region: str) -> None:
    """Delete all objects under *prefix* inside *bucket_name*."""
    _ensure_boto3()
    from botocore.exceptions import ClientError

    session = _aws_session(cfg, region)
    s3 = session.client("s3", region_name=region)

    prefix = prefix.rstrip("/") + "/"
    _step(f"Deleting S3 objects under s3://{bucket_name}/{prefix}")
    paginator = s3.get_paginator("list_objects_v2")
    deleted = 0
    try:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            objects = page.get("Contents", [])
            if not objects:
                continue
            s3.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": [{"Key": obj["Key"]} for obj in objects]},
            )
            deleted += len(objects)
    except ClientError as exc:
        _warn(f"Could not fully clean S3 prefix s3://{bucket_name}/{prefix}: {exc}")
    _ok(f"Deleted {deleted} object(s) from s3://{bucket_name}/{prefix}")


def _delete_s3_bucket(cfg: dict, bucket_name: str, region: str) -> None:
    """Empty and delete an S3 bucket that was created by _ensure_s3_bucket."""
    _ensure_boto3()
    from botocore.exceptions import ClientError

    session = _aws_session(cfg, region)
    s3 = session.client("s3", region_name=region)

    _step(f"Emptying S3 bucket: s3://{bucket_name}")
    try:
        paginator = s3.get_paginator("list_object_versions")
        for page in paginator.paginate(Bucket=bucket_name):
            to_delete = [
                {"Key": v["Key"], "VersionId": v["VersionId"]}
                for v in page.get("Versions", []) + page.get("DeleteMarkers", [])
            ]
            if to_delete:
                s3.delete_objects(Bucket=bucket_name, Delete={"Objects": to_delete})
    except ClientError:
        pass

    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name):
            objects = page.get("Contents", [])
            if objects:
                s3.delete_objects(
                    Bucket=bucket_name,
                    Delete={"Objects": [{"Key": o["Key"]} for o in objects]},
                )
    except ClientError:
        pass

    try:
        s3.delete_bucket(Bucket=bucket_name)
        _ok(f"S3 bucket deleted: s3://{bucket_name}")
    except ClientError as exc:
        _warn(f"Could not delete S3 bucket s3://{bucket_name}: {exc}")
        _warn("Delete it manually in the AWS Console → S3.")


def _delete_iam_role(cfg: dict, role_name: str, region: str) -> None:
    """Delete the IAM role created by _create_uc_iam_role (inline policies + role)."""
    _ensure_boto3()
    session = _aws_session(cfg, region)
    iam     = session.client("iam")

    try:
        for policy in iam.list_role_policies(RoleName=role_name).get("PolicyNames", []):
            iam.delete_role_policy(RoleName=role_name, PolicyName=policy)
    except iam.exceptions.NoSuchEntityException:
        return  # role doesn't exist — nothing to do

    try:
        for p in iam.list_attached_role_policies(RoleName=role_name).get("AttachedPolicies", []):
            iam.detach_role_policy(RoleName=role_name, PolicyArn=p["PolicyArn"])
    except Exception:
        pass

    try:
        iam.delete_role(RoleName=role_name)
    except Exception as exc:
        _warn(f"Could not delete IAM role {role_name!r}: {exc}")


# ---------------------------------------------------------------------------
# AWSProvider — implements the CloudProvider interface
# ---------------------------------------------------------------------------

class AWSProvider(CloudProvider):
    """AWS cloud provider — uses S3 + IAM role for Unity Catalog."""

    @property
    def account_host(self) -> str:
        return "https://accounts.cloud.databricks.com"

    def validate_config(self, cfg: dict[str, str]) -> None:
        required = ["DATABRICKS_AWS_REGION"]
        missing = [k for k in required if not cfg.get(k)]
        if missing:
            _err(f"Missing AWS config: {', '.join(missing)}")
            sys.exit(1)

    def get_region(self, cfg: dict[str, str]) -> str:
        return cfg["DATABRICKS_AWS_REGION"]

    def setup_storage(self, cfg: dict[str, str], run_id: str, region: str, account_id: str) -> StorageResult:
        _ensure_boto3()
        session = _aws_session(cfg, region)
        aws_account_id = session.client("sts", region_name=region).get_caller_identity()["Account"]
        bucket_name = f"genie-uc-test-{aws_account_id}"

        # Step 0: Ensure S3 bucket exists
        bucket_created = _ensure_s3_bucket(cfg, bucket_name, region)

        # Step 1: Create IAM role
        iam_role_name = f"genie-test-uc-role-{run_id}"
        _step(f"Creating AWS IAM role: {iam_role_name}")
        iam_role_arn = _create_uc_iam_role(
            cfg=cfg,
            role_name=iam_role_name,
            bucket=bucket_name,
            account_id=account_id,
            region=region,
        )
        _ok(f"IAM role created: {iam_role_arn}")

        storage_url = f"s3://{bucket_name}"
        ext_loc_url = f"{storage_url}/genie-test-{run_id}"

        return StorageResult(
            storage_url=ext_loc_url,
            credential_name="test-ext-loc-cred",
            iam_role_arn=iam_role_arn,
            bucket_name=bucket_name,
            bucket_created=bucket_created,
        )

    def register_storage_credential(
        self,
        account_client: Any,
        metastore_id: str,
        storage_result: StorageResult,
    ) -> CredentialResult:
        from databricks.sdk.service.catalog import (
            CreateAccountsStorageCredential,
            AwsIamRoleRequest,
        )

        _step("Registering storage credential in metastore")
        try:
            new_cred_resp = account_client.storage_credentials.create(
                metastore_id=metastore_id,
                credential_info=CreateAccountsStorageCredential(
                    name=storage_result.credential_name,
                    aws_iam_role=AwsIamRoleRequest(role_arn=storage_result.iam_role_arn),
                    comment="Storage credential for test External Location — provision_test_env.py",
                ),
            )
            new_cred_info = getattr(new_cred_resp, "credential_info", None) or new_cred_resp
            storage_cred_id = new_cred_info.id
            # Extract the Databricks-side UC IAM ARN so we can narrow the trust policy.
            aws_iam_role_info = getattr(new_cred_info, "aws_iam_role", None)
            unity_catalog_iam_arn = getattr(aws_iam_role_info, "unity_catalog_iam_arn", None)
            _ok(f"Storage credential registered: id={storage_cred_id!r}")
            if unity_catalog_iam_arn:
                _ok(f"Databricks UC principal  : {unity_catalog_iam_arn}")
            return CredentialResult(
                credential_id=storage_cred_id,
                unity_catalog_iam_arn=unity_catalog_iam_arn,
            )
        except Exception as exc:
            _warn(f"Could not register storage credential: {exc}")
            _warn("External location creation will fail.")
            return CredentialResult()

    def post_credential_setup(
        self,
        cfg: dict[str, str],
        storage_result: StorageResult,
        credential_result: CredentialResult,
        account_id: str,
        region: str,
    ) -> None:
        if credential_result.unity_catalog_iam_arn:
            _step("Updating IAM trust policy with Databricks UC principal")
            # Derive role_name from the ARN: arn:aws:iam::XXXX:role/<role_name>
            role_name = storage_result.iam_role_arn.rsplit("/", 1)[-1] if storage_result.iam_role_arn else ""
            try:
                _update_uc_trust_policy(
                    cfg=cfg,
                    role_name=role_name,
                    role_arn=storage_result.iam_role_arn,
                    unity_catalog_iam_arn=credential_result.unity_catalog_iam_arn,
                    account_id=account_id,
                    region=region,
                )
                _ok("Trust policy updated")
                # AWS IAM is eventually consistent — wait for the updated trust
                # policy (including the self-assume statement) to propagate before
                # Databricks validates it when creating the External Location.
                # 15 s is often too short; use 60 s to avoid transient failures.
                _warn("Waiting 60 s for trust policy propagation…")
                time.sleep(60)
            except Exception as exc:
                _warn(f"Could not update trust policy: {exc}")
                _warn("Storage credential may not work. Update the trust policy manually in AWS IAM.")

    def teardown_storage(self, cfg: dict[str, str], state: dict) -> None:
        iam_role_name  = state.get("iam_role_name")
        region         = state.get("region", "us-east-1")
        bucket_name    = state.get("bucket_name")
        bucket_created = state.get("bucket_created", False)
        ext_loc_prefix = state.get("ext_loc_prefix", "")

        # Delete IAM role
        if iam_role_name:
            _step(f"Deleting AWS IAM role: {iam_role_name}")
            try:
                _delete_iam_role(cfg, iam_role_name, region)
                _ok(f"IAM role deleted: {iam_role_name}")
            except Exception as exc:
                _warn(f"Could not delete IAM role {iam_role_name!r}: {exc}")
                if "ExpiredToken" in str(exc) or "expired" in str(exc).lower():
                    _warn("Your AWS session token has expired.  To retry with fresh credentials:")
                    _warn("  1. Export new tokens:  AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_SESSION_TOKEN=...")
                    _warn("  2. Re-run teardown:    python scripts/provision_test_env.py teardown")
                    _warn("  Or delete manually:   AWS Console → IAM → Roles → search for the name above.")
                else:
                    _warn("Delete it manually in the AWS console: IAM → Roles → search for the name above.")
        else:
            _step("No IAM role in state — skipping IAM deletion")

        # S3 cleanup
        if bucket_name and ext_loc_prefix:
            try:
                _delete_s3_prefix(cfg, bucket_name, ext_loc_prefix, region)
            except Exception as exc:
                _warn(f"Could not clean S3 prefix: {exc}")
        if bucket_name and bucket_created:
            _step(f"Deleting S3 bucket created by provision: s3://{bucket_name}")
            try:
                _delete_s3_bucket(cfg, bucket_name, region)
            except Exception as exc:
                _warn(f"Could not delete S3 bucket: {exc}")
                _warn(f"Delete it manually: aws s3 rb s3://{bucket_name} --force")
        elif bucket_name:
            _ok(f"Bucket s3://{bucket_name} was pre-existing — not deleted.")

    def workspace_create_kwargs(self, region: str) -> dict:
        return {"aws_region": region}

    def state_extras(self, storage_result: StorageResult) -> dict:
        # Derive role_name from the ARN
        role_name = storage_result.iam_role_arn.rsplit("/", 1)[-1] if storage_result.iam_role_arn else None
        return {
            "iam_role_name":  role_name,
            "bucket_name":    storage_result.bucket_name,
            "bucket_created": storage_result.bucket_created,
            "ext_loc_prefix": storage_result.storage_url.rsplit("/", 1)[-1] if storage_result.storage_url else None,
        }

    def storage_url_for_ext_location(self, storage_result: StorageResult, run_id: str) -> str:
        return storage_result.storage_url
