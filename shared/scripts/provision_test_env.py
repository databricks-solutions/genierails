#!/usr/bin/env python3
"""
Provision a fresh integration-test environment: one serverless Databricks workspace
+ one Unity Catalog metastore, with the test Service Principal set as both
metastore admin and workspace admin.

Solves the FGAC quota counter drift problem by giving every integration-test
run a brand-new metastore (counter always starts at 0).  After the run, call
`teardown` to delete the workspace and metastore — wiping all state cleanly.

Usage
-----
  # One-time: `make setup` creates scripts/account-admin.<cloud>.env automatically.
  # Fill in your account-admin SP credentials, then provision:
  python scripts/provision_test_env.py provision

  # Check what is currently provisioned
  python scripts/provision_test_env.py status

  # Run the integration tests against the provisioned environment
  python scripts/run_integration_tests.py --scenario all

  # Tear down everything when done
  python scripts/provision_test_env.py teardown

Environment file (scripts/account-admin.<cloud>.env)
-----------------------------------------------------
  DATABRICKS_ACCOUNT_ID       = <your Databricks account UUID>
  DATABRICKS_CLIENT_ID        = <SP application/client ID — must have Account Admin>
  DATABRICKS_CLIENT_SECRET    = <SP OAuth secret>
  DATABRICKS_AWS_REGION       = ap-southeast-2   # or your region

  # AWS credentials (needed to create/delete the UC IAM role automatically).
  # AWS credentials (needed to create/delete the UC IAM role + S3 bucket).
  # Leave blank to use the default boto3 chain: ~/.aws/credentials, AWS_PROFILE,
  # instance profile, etc.
  AWS_ACCESS_KEY_ID     =
  AWS_SECRET_ACCESS_KEY =

Storage setup (fully automated)
--------------------------------
  The provision script creates everything needed in AWS and Databricks:

  0. S3 bucket        — named genie-uc-test-<aws-account-id>.  Created on first
                        run; reused if it already exists.  Deleted on teardown.
  1. AWS IAM role     — a fresh role scoped to the test-run S3 prefix, with the
                        correct UC trust policy (principal = Databricks UC service).
  0. S3 bucket     — named genie-uc-test-<aws-account-id>.  Created on first
                     run; reused if it already exists.
  1. AWS IAM role  — a fresh role scoped to the test-run S3 prefix, with the
                     correct UC trust policy (principal = Databricks UC service).
  2. Storage credential — registered in the new metastore via the Databricks API.
  3. External Location  — path-scoped S3 prefix for this run registered in the
                          new workspace, so catalogs can be created without a
                          metastore-level storage root.

  The IAM role and S3 test prefix are both deleted automatically by `teardown`.
  The bucket itself is deleted only if this script created it.

  You only need to provide:
    • AWS credentials with permission to create/delete IAM roles, S3 buckets,
      and policies (iam:CreateRole, iam:DeleteRole, iam:PutRolePolicy,
      iam:DeleteRolePolicy, iam:UpdateAssumeRolePolicy, sts:GetCallerIdentity,
      s3:CreateBucket, s3:DeleteBucket, s3:PutPublicAccessBlock,
      s3:ListBucketVersions, s3:DeleteObject, s3:DeleteObjectVersion).
       iam:DeleteRolePolicy, iam:UpdateAssumeRolePolicy, sts:GetCallerIdentity,
       s3:CreateBucket, s3:PutPublicAccessBlock, s3:HeadBucket).

State file
----------
  scripts/.test_env_state.json  — written by provision, read by teardown.
  This file is gitignored.  If it is missing, teardown does nothing.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import ssl
import sys
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR  = Path(__file__).resolve().parent
MODULE_ROOT = SCRIPT_DIR.parent                          # …/genie/shared/
# Infer cloud from CLOUD_ROOT path name (set by Makefile), fall back to CLOUD_PROVIDER env var.
_cloud_root_env = Path(os.environ.get("CLOUD_ROOT", ""))
_default_cloud  = (
    _cloud_root_env.name
    if _cloud_root_env.name in ("aws", "azure")
    else os.environ.get("CLOUD_PROVIDER", "aws").lower()
)
CLOUD_ROOT  = Path(os.environ.get("CLOUD_ROOT", MODULE_ROOT.parent / _default_cloud))
ENVS_DIR    = CLOUD_ROOT / "envs"                       # user's real envs (never touched)
# Support per-scenario parallel provisioning: _PARALLEL_STATE_FILE overrides the
# default state file and test envs directory so each scenario gets its own workspace.
_parallel_state = os.environ.get("_PARALLEL_STATE_FILE", "")
_parallel_suite = os.environ.get("_PARALLEL_SUITE_ID", "").strip()
if _parallel_state:
    STATE_FILE = Path(_parallel_state)
    _scenario_name = STATE_FILE.stem.split(".")[-1]
    if _parallel_suite:
        TEST_ENVS_DIR = (
            CLOUD_ROOT / "envs" / "parallel_test" / _parallel_suite / _scenario_name
        )
    else:
        TEST_ENVS_DIR = CLOUD_ROOT / "envs" / "parallel_test" / _scenario_name
else:
    STATE_FILE = SCRIPT_DIR / f".test_env_state.{_default_cloud}.json"
    TEST_ENVS_DIR = CLOUD_ROOT / "envs" / "test"
DEFAULT_ENV_FILE = SCRIPT_DIR / f"account-admin.{_default_cloud}.env"


def _display_path(p: Path) -> Path:
    """Return a short relative path for display, trying MODULE_ROOT then CLOUD_ROOT."""
    for base in (MODULE_ROOT, CLOUD_ROOT):
        try:
            return p.relative_to(base)
        except ValueError:
            continue
    return p


# ---------------------------------------------------------------------------
# ANSI helpers
# ---------------------------------------------------------------------------

from cloud_providers._ansi import _green, _red, _cyan, _yellow, _step, _ok, _warn, _err

def _bold(s: str)  -> str:  return f"\033[1m{s}\033[0m"

def _banner(title: str) -> None:
    width = 66
    print()
    print("=" * width)
    print(f"  {title}")
    print("=" * width)

# ---------------------------------------------------------------------------
# AWS / IAM helpers  (boto3-based, installed on demand)
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
            # Unexpected error — re-raise so it surfaces clearly.
            raise

    # Bucket does not exist (404/NoSuchBucket) or we got 403 on a bucket name
    # that belongs to someone else.  403 is treated as "exists but not ours";
    # only 404/NoSuchBucket means we should create it.
    if code == "403":
        _err(f"S3 bucket s3://{bucket_name} exists but is owned by another AWS account.")
        _err("Choose a different bucket name in DATABRICKS_S3_BUCKET.")
        raise RuntimeError(f"Bucket s3://{bucket_name} is owned by another account (HTTP 403).")

    _step(f"Creating S3 bucket: s3://{bucket_name}  (region={region})")
    try:
        if region == "us-east-1":
            # us-east-1 does NOT accept a LocationConstraint — it's the default.
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        # Block public access — best practice for UC storage buckets.
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

    # Delete all object versions and delete-markers (handles versioned buckets).
    _step(f"Emptying S3 bucket: s3://{bucket_name}")
    try:
        paginator = s3.get_paginator("list_object_versions")
        deleted = 0
        for page in paginator.paginate(Bucket=bucket_name):
            to_delete = [
                {"Key": v["Key"], "VersionId": v["VersionId"]}
                for v in page.get("Versions", []) + page.get("DeleteMarkers", [])
            ]
            if to_delete:
                s3.delete_objects(Bucket=bucket_name, Delete={"Objects": to_delete})
                deleted += len(to_delete)
    except ClientError as exc:
        _warn(f"Could not empty bucket (may have no versioning): {exc}")

    # Also handle non-versioned objects.
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name):
            objects = page.get("Contents", [])
            if objects:
                s3.delete_objects(
                    Bucket=bucket_name,
                    Delete={"Objects": [{"Key": o["Key"]} for o in objects]},
                )
    except ClientError as exc:
        _warn(f"Could not clean non-versioned objects: {exc}")

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


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def _load_env_file(path: Path) -> dict[str, str]:
    """Parse a KEY=VALUE .env file (comments and blank lines ignored)."""
    cfg: dict[str, str] = {}
    if not path.exists():
        return cfg
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, _, v = line.partition("=")
            cfg[k.strip()] = v.strip().strip('"').strip("'")
    return cfg


def _load_config(env_file: Path) -> dict[str, str]:
    """Load config from env file, with os.environ overrides."""
    cfg = _load_env_file(env_file)
    for key in [
        "DATABRICKS_ACCOUNT_ID",
        "DATABRICKS_CLIENT_ID",
        "DATABRICKS_CLIENT_SECRET",
        "DATABRICKS_AWS_REGION",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_PROFILE",
        "CLOUD_PROVIDER",
        "AZURE_SUBSCRIPTION_ID",
        "AZURE_RESOURCE_GROUP",
        "AZURE_REGION",
        "AZURE_TENANT_ID",
        "AZURE_CLIENT_ID",
        "AZURE_CLIENT_SECRET",
    ]:
        if key in os.environ:
            cfg[key] = os.environ[key]
    return cfg


def _validate_config(cfg: dict[str, str]) -> None:
    required = [
        "DATABRICKS_ACCOUNT_ID",
        "DATABRICKS_CLIENT_ID",
        "DATABRICKS_CLIENT_SECRET",
    ]
    missing = [k for k in required if not cfg.get(k)]
    if missing:
        _err(f"Missing required config keys: {', '.join(missing)}")
        print(f"\n  Edit {DEFAULT_ENV_FILE} and fill in all values.", file=sys.stderr)
        sys.exit(1)
    # Cloud-specific validation is done by the provider


def _get_cloud_provider(cfg: dict) -> "CloudProvider":
    """Return the appropriate cloud provider based on config."""
    cloud = cfg.get("CLOUD_PROVIDER", _default_cloud).lower()
    if cloud == "aws":
        from cloud_providers.aws_provider import AWSProvider
        return AWSProvider()
    elif cloud == "azure":
        from cloud_providers.azure_provider import AzureProvider
        return AzureProvider()
    raise ValueError(f"Unsupported CLOUD_PROVIDER: {cloud}")

# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------

def _save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


def _load_state() -> dict | None:
    if not STATE_FILE.exists():
        return None
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return None


def _clear_state() -> None:
    if STATE_FILE.exists():
        STATE_FILE.unlink()

# ---------------------------------------------------------------------------
# Workspace URL helper
# ---------------------------------------------------------------------------

def _workspace_host(workspace) -> str:
    """Derive the workspace HTTPS host from a Workspace SDK object."""
    # The SDK returns deployment_name like "dbc-b89659bd-e807"
    # Workspace URL is https://<deployment_name>.cloud.databricks.com
    if hasattr(workspace, "workspace_url") and workspace.workspace_url:
        url = workspace.workspace_url
        return url if url.startswith("https://") else f"https://{url}"
    deployment = getattr(workspace, "deployment_name", None)
    if deployment:
        # Azure deployments end with .azuredatabricks.net; AWS with .cloud.databricks.com
        if ".azuredatabricks.net" in deployment or ".cloud.databricks.com" in deployment:
            return f"https://{deployment}"
        return f"https://{deployment}.cloud.databricks.com"
    return f"https://dbc-{workspace.workspace_id}.cloud.databricks.com"

# ---------------------------------------------------------------------------
# REST API helpers for workspace provisioning
# (used when the installed SDK is too old to support compute_mode=SERVERLESS)
# ---------------------------------------------------------------------------

# Default is AWS; overridden by provider.account_host before first use.
_ACCOUNT_HOST = "https://accounts.cloud.databricks.com"
_SSL_CTX = ssl.create_default_context()


def _set_account_host(host: str) -> None:
    """Override the account host used by REST helper functions."""
    global _ACCOUNT_HOST
    _ACCOUNT_HOST = host


def _oauth_token(account_id: str, client_id: str, client_secret: str) -> str:
    """Obtain an OAuth2 M2M access token from the Databricks account."""
    url  = f"{_ACCOUNT_HOST}/oidc/accounts/{account_id}/v1/token"
    cred = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    data = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "scope": "all-apis",
    }).encode()
    req = urllib.request.Request(url, data=data, headers={
        "Authorization": f"Basic {cred}",
        "Content-Type": "application/x-www-form-urlencoded",
    })
    with urllib.request.urlopen(req, context=_SSL_CTX) as resp:
        return json.loads(resp.read())["access_token"]


def _account_get(token: str, path: str) -> dict:
    req = urllib.request.Request(
        f"{_ACCOUNT_HOST}{path}",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req, context=_SSL_CTX) as resp:
        return json.loads(resp.read())


def _account_post(token: str, path: str, body: dict) -> dict:
    data = json.dumps(body).encode()
    req  = urllib.request.Request(
        f"{_ACCOUNT_HOST}{path}",
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, context=_SSL_CTX) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body_bytes = e.read()
        try:
            detail = json.loads(body_bytes).get("message", body_bytes.decode())
        except Exception:
            detail = body_bytes.decode(errors="replace")
        raise RuntimeError(f"HTTP {e.code} {e.reason} — {detail}") from e


def _create_serverless_workspace_rest(
    account_id: str,
    client_id: str,
    client_secret: str,
    ws_name: str,
    region: str,
    timeout_s: int = 1200,
    cloud_kwargs: dict | None = None,
) -> tuple[int, str]:
    """Create a serverless workspace via REST API and wait until RUNNING.

    Returns (workspace_id, workspace_host).
    Used when the installed databricks-sdk is too old to support compute_mode.
    """
    token = _oauth_token(account_id, client_id, client_secret)

    payload = {
        "workspace_name": ws_name,
        "pricing_tier":   "ENTERPRISE",
        "compute_mode":   "SERVERLESS",
    }
    if cloud_kwargs:
        payload.update(cloud_kwargs)
    else:
        payload["aws_region"] = region

    ws_data = _account_post(token, f"/api/2.0/accounts/{account_id}/workspaces", payload)
    ws_id = ws_data["workspace_id"]
    print(f"  Workspace ID {ws_id} created — polling for RUNNING state…")

    deadline = time.time() + timeout_s
    poll = 30
    while time.time() < deadline:
        time.sleep(poll)
        data   = _account_get(token, f"/api/2.0/accounts/{account_id}/workspaces/{ws_id}")
        status = data.get("workspace_status", "UNKNOWN")
        msg    = data.get("workspace_status_message", "")
        print(f"  [{int(time.time() % 100000)}]  {status}  {msg}")
        if status == "RUNNING":
            deployment = data.get("deployment_name", "")
            if deployment:
                if ".azuredatabricks.net" in deployment or ".cloud.databricks.com" in deployment:
                    host = f"https://{deployment}"
                else:
                    host = f"https://{deployment}.cloud.databricks.com"
            else:
                host = f"https://dbc-{ws_id}.cloud.databricks.com"
            return ws_id, host
        if status in ("FAILED", "BANNED", "CANCELLED"):
            raise RuntimeError(f"Workspace creation failed: {status} — {msg}")
        poll = min(poll + 10, 60)   # back off up to 60s between polls

    raise TimeoutError(f"Workspace did not reach RUNNING within {timeout_s // 60} minutes")


# ---------------------------------------------------------------------------
# Auth file writer
# ---------------------------------------------------------------------------


def _create_workspace_via_account_api(
    account_client,
    provider,
    ws_name: str,
    region: str,
    account_id: str,
    client_id: str,
    client_secret: str,
    has_compute_mode: bool,
) -> tuple[int, str]:
    """Create a workspace using the Databricks Account API (AWS path)."""
    from databricks.sdk.service.provisioning import PricingTier

    _step(f"Creating serverless workspace: {ws_name}")
    print("  (This typically takes 10-15 minutes — please wait…)")

    ws_cloud_kwargs = provider.workspace_create_kwargs(region)
    # The SDK path only works for AWS (it expects aws_region, not location).
    # For non-AWS clouds, always use the REST path which handles cloud_kwargs generically.
    use_sdk = has_compute_mode and "aws_region" in ws_cloud_kwargs
    if use_sdk:
        from databricks.sdk.service.provisioning import CustomerFacingComputeMode
        ws_obj = account_client.workspaces.create_and_wait(
            workspace_name=ws_name,
            pricing_tier=PricingTier.ENTERPRISE,
            compute_mode=CustomerFacingComputeMode.SERVERLESS,
            **ws_cloud_kwargs,
        )
        ws_id = ws_obj.workspace_id
        ws_host = _workspace_host(ws_obj)
    else:
        ws_id, ws_host = _create_serverless_workspace_rest(
            account_id, client_id, client_secret, ws_name, region,
            cloud_kwargs=ws_cloud_kwargs,
        )
    return ws_id, ws_host


def _write_auth_file(
    env: str,
    account_id: str,
    client_id: str,
    client_secret: str,
    workspace_id: int,
    workspace_host: str,
    account_host: str = "https://accounts.cloud.databricks.com",
) -> Path:
    """Write envs/<env>/auth.auto.tfvars with the provisioned credentials."""
    env_dir = ENVS_DIR / env
    env_dir.mkdir(parents=True, exist_ok=True)
    (env_dir / "data_access").mkdir(parents=True, exist_ok=True)

    content = f"""\
# Generated by provision_test_env.py — DO NOT EDIT MANUALLY.
# Run `python scripts/provision_test_env.py teardown` to clean up.
# This file is gitignored.

databricks_account_host  = "{account_host}"
databricks_account_id    = "{account_id}"
databricks_client_id     = "{client_id}"
databricks_client_secret = "{client_secret}"
databricks_workspace_id  = "{workspace_id}"
databricks_workspace_host = "{workspace_host}"
"""
    auth_file = env_dir / "auth.auto.tfvars"
    auth_file.write_text(content)
    return auth_file

# ---------------------------------------------------------------------------
# Provision
# ---------------------------------------------------------------------------

def cmd_provision(cfg: dict[str, str], dry_run: bool = False, force: bool = False, env_file: Path | None = None) -> None:
    _banner("Provision Fresh Integration-Test Environment")

    account_id    = cfg["DATABRICKS_ACCOUNT_ID"]
    client_id     = cfg["DATABRICKS_CLIENT_ID"]
    client_secret = cfg["DATABRICKS_CLIENT_SECRET"]

    # Instantiate the cloud provider plugin and validate cloud-specific config.
    provider = _get_cloud_provider(cfg)
    provider.validate_config(cfg)
    region = provider.get_region(cfg)

    # Check if already provisioned
    existing = _load_state()
    if existing:
        if force:
            _warn("Existing environment found — tearing it down before re-provisioning (--force).")
            _warn(f"  Workspace:  {existing.get('workspace_name')} ({existing.get('workspace_id')})")
            _warn(f"  Metastore:  {existing.get('metastore_name')} ({existing.get('metastore_id')})")
            cmd_teardown(env_file=env_file)
        else:
            _warn("A provisioned environment already exists (found .test_env_state.json).")
            _warn(f"  Workspace:  {existing.get('workspace_name')} ({existing.get('workspace_id')})")
            _warn(f"  Metastore:  {existing.get('metastore_name')} ({existing.get('metastore_id')})")
            print("\n  Run `teardown` first, or use --force to replace it.", file=sys.stderr)
            sys.exit(1)

    run_id         = uuid.uuid4().hex[:10]
    ws_name        = f"genie-test-{run_id}"
    ms_name        = f"genie-test-ms-{run_id}"

    print(f"\n  Run ID          : {run_id}")
    print(f"  Workspace       : {ws_name}")
    print(f"  Metastore       : {ms_name}")
    print(f"  Region          : {region}")
    print(f"  Cloud provider  : {cfg.get('CLOUD_PROVIDER', _default_cloud)}")
    print(f"  SP (admin)      : {client_id}")

    if dry_run:
        print("\n  [DRY RUN] No resources will be created.")
        return

    # ------------------------------------------------------------------
    # Step 0: Create cloud storage resources (S3 bucket + IAM role for AWS,
    # ADLS container + Access Connector for Azure, etc.).
    # The provider handles all cloud-specific resource creation.
    # ------------------------------------------------------------------
    storage_result = provider.setup_storage(cfg, run_id, region, account_id)
    ext_loc_url = provider.storage_url_for_ext_location(storage_result, run_id)

    # ------------------------------------------------------------------
    # Ensure the SDK is new enough to support compute_mode=SERVERLESS.
    # CustomerFacingComputeMode was added in databricks-sdk 0.6x.
    # If it's missing, upgrade automatically (requires internet access).
    # ------------------------------------------------------------------
    _has_compute_mode = False
    try:
        from databricks.sdk.service.provisioning import CustomerFacingComputeMode
        _COMPUTE_MODE = CustomerFacingComputeMode.SERVERLESS
        _has_compute_mode = True
    except ImportError:
        pass

    if not _has_compute_mode:
        import importlib
        import subprocess as _sp
        _warn("databricks-sdk is too old (CustomerFacingComputeMode missing).")
        _warn("Upgrading automatically…  (pip install --upgrade databricks-sdk)")
        result = _sp.run(
            [sys.executable, "-m", "pip", "install", "--upgrade", "databricks-sdk"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            _err("Upgrade failed. Run manually:  pip install --upgrade databricks-sdk")
            _err(result.stderr[-1000:])
            sys.exit(1)
        # Reload the module so the upgraded version is used in this process.
        import databricks.sdk.service.provisioning as _prov_mod
        importlib.reload(_prov_mod)
        try:
            from databricks.sdk.service.provisioning import CustomerFacingComputeMode
            _COMPUTE_MODE = CustomerFacingComputeMode.SERVERLESS
            _has_compute_mode = True
            _ok("databricks-sdk upgraded successfully.")
        except ImportError:
            # Unlikely after a successful upgrade, but fall back to REST.
            _warn("Could not import CustomerFacingComputeMode even after upgrade.")
            _warn("Will use REST API fallback for workspace creation.")

    from databricks.sdk import AccountClient
    from databricks.sdk.service.provisioning import PricingTier
    from databricks.sdk.service.iam import WorkspacePermission, ComplexValue
    from databricks.sdk.service.catalog import (
        CreateAccountsMetastore,
        CreateMetastoreAssignment,
    )

    account_host = provider.account_host
    _set_account_host(account_host)  # for REST fallback functions
    a = AccountClient(
        host=account_host,
        account_id=account_id,
        client_id=client_id,
        client_secret=client_secret,
    )

    # ------------------------------------------------------------------
    # Step 1: Resolve the SP's SCIM ID
    # ------------------------------------------------------------------
    _step("Resolving Service Principal SCIM identity")
    sp_scim_id: int | None = None
    try:
        for sp in a.service_principals.list():
            if str(sp.application_id) == str(client_id):
                sp_scim_id = sp.id
                _ok(f"Found SP: display_name={sp.display_name!r}  scim_id={sp.id}  app_id={sp.application_id}")
                break
        if sp_scim_id is None:
            _warn("Could not find SP by client_id in account.")
    except Exception as exc:
        _warn(f"SP lookup failed: {exc}")

    # ------------------------------------------------------------------
    # Step 2: Create admin group and add SP as a member
    # ------------------------------------------------------------------
    group_name = f"genie-test-admins-{run_id}"
    _step(f"Creating admin group: {group_name}")
    group_id: str | None = None
    try:
        members = [ComplexValue(value=str(sp_scim_id))] if sp_scim_id else []
        grp = a.groups.create(display_name=group_name, members=members)
        group_id = grp.id
        _ok(f"Group created: id={group_id}  members={[m.value for m in (grp.members or [])]}")
    except Exception as exc:
        _warn(f"Could not create admin group: {exc}")
        _warn("Will fall back to assigning the SP directly.")

    # Shared mutable state dict — saved incrementally after each step so
    # that a crash mid-way still leaves enough info for teardown.
    state: dict = {
        "run_id":          run_id,
        "cloud_provider":  cfg.get("CLOUD_PROVIDER", _default_cloud).lower(),
        "workspace_name":  ws_name,
        "workspace_id":    None,
        "workspace_host":  None,
        "metastore_name":  ms_name,
        "metastore_id":    None,
        "ext_loc_url":     ext_loc_url,
        "region":          region,
        "account_id":        account_id,
        "sp_client_id":      client_id,
        "sp_scim_id":        sp_scim_id,
        "admin_group_name":  group_name,
        "admin_group_id":    group_id,
        "written_auth_envs": [],
        "provisioned_at":    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        **provider.state_extras(storage_result),
    }

    # ------------------------------------------------------------------
    # Step 2: Create serverless workspace (fire and wait)
    # ------------------------------------------------------------------
    # Azure workspaces are ARM resources — use provider.create_workspace().
    # AWS workspaces use the Databricks Account API.
    if hasattr(provider, 'create_workspace') and callable(getattr(provider, 'create_workspace', None)):
        try:
            ws_id, ws_host = provider.create_workspace(cfg, ws_name, region, a)
        except NotImplementedError:
            ws_id, ws_host = _create_workspace_via_account_api(
                a, provider, ws_name, region, account_id, client_id, client_secret,
                _has_compute_mode,
            )
    else:
        ws_id, ws_host = _create_workspace_via_account_api(
            a, provider, ws_name, region, account_id, client_id, client_secret,
            _has_compute_mode,
        )

    state["workspace_id"]   = ws_id
    state["workspace_host"] = ws_host
    _save_state(state)   # save now — workspace exists even if later steps fail
    _ok(f"Workspace ready: id={ws_id}  host={ws_host}")

    # ------------------------------------------------------------------
    # Step 3: Create a fresh Unity Catalog metastore (no storage_root).
    # Storage is managed via an External Location (created below),
    # which is the recommended UC pattern — External Locations provide
    # fine-grained, path-scoped access instead of a metastore-wide root.
    # ------------------------------------------------------------------
    _step(f"Creating Unity Catalog metastore: {ms_name}")
    try:
        ms_resp = a.metastores.create(
            metastore_info=CreateAccountsMetastore(
                name=ms_name,
                region=region,
                # No storage_root — catalogs will use an explicit External Location.
            )
        )
    except Exception as exc:
        # Metastore creation failed (e.g. region limit reached).
        # Clean up the workspace, group, and storage that were already created
        # so they don't become permanent orphans.
        _err(f"Metastore creation failed: {exc}")
        _err("Cleaning up already-created resources to prevent orphans...")
        try:
            provider.teardown_storage(cfg, state)
        except Exception:
            pass
        ws_id_cleanup = state.get("workspace_id")
        if ws_id_cleanup:
            try:
                if hasattr(provider, 'teardown_workspace') and callable(getattr(provider, 'teardown_workspace', None)):
                    provider.teardown_workspace(cfg, state)
                else:
                    a.workspaces.delete(workspace_id=int(ws_id_cleanup))
            except Exception:
                pass
        grp_id_cleanup = state.get("admin_group_id")
        if grp_id_cleanup:
            try:
                a.groups.delete(id=grp_id_cleanup)
            except Exception:
                pass
        raise
    ms_info = getattr(ms_resp, "metastore_info", None) or ms_resp
    ms_id   = ms_info.metastore_id
    state["metastore_id"] = ms_id
    _save_state(state)
    _ok(f"Metastore created: id={ms_id}")

    # ------------------------------------------------------------------
    # Step 3c: Register the storage credential in the new metastore.
    # The provider handles cloud-specific credential creation (AWS IAM
    # role request, Azure managed identity, etc.).
    # ------------------------------------------------------------------
    credential_result = provider.register_storage_credential(a, ms_id, storage_result)
    storage_cred_id = credential_result.credential_id

    # ------------------------------------------------------------------
    # Step 3d: Perform any post-credential-registration steps.
    # For AWS: tighten the IAM trust policy to the specific UC principal.
    # For Azure: no-op.
    # ------------------------------------------------------------------
    provider.post_credential_setup(cfg, storage_result, credential_result, account_id, region)

    # ------------------------------------------------------------------
    # Step 4: Assign metastore to workspace
    # ------------------------------------------------------------------
    _step("Assigning metastore to workspace")
    a.metastore_assignments.create(
        workspace_id=ws_id,
        metastore_id=ms_id,
        metastore_assignment=CreateMetastoreAssignment(
            workspace_id=ws_id,
            metastore_id=ms_id,
            default_catalog_name="main",
        ),
    )
    _ok("Metastore assigned to workspace")

    # ------------------------------------------------------------------
    # Step 5a: Assign workspace admin BEFORE creating the external location.
    # The SP needs workspace access to call the workspace API.  On AWS the
    # SP as metastore creator has implicit access; on Azure it doesn't.
    # ------------------------------------------------------------------
    principals_to_assign = []
    if group_id:
        principals_to_assign.append((group_id, f"group '{group_name}'"))
    if sp_scim_id:
        principals_to_assign.append((sp_scim_id, f"SP (scim_id={sp_scim_id})"))

    if principals_to_assign:
        for principal_id, label in principals_to_assign:
            _step(f"Adding {label} as workspace admin")
            try:
                a.workspace_assignment.update(
                    workspace_id=ws_id,
                    principal_id=principal_id,
                    permissions=[WorkspacePermission.ADMIN],
                )
                _ok(f"Workspace admin granted to {label}")
            except Exception as exc:
                _warn(f"Could not assign workspace admin to {label}: {exc}")

    _warn("Waiting 20 s for workspace identity propagation…")
    time.sleep(20)

    # Step 5a-2: Explicitly grant metastore admin to the SP via workspace API.
    # On Azure, implicit metastore admin from being the creator is not always
    # recognized when accessing through the workspace API. Explicit grant ensures
    # FGAC policy creation (CREATE_FUNCTION, tag assignments) works.
    try:
        from databricks.sdk import WorkspaceClient as _WC_ma
        from databricks.sdk.service.catalog import PermissionsChange, Privilege
        w_admin = _WC_ma(host=ws_host, client_id=client_id, client_secret=client_secret)
        w_admin.grants.update(
            securable_type="metastore",
            full_name=ms_id,
            changes=[PermissionsChange(
                principal=client_id,
                add=[Privilege.CREATE_CATALOG, Privilege.CREATE_EXTERNAL_LOCATION],
            )],
        )
        _ok("Metastore privileges granted to SP via workspace API")
    except Exception as exc:
        _warn(f"Could not grant metastore privileges (SP may already have them as creator): {exc}")

    # ------------------------------------------------------------------
    # Step 5c: Enable Partner Powered AI and warm up the Genie API.
    #
    # On AWS (especially ap-southeast-2), the account-level "Enforce On"
    # setting does not propagate instantly to new workspaces.  We:
    #   1. Enable the workspace-level setting via PATCH.
    #   2. Create a throwaway Genie Space and poll GET until the read-back
    #      succeeds (confirming the Genie API is fully functional).
    #   3. Delete the throwaway space.
    #
    # This "pre-warm" avoids a 10+ minute surprise during the test run.
    # ------------------------------------------------------------------
    _step("Enabling Partner Powered AI on workspace")
    try:
        import json as _json_ppai
        import ssl as _ssl_ppai
        import urllib.request as _ur_ppai

        _ctx_ppai = _ssl_ppai.create_default_context()
        _ctx_ppai.check_hostname = False
        _ctx_ppai.verify_mode = _ssl_ppai.CERT_NONE

        from databricks.sdk import WorkspaceClient as _WC_ppai
        _w_ppai = _WC_ppai(host=ws_host, client_id=client_id, client_secret=client_secret)
        _token_ppai = _w_ppai.config.authenticate()

        _base_ppai = ws_host.rstrip("/")
        _settings_url = f"{_base_ppai}/api/2.0/settings/types/llm_proxy_partner_powered/names/default"

        # GET current etag
        _get_req = _ur_ppai.Request(_settings_url, headers=_token_ppai)
        with _ur_ppai.urlopen(_get_req, timeout=30, context=_ctx_ppai) as _r:
            _current = _json_ppai.loads(_r.read())
        _etag = _current.get("etag", "")

        # PATCH to enable
        _patch_body = _json_ppai.dumps({
            "allow_missing": True,
            "field_mask": "boolean_val",
            "setting": {
                "etag": _etag,
                "boolean_val": {"value": True},
            },
        }).encode()
        _patch_req = _ur_ppai.Request(
            _settings_url,
            data=_patch_body,
            method="PATCH",
            headers={**_token_ppai, "Content-Type": "application/json"},
        )
        with _ur_ppai.urlopen(_patch_req, timeout=30, context=_ctx_ppai) as _r:
            _result = _json_ppai.loads(_r.read())
        _ok("Partner Powered AI enabled on workspace")
    except Exception as exc:
        _warn(f"Could not enable Partner Powered AI: {exc}")
        _warn("Genie Space read-back may fail if it hasn't propagated from account settings.")

    # Note: Genie API GET read-back may remain blocked by Partner Powered AI
    # on fresh AWS workspaces even after the workspace-level setting is enabled.
    # The generate_abac.py PATCH fallback handles this transparently — no
    # warm-up polling needed here.

    # ------------------------------------------------------------------
    # Step 5b: Create the External Location.  The SP now has workspace
    # access and explicit metastore admin rights.
    #
    # Trailing slash on the URL is required so Databricks prefix-matches
    # sub-paths like s3://bucket/prefix/catalog_name as being covered by
    # this location.
    # ------------------------------------------------------------------
    if storage_cred_id:
        ext_loc_url_with_slash = ext_loc_url.rstrip("/") + "/"
        _step(f"Creating External Location: {ext_loc_url_with_slash}")
        # Retry with back-off because IAM trust-policy propagation is eventually
        # consistent.  The first attempt occasionally fails with "Bucket X does not
        # exist" even though the bucket is reachable — this is Databricks signalling
        # that it could not assume the IAM role yet.  A short retry resolves it.
        el_created = False
        for _attempt, _delay in enumerate([0, 30, 60, 90]):
            if _delay:
                _warn(f"  Retrying External Location creation in {_delay} s (attempt {_attempt + 1})…")
                time.sleep(_delay)
            try:
                from databricks.sdk import WorkspaceClient as _WC
                w_new = _WC(host=ws_host, client_id=client_id, client_secret=client_secret)
                el = w_new.external_locations.create(
                    name="test-external-location",
                    url=ext_loc_url_with_slash,
                    credential_name="test-ext-loc-cred",
                    comment="Test External Location — provision_test_env.py",
                )
                _ok(f"External Location created: {el.url}")
                el_created = True
                # The SP (metastore creator) has implicit admin rights on the
                # external location it created — no explicit grant needed.
                break
            except Exception as exc:
                _warn(f"Could not create External Location (attempt {_attempt + 1}): {exc}")
        if not el_created:
            print("ERROR: External Location creation failed after 3 attempts.")
            print("  Catalog creation requires an External Location. Aborting.")
            sys.exit(1)

    # Note: we intentionally do NOT transfer metastore ownership to the admin
    # group here.  The SP (account admin and metastore creator) retains its
    # implicit metastore admin status, which grants CREATE CATALOG and all
    # other UC privileges needed for the integration tests.  Transferring
    # ownership would strip those rights and cause catalog creation to fail.

    # Workspace admin assignment already done in Step 5a above.

    # ------------------------------------------------------------------
    # Step 7: Write auth.auto.tfvars into the isolated envs/test/ directory
    #         so user's real envs/dev/ is never touched.
    # ------------------------------------------------------------------
    _step(f"Writing auth.auto.tfvars into {_display_path(TEST_ENVS_DIR)}/")
    written_envs = []
    for env in ["dev", "bu2", "prod"]:
        env_dir = TEST_ENVS_DIR / env
        env_dir.mkdir(parents=True, exist_ok=True)
        (TEST_ENVS_DIR / env / "data_access").mkdir(parents=True, exist_ok=True)
        content = (
            f'# Generated by provision_test_env.py — DO NOT EDIT MANUALLY.\n'
            f'# Run `python scripts/provision_test_env.py teardown` to clean up.\n'
            f'databricks_account_host   = "{provider.account_host}"\n'
            f'databricks_account_id     = "{account_id}"\n'
            f'databricks_client_id      = "{client_id}"\n'
            f'databricks_client_secret  = "{client_secret}"\n'
            f'databricks_workspace_id   = "{ws_id}"\n'
            f'databricks_workspace_host = "{ws_host}"\n'
            f'# Base storage URL for catalog managed storage (External Location).\n'
            f'# Each catalog gets its own subfolder: {{catalog_storage_base}}/{{catalog_name}}/\n'
            f'catalog_storage_base      = "{ext_loc_url}"\n'
        )
        auth_path = env_dir / "auth.auto.tfvars"
        auth_path.write_text(content)
        written_envs.append(str(auth_path))
        _ok(f"Wrote {auth_path.relative_to(CLOUD_ROOT)}")

    acct_dir = TEST_ENVS_DIR / "account"
    acct_dir.mkdir(parents=True, exist_ok=True)
    (acct_dir / "auth.auto.tfvars").write_text(
        f'# Generated by provision_test_env.py\n'
        f'databricks_account_host  = "{provider.account_host}"\n'
        f'databricks_account_id    = "{account_id}"\n'
        f'databricks_client_id     = "{client_id}"\n'
        f'databricks_client_secret = "{client_secret}"\n'
        f'databricks_workspace_id  = "{ws_id}"\n'
        f'databricks_workspace_host = "{ws_host}"\n'
    )
    _ok(f"Wrote {(acct_dir / 'auth.auto.tfvars').relative_to(CLOUD_ROOT)}")

    state["written_auth_envs"] = written_envs
    state["test_envs_dir"] = str(TEST_ENVS_DIR)
    _save_state(state)
    _ok(f"State saved to {_display_path(STATE_FILE)}")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    _banner("Environment Ready")
    print(f"\n  Workspace      : {ws_host}")
    print(f"  Workspace ID   : {ws_id}")
    print(f"  Metastore      : {ms_name}  ({ms_id})")
    print(f"  External loc   : {ext_loc_url}")
    print(f"  Admin group    : {group_name}  ({group_id})")
    print(f"  Config dir     : envs/test/  (your real envs/dev/ is untouched)")
    print()
    print("  Next steps:")
    print()
    print(f"    python scripts/run_integration_tests.py")
    print()
    print("  To tear down when finished:")
    print("    python scripts/provision_test_env.py teardown")
    print()


# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------

def cmd_teardown(dry_run: bool = False, env_file: Path | None = None) -> None:
    _banner("Tear Down Integration-Test Environment")

    state = _load_state()
    if not state:
        _warn("No provisioned environment found (state file missing).")
        _warn("Nothing to tear down.")
        return

    ws_id  = state["workspace_id"]
    ms_id  = state["metastore_id"]
    ws_name = state["workspace_name"]
    ms_name = state["metastore_name"]
    account_id    = state["account_id"]
    client_id     = state["sp_client_id"]

    print(f"\n  Workspace  : {ws_name} ({ws_id})")
    print(f"  Metastore  : {ms_name} ({ms_id})")
    print(f"  Provisioned: {state.get('provisioned_at', 'unknown')}")

    if dry_run:
        print("\n  [DRY RUN] No resources will be deleted.")
        return

    # Load secrets from env file with os.environ overrides.
    # Using _load_config (not _load_env_file) so that freshly-exported AWS
    # credentials in the shell (e.g. a renewed AWS_SESSION_TOKEN) take
    # priority over stale values stored in the file.  This matters for
    # long test runs where temporary STS tokens can expire before teardown.
    env_file = env_file or DEFAULT_ENV_FILE
    env_cfg  = _load_config(env_file)
    client_secret = (
        os.environ.get("DATABRICKS_CLIENT_SECRET")
        or env_cfg.get("DATABRICKS_CLIENT_SECRET")
        or ""
    )
    if not client_secret:
        _err("DATABRICKS_CLIENT_SECRET not found in env file or environment.")
        _err(f"  Set it in {env_file} or export DATABRICKS_CLIENT_SECRET=...")
        sys.exit(1)

    # Resolve cloud provider (from state or env file) before creating AccountClient
    # so we use the correct account host (AWS vs Azure).
    if not env_cfg.get("CLOUD_PROVIDER") and state.get("cloud_provider"):
        env_cfg["CLOUD_PROVIDER"] = state["cloud_provider"]
    provider = _get_cloud_provider(env_cfg)

    from databricks.sdk import AccountClient

    a = AccountClient(
        host=provider.account_host,
        account_id=account_id,
        client_id=client_id,
        client_secret=client_secret,
    )

    # ------------------------------------------------------------------
    # Step 0: Delete cloud storage resources (IAM role + S3 for AWS,
    # Access Connector + Storage Account for Azure, etc.)
    # ------------------------------------------------------------------
    provider.teardown_storage(env_cfg, state)

    # ------------------------------------------------------------------
    # Step 1: Unassign metastore from workspace (required before deletion)
    # ------------------------------------------------------------------
    _step("Unassigning metastore from workspace")
    try:
        a.metastore_assignments.delete(workspace_id=ws_id, metastore_id=ms_id)
        _ok("Metastore unassigned")
    except Exception as exc:
        _warn(f"Could not unassign metastore (may already be unassigned): {exc}")

    # ------------------------------------------------------------------
    # Step 2: Delete metastore (force=True deletes all catalogs + policies)
    # ------------------------------------------------------------------
    _step(f"Deleting metastore: {ms_name}")
    try:
        a.metastores.delete(metastore_id=ms_id, force=True)
        _ok(f"Metastore deleted (all catalogs, schemas, policies removed)")
    except Exception as exc:
        _warn(f"Could not delete metastore: {exc}")
        _warn("You may need to delete it manually in the Account Console.")

    # ------------------------------------------------------------------
    # Step 3: Delete admin group
    # ------------------------------------------------------------------
    group_id   = state.get("admin_group_id")
    group_name = state.get("admin_group_name", "")
    if group_id:
        _step(f"Deleting admin group: {group_name}")
        try:
            a.groups.delete(id=group_id)
            _ok(f"Admin group deleted ({group_name})")
        except Exception as exc:
            _warn(f"Could not delete admin group: {exc}")
    else:
        _step("No admin group in state — skipping group deletion")

    # ------------------------------------------------------------------
    # Step 4: Delete workspace
    # ------------------------------------------------------------------
    if hasattr(provider, 'teardown_workspace') and callable(getattr(provider, 'teardown_workspace', None)):
        provider.teardown_workspace(env_cfg, state)
    else:
        _step(f"Deleting workspace: {ws_name}")
        try:
            a.workspaces.delete(workspace_id=int(ws_id))
            _ok(f"Workspace deleted")
        except Exception as exc:
            _warn(f"Could not delete workspace: {exc}")
            _warn("You may need to delete it manually in the Account Console.")

    # ------------------------------------------------------------------
    # Step 5: Remove the entire envs/test/ directory
    # ------------------------------------------------------------------
    _step("Removing envs/test/ directory")
    test_envs = Path(state.get("test_envs_dir", str(TEST_ENVS_DIR)))
    if test_envs.exists():
        import shutil as _shutil
        _shutil.rmtree(test_envs)
        _ok(f"Removed {_display_path(test_envs)}")

    # ------------------------------------------------------------------
    # Step 6: Clear state
    # ------------------------------------------------------------------
    _clear_state()
    _ok("State file cleared")

    _banner("Teardown Complete")
    print("\n  Cloud resources, workspace, metastore, and admin group have been deleted.")
    print("  Run `provision` to create a fresh environment for the next test run.")
    print()


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def cmd_status() -> None:
    _banner("Integration-Test Environment Status")
    state = _load_state()
    if not state:
        print("\n  No provisioned environment (state file not found).")
        print(f"\n  Run `python scripts/provision_test_env.py provision` to create one.")
        return

    print(f"\n  Run ID       : {state.get('run_id')}")
    print(f"  Provisioned  : {state.get('provisioned_at', 'unknown')}")
    print()
    print(f"  Workspace")
    print(f"    Name       : {state.get('workspace_name')}")
    print(f"    ID         : {state.get('workspace_id')}")
    print(f"    Host       : {state.get('workspace_host')}")
    print()
    print(f"  Metastore")
    print(f"    Name       : {state.get('metastore_name')}")
    print(f"    ID         : {state.get('metastore_id')}")
    print(f"    Storage    : {state.get('metastore_storage')}")
    print()
    print(f"  Admin group  : {state.get('admin_group_name')}  ({state.get('admin_group_id')})")
    print(f"  SP (member)  : {state.get('sp_client_id')}")
    print(f"  Config dir   : {state.get('test_envs_dir', 'envs/test/')}")
    print()
    print("  Run the tests:")
    print(f"    python scripts/run_integration_tests.py")
    print()
    print("  Tear down:")
    print("    python scripts/provision_test_env.py teardown")
    print()


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Provision / tear down a fresh Databricks integration-test environment.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "command",
        choices=["provision", "teardown", "status"],
        help="Action to perform",
    )
    parser.add_argument(
        "--env-file",
        default=str(DEFAULT_ENV_FILE),
        metavar="PATH",
        help=f"Path to account-admin credentials env file (default: {DEFAULT_ENV_FILE})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without creating/deleting any resources",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="With provision: overwrite an existing provisioned environment",
    )
    args = parser.parse_args()

    if args.command == "status":
        cmd_status()
        return

    if args.command == "teardown":
        cmd_teardown(dry_run=args.dry_run, env_file=Path(args.env_file))
        return

    # provision
    env_file = Path(args.env_file)
    cfg = _load_config(env_file)
    _validate_config(cfg)
    cmd_provision(cfg, dry_run=args.dry_run, force=args.force, env_file=env_file)


if __name__ == "__main__":
    main()
