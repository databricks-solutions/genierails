# Bank Demo Quickstart

Three pre-built demos showcase GenieRails with realistic banking scenarios:

| Demo | Overlay | Region | Key Features |
|---|---|---|---|
| `aus_bank_demo` | COUNTRY=ANZ, INDUSTRY=financial_services | Australia | TFN, Medicare, BSB masking + PCI + AML row filters |
| `india_bank_demo` | COUNTRY=IN, INDUSTRY=financial_services | India | Aadhaar, PAN, GSTIN, UPI masking + AML compliance |
| `asean_bank_demo` | COUNTRY=SEA, INDUSTRY=financial_services | SE Asia | NRIC, MyKad, NIK, Thai ID + multi-currency banking |

## Prerequisites

1. **Account admin credentials**: `shared/scripts/account-admin.<cloud>.env` must exist with:
   - `DATABRICKS_ACCOUNT_ID`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`
   - For AWS: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (or active AWS SSO session)
   - For Azure: `AZURE_SUBSCRIPTION_ID`, `AZURE_RESOURCE_GROUP`, `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`

2. **Cloud CLI auth**:
   - AWS: `aws sts get-caller-identity` must succeed
   - Azure: `az account show` must succeed

## Run a Demo

### Step 1: Provision

```bash
python shared/examples/<demo>/setup_demo.py provision \
  --env-file shared/scripts/account-admin.<cloud>.env
```

Replace `<demo>` with `aus_bank_demo`, `india_bank_demo`, or `asean_bank_demo`.
Replace `<cloud>` with `aws` or `azure`.

This creates:
- A fresh Databricks workspace
- A Unity Catalog metastore
- Test catalogs with synthetic banking data (customers, accounts, transactions, credit cards)
- Cloud storage resources (S3 bucket + IAM role, or Azure Storage + Access Connector)

Provisioning takes 10-15 minutes.

### Step 2: Generate + Apply

After provisioning completes, the script outputs instructions. Typically:
```bash
cd <cloud>
make generate ENV=dev COUNTRY=<overlay> INDUSTRY=financial_services
make validate-generated
make apply
```

### Step 3: Verify

Open the Genie Space in the workspace and test:
- Ask a question about the data
- Verify masked columns show redacted values
- Check that row filters restrict AML-flagged transactions

### Step 4: Teardown

```bash
python shared/examples/<demo>/setup_demo.py teardown \
  --env-file shared/scripts/account-admin.<cloud>.env
```

This deletes the workspace, metastore, and all cloud storage resources.

## Running All Demos (Parallel CI)

For automated testing of all scenarios:
```bash
python shared/scripts/run_parallel_tests.py \
  --env-file shared/scripts/account-admin.<cloud>.env \
  --scenarios aus-bank-demo,india-bank-demo,asean-bank-demo \
  --no-fail-fast
```
