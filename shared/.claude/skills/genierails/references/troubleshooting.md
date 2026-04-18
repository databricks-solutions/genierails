# Troubleshooting

## Quick Error Reference

| Error | Fix |
|---|---|
| "already exists" | `make import ENV=<env>` to adopt existing resources |
| "Provider produced inconsistent result" | Re-run `make apply` — tag policy state reconciliation |
| "oauth-m2m: invalid_request" | Check credentials in `auth.auto.tfvars`, test connectivity |
| "Provider type mismatch" | Delete `.terraform/` in the failing env dir, re-run `make apply` |
| "invalid_client: Client authentication failed" | Wrong `client_id` or `client_secret` — verify in Account Console |
| Tag policy visibility delay (apply hangs or fails) | `make wait-tag-policies` then retry, or wait 5 min |
| "No assembled generated/abac.auto.tfvars" | Run `make generate` first (full generation, not delta) |
| Missing `DEST_CATALOG_MAP` | Provide all source→dest catalog mappings |
| "FGAC policy limit exceeded" | Add COUNTRY= overlay to improve priority scoring, or manually reduce policies |
| "Workspace env must not set manage_groups" | Remove `manage_groups` from workspace `env.auto.tfvars` |
| Destroy fails dropping masking functions | Re-run `make destroy` — current code handles SP grant ordering |
| "Account config still contains non-account sections" | Re-run `make promote` to resplit layers |
| Empty LLM output / "groups is missing" | Reduce prompt complexity (fewer tables), re-run `make generate` |

## When to Regenerate

| Situation | Command |
|---|---|
| Minor column additions/removals | `make generate-delta` (keeps existing tuning) |
| Major schema changes | `make generate` (full regen) |
| Adding new overlays | `make generate COUNTRY=... INDUSTRY=...` |
| LLM produced bad output | `make generate` (retry) |
| After manual abac.auto.tfvars edits | `make validate-generated` (just validate, don't regen) |

## Terraform State Recovery

### Import existing resources
```bash
make import ENV=account    # Account groups + tag policies
make import ENV=dev        # Env-scoped governance + workspace
```

### Manual state operations (advanced)
```bash
# Remove a corrupted resource from state
cd envs/account
../../scripts/terraform_layer.sh account account state-rm 'module.account.databricks_tag_policy.policies["pii_level"]'

# Re-import it
../../scripts/terraform_layer.sh account account import 'module.account.databricks_tag_policy.policies["pii_level"]' "pii_level"
```

### Full state recovery for tag policies
```bash
cd envs/account
python3 -c "import hcl2; d=hcl2.load(open('abac.auto.tfvars')); [print(tp['key']) for tp in d.get('tag_policies',[])]" | \
  while read key; do
    ../../scripts/terraform_layer.sh account account state-rm "module.account.databricks_tag_policy.policies[\"$key\"]" 2>/dev/null || true
    ../../scripts/terraform_layer.sh account account import "module.account.databricks_tag_policy.policies[\"$key\"]" "$key" || true
  done
make apply
```

## Credential Issues

### Test connectivity
```bash
python3 -c "
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
print(f'Connected as: {w.current_user.me().user_name}')
print(f'Workspace: {w.config.host}')
"
```

### Common credential problems
- **Azure missing account_host**: add `databricks_account_host = "https://accounts.azuredatabricks.net"` to auth.auto.tfvars
- **Token expired**: regenerate the OAuth secret in Account Console → Service principals
- **Wrong workspace**: verify `databricks_workspace_host` matches the target workspace URL
- **Insufficient permissions**: SP needs Account Admin + Workspace Admin + Metastore Admin (or just Workspace Admin if `genie_only = true`)
