# Sourced by every script that runs Terraform (scripts/terraform_layer.sh,
# test.sh); shared/tests/test_terraform_layer_ambient_auth.py checks that
# nothing else does.
#
# The roots' providers authenticate only from the env's auth.auto.tfvars. The
# Databricks provider also reads the shell's DATABRICKS_*, ARM_* and
# ~/.databrickscfg: a stray DATABRICKS_TOKEN or ARM_CLIENT_SECRET fails "more
# than one authorization method", DATABRICKS_AUTH_TYPE picks another identity,
# and a DATABRICKS_HOST fills a host the env leaves empty (sending the SP's
# credentials there). genierails_terraform_env removes all of it from this
# shell, so Terraform and the provisioners it runs never see it. Kept:
#   - DATABRICKS_USER_AGENT_EXTRA (not auth);
#   - DATABRICKS_AUTH_TYPE=github-oidc only in a GitHub Actions job
#     (GITHUB_ACTIONS=true), azure-devops-oidc only in an Azure Pipelines job
#     (TF_BUILD=True): the pipeline samples (docs/pipelines.md) write an
#     IDs-only auth file and pick OIDC this way. The job's own OIDC token
#     variables (ACTIONS_ID_TOKEN_REQUEST_*, SYSTEM_*) are kept only there too,
#     so a local shell can't present one;
#   - ARM_*, with "azurerm" as the argument: that backend reads state with them.
genierails_terraform_env() {
  local keep_arm="${1:-}" name github=0 azure_devops=0
  if [ "${GITHUB_ACTIONS:-}" = true ]; then github=1; fi
  case "${TF_BUILD:-}" in [Tt][Rr][Uu][Ee]) azure_devops=1 ;; esac
  for name in ${!DATABRICKS_@} ${!ARM_@}; do
    case "$name" in
      DATABRICKS_USER_AGENT_EXTRA) ;;
      DATABRICKS_AUTH_TYPE)
        case "$DATABRICKS_AUTH_TYPE:$github:$azure_devops" in
          github-oidc:1:*|azure-devops-oidc:*:1) ;;
          *) unset "$name" ;;
        esac
        ;;
      ARM_*) [ "$keep_arm" = azurerm ] || unset "$name" ;;
      *) unset "$name" ;;
    esac
  done
  if [ "$github" != 1 ]; then
    unset ACTIONS_ID_TOKEN_REQUEST_URL ACTIONS_ID_TOKEN_REQUEST_TOKEN
  fi
  if [ "$azure_devops" != 1 ]; then
    unset SYSTEM_ACCESSTOKEN SYSTEM_OIDCREQUESTURI SYSTEM_TEAMFOUNDATIONCOLLECTIONURI \
      SYSTEM_TEAMPROJECTID SYSTEM_PLANID SYSTEM_JOBID SYSTEM_HOSTTYPE
  fi
  export DATABRICKS_CONFIG_FILE=/dev/null
}
