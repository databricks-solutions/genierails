---
name: feedback
description: Turn feedback about GenieRails into a GitHub issue on databricks-solutions/genierails. Use when the user wants to report a bug, request a feature, flag a docs gap, or otherwise give feedback on the project. Triggers on "feedback", "report a bug", "file an issue", "raise an issue", "feature request", "something's broken".
---

# Feedback → GitHub issue

Collect feedback about GenieRails and file it as a well-formed GitHub issue on
[`databricks-solutions/genierails`](https://github.com/databricks-solutions/genierails).

Use this whenever the user wants to report a bug, request a feature, flag a
documentation gap, or otherwise give feedback about the project — including when they
say something went wrong while using GenieRails and you have the context in the current
conversation.

## Prerequisites

Verify the GitHub CLI is installed and authenticated before doing anything else:

```bash
gh auth status
```

If `gh` is missing, tell the user to install it (`brew install gh`) and authenticate
(`gh auth login`). Do not attempt to file the issue any other way.

## Workflow

1. **Gather the feedback.**
   - If the user gave feedback in their message, use it.
   - Otherwise, ask what the feedback is about.
   - Pull relevant context from the current conversation: the command that was run, the
     cloud (aws/azure), error output, the doc or module involved, and the GenieRails
     version/commit if known. Do **not** include secrets, tokens, workspace hostnames,
     account IDs, or customer data — redact them.

2. **Classify it** into one of:
   - `bug` — something is broken or behaves incorrectly
   - `enhancement` — a feature request or improvement
   - `documentation` — a docs gap, error, or unclear instruction
   - `question` — a usage question

3. **Draft the issue** using the template below. Compose a concise, specific title
   (e.g. `bug: column mask UDF fails on nested struct columns`, not `it doesn't work`).

4. **Show the draft to the user and get explicit confirmation** before filing. Filing a
   GitHub issue is public and outward-facing — never create it without a clear yes.
   Offer to let them edit the title, body, or labels.

5. **File the issue** with `gh`:

   ```bash
   gh issue create \
     --repo databricks-solutions/genierails \
     --title "<title>" \
     --label "<label>" \
     --body "$(cat <<'EOF'
   <body>
   EOF
   )"
   ```

   If a label doesn't exist on the repo, `gh` will error — retry without `--label` and
   mention it to the user rather than failing.

6. **Report the result** — return the issue URL that `gh` prints so the user can open it.

## Issue body template

```markdown
### Type
<bug | enhancement | documentation | question>

### Summary
<one-to-two sentence description of the feedback>

### Details
<what happened / what's wanted, in the user's words plus context you gathered>

### Environment
- Cloud: <aws | azure | n/a>
- Command / area: <e.g. `make apply`, quickstart docs, masking module>
- GenieRails version/commit: <if known, else "unknown">

### Expected vs. actual (bugs only)
- Expected: <...>
- Actual: <...>

### Reproduction steps (bugs only)
1. <...>

---
_Filed via the GenieRails `/feedback` skill._
```

## Notes

- Keep the title under ~70 characters and lead with the type prefix (`bug:`,
  `docs:`, `feat:`).
- One issue per invocation. If the user raises several unrelated points, ask whether to
  file them separately.
- This skill files **issues only** — it does not open pull requests or push code.
