# Security Guide

## Secret Handling Rules

- Never commit `.env`, cookie exports, API keys, tokens, or DB passwords.
- Use `.env.example` as the only committed env template.
- Keep production secrets in local env files, systemd env files, or a secret manager.

## Secret Scanning

This repo includes `.pre-commit-config.yaml` with `gitleaks` and `detect-private-key` hooks.

Run manually:

```bash
pre-commit run --all-files
```

## If Credentials Were Committed in History

1. Rotate every exposed credential immediately:
- Telegram bot token (`YT_BOT_TOKEN`)
- DB user/password in `STATE_DB_DSN`
- AI provider keys (`OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `HF_TOKEN`)
- Any exported YouTube cookies

2. Remove secrets from git history and force-push cleaned history:

```bash
# Example using git-filter-repo (run from repo root)
git filter-repo --path cookies.txt --invert-paths
```

3. Invalidate existing local clones and re-clone after history rewrite.
