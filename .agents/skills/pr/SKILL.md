---
name: pr
description: Create a pull request using this project's PR template
disable-model-invocation: true
argument-hint: "[issue-number]"
---

Create a pull request following the project's PR template at `.github/pull_request_template.md`.

## Rules

- Run `git fetch origin main` to update the remote ref, then run `git log origin/main..HEAD` to get the commit messages for all commits between the local current branch and the remote main branch.
- Write a concise PR title summarizing the changes (under 72 characters). Base it on the commit messages.
- Fill in the Summary section with a bullet list of the key changes in the PR, written in imperative voice (e.g. "Rename appendix to wiki"). Derive each bullet point from the commit messages — do not invent or assume changes not described in them.
- If `$ARGUMENTS` contains an issue number, include `- Closes #<issue-number>` in the Summary section; otherwise omit that line entirely.
- Use `gh pr create` with `--title` and `--body` to open the PR.
- Target the `main` branch (`--base main`).
- Do not push or create the PR without showing the user the title and body first and asking for confirmation.
