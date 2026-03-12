# Commit conventions

<h2>Table of contents</h2>

- [1. Format](#1-format)
- [2. Types](#2-types)
- [3. Scopes](#3-scopes)
  - [3.1. Scope mapping](#31-scope-mapping)
- [4. Subject line](#4-subject-line)
- [5. Body](#5-body)
- [6. Examples](#6-examples)

This project follows [Conventional Commits](https://www.conventionalcommits.org/).

## 1. Format

```text
type(scope): subject

Optional body — use when additional context helps reviewers understand
why the change was made. Write in imperative mood. Wrap at 72 characters.
If the body covers 2+ points, use a bullet list.
```

## 2. Types

- `feat` — Adding new content or a new feature
- `fix` — Correcting a bug or improving existing behaviour
- `docs` — Documentation-only changes
- `refactor` — Code restructuring with no behaviour change
- `test` — Adding or updating tests
- `chore` — Maintenance tasks (deps, CI, tooling)

## 3. Scopes

Every commit must include a scope. Use the scope that best matches the
area of change:

### 3.1. Scope mapping

- `wiki` — `wiki/` pages
- `instructors` — `instructors/` (internal design notes)
- `docs` — `docs/` (architecture docs)
- `contributing` — `contributing/` (conventions, configuration)
- `readme` — Root `README.md`
- `caddy` — `caddy/` config and reverse-proxy setup
- `lab` — `lab/` (task sheets, setup guide)
- `frontend` — `frontend/`
- `backend` — `backend/`, `pyproject.toml`, backend config
- `tests` — `backend/tests/` (test files and fixtures)
- `vscode` — `.vscode/` settings and extensions
- `git` — `.gitignore`, `.gitmodules`, git config
- `github` — `.github/` (workflows, issue templates, PR templates)
- `agent` — `.agents/`, `AGENTS.md` (skills, settings)
- `nix` — `flake.nix`, `flake.lock`, Nix-related config
- `docker` — `Dockerfile`, `docker-compose.yml`, Docker-related config
- `database` — `.sql` files, migrations, schema
- `markdownlint` — `.markdownlint*` config

## 4. Subject line

- Lowercase, imperative mood, no period at the end
- Under 72 characters
- Example: `feat(wiki): add Docker troubleshooting page`

## 5. Body

- Optional — add one when the subject alone doesn't fully explain the
  change (e.g. non-obvious decisions, side effects, grouped changes)
- Separate from the subject with a blank line
- Write in imperative mood, wrap at 72 characters
- **2+ points &rarr; always use a bullet list** (never multiple
  paragraphs for separate points)

## 6. Examples

```text
feat(docs): add Service section to Docker wiki and link from setup
```

```text
fix(backend): handle missing token in auth middleware

- Return 401 instead of crashing on None
- Add unit test for the empty-header case
```

```text
chore(nix): update flake inputs
```
