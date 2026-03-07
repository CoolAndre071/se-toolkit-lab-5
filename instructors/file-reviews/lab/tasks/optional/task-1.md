# Review: `lab/tasks/optional/task-1.md`

- **Date:** 2026-03-06
- **Convention files used:**
  - `instructors/context/conventions/tasks.md` — task structure, design principles, conceptual review dimensions (D1–D12)
  - `instructors/context/conventions/common.md` — writing conventions (4.1–4.26)
- **Wiki pages verified:** `wiki/postgresql.md`, `wiki/sql.md`, `wiki/docker.md`, `wiki/docker-compose.md`, `wiki/docker-compose-yml.md`, `wiki/dotenv-docker-secret.md` exist. No wiki pages for Grafana, React, or Chart.js.

---

## Conceptual findings

### D1. Learning objective clarity

1. **[Medium]** Line 9 — Purpose lists three activities ("Set up Grafana…, connect it to PostgreSQL, and build visualizations") rather than stating a single learning outcome. It reads as a task description, not as what the student will *learn*.
   - **Suggested fix:** Rewrite to focus on the learning outcome, e.g., "Learn how a dedicated dashboarding tool (`Grafana`) can replace custom front-end code for building data visualizations."

### D2. Step-by-step completeness

1. **[Low]** Line 42 — Compound instruction: "uncomment and edit" combines two actions in one step.
   - **Suggested fix:** Split into two numbered steps: one for uncommenting, one for editing values.
2. **[Medium]** Line 88 — "Add at least two panels" gives the student freedom but the examples are labeled "Example ideas," making it unclear whether those specific panels are required or truly optional. Students may build different panels that are harder to verify.
   - **Suggested fix:** Either require the two specific panels shown, or explicitly state "You may use the examples below or choose your own queries."

### D3. Student navigation

No issues found.

### D4. Checkpoints and feedback loops

1. **[Medium]** Lines 50–53 — The `docker compose up` command has no expected output or checkpoint. Students won't know if containers started successfully.
   - **Suggested fix:** Add expected output (e.g., "You should see containers starting") or a verification command (`docker compose ps`).
2. **[Medium]** Line 55 — Opening Grafana in the browser has no checkpoint (e.g., "You should see the Grafana login page").
   - **Suggested fix:** Add a visual confirmation or description of the expected page.
3. **[Medium]** Lines 50–53 — Infrastructure step (`docker compose up`) lacks a collapsible troubleshooting block for common failures (port conflicts, container exit).
   - **Suggested fix:** Add a `<details><summary>Troubleshooting</summary>` block covering port-3000 conflicts and container startup failures.
4. **[Low]** Line 126 — "Save the dashboard" has no checkpoint confirming the save succeeded.
   - **Suggested fix:** Add "You should see a confirmation message" or similar.

### D5. Acceptance criteria alignment

No issues found. All criteria trace back to specific steps.

### D6. Difficulty and progression

No issues found. Appropriate complexity for an optional task.

### D7. Practical usability

1. **[Medium]** Lines 65–83 — The database connection step (step 1.4) is infrastructure-dependent but has no troubleshooting block for connection failures.
   - **Suggested fix:** Add a troubleshooting block for common issues (e.g., "Connection refused" — verify the `postgres` container is running).
2. **[Low]** Lines 94–107, 115–121 — SQL queries are provided without explanation. Students unfamiliar with `SQL` may not understand `CASE WHEN`, `GROUP BY`, or `DATE()`.
   - **Suggested fix:** Add a brief `> [!NOTE]` explaining what each query does conceptually, or link to the `wiki/sql.md` page.

### D8. LLM-independence

No issues found. Task provides step-by-step instructions and example SQL queries.

### D9. Git workflow coherence

1. **[High]** Lines 33–42, 136–138 — The task modifies `docker-compose.yml` (step 1.2, a code change) but does not include a "Follow the `Git workflow`" step and does not end with a PR. Per convention 4.6 and 4.11, tasks that produce code changes must start with the Git workflow and end with PR creation. If the intent is that the change stays on the VM only (no commit), this must be stated explicitly.
   - **Suggested fix:** Either (a) add the Git workflow step and change the ending to PR-based, or (b) clarify that the `docker-compose.yml` change is local to the VM and does not need to be committed, and rename step 1.7 to match a "no code" ending pattern (e.g., "Write a comment and close the issue").
2. **[High]** Missing step — Convention 4.23 (tasks.md) requires every task to have a "Check the task using the autochecker" step as the final step before `## 2. Acceptance criteria`. This step is absent.
   - **Suggested fix:** Add `### 1.8. Check the task using the autochecker` with the standard autochecker instruction before the `---` and `## 2. Acceptance criteria`.

### D10. Conceptual gaps and misconceptions

1. **[Low]** Line 42 — `.env.docker.secret` is mentioned without explaining what it is or why credentials are stored there. A student encountering this for the first time in this task would lack context.
   - **Suggested fix:** Link to `wiki/dotenv-docker-secret.md` and/or add a brief `> [!NOTE]`.
2. **[Low]** Lines 94–107, 115–121 — SQL queries use constructs (`CASE WHEN`, `GROUP BY`, `DATE()`) with no explanation or reference. Students might copy-paste without understanding.
   - **Suggested fix:** Add a note or link to `wiki/sql.md` explaining the query logic.

### D11. Controlled AI steps

Not applicable — no AI-assisted steps in this task.

### D12. Autochecker verifiability

1. **[Medium]** Line 147 — "The issue contains a reflection comment comparing the two dashboard approaches" is open-ended free text. The autochecker cannot verify the quality or content of a reflection paragraph.
   - **Suggested fix:** Replace with a structured deliverable (e.g., a questionnaire with specific questions and constrained answer format) or accept that this criterion requires human review and note it accordingly.
2. **[Medium]** Line 146 — "The dashboard has at least 2 panels with data from `PostgreSQL`" requires verifying Grafana's internal state, which may be complex for the autochecker.
   - **Suggested fix:** Consider whether the autochecker can verify this via the Grafana API, or replace with a verifiable proxy (e.g., a screenshot posted to the issue).

---

## Convention findings

### 4.1. Instructions wording

1. **Line 42:** Compound instruction — "uncomment and edit the `GF_SECURITY_ADMIN_USER` and `GF_SECURITY_ADMIN_PASSWORD` lines." Convention says "Never write 'Do A and do B.' Instead, split into two numbered steps."

### 4.2. Terminal commands

1. **Line 46:** The terminal command block starts with "On your VM, to start `Grafana`," instead of the required "To <intention>," pattern. Convention requires starting with "To" (not prefixing with "On your VM,").

### 4.3. Command Palette commands

Not applicable — no Command Palette commands in this file.

### 4.4. Options vs steps

Not applicable — no options sections in this file.

### 4.5. Ordered lists

No issues found. All ordered lists use sequential numbering (`1. 2. 3.`).

### 4.6. Mini-ToC

Not applicable.

### 4.7. Table of contents

No issues found. ToC entries match all actual headings.

### 4.8. Links and cross-references

1. **Line 9:** `PostgreSQL` (first mention in Purpose section) not linked to `wiki/postgresql.md`.
2. **Line 42:** `.env.docker.secret` not linked to `wiki/dotenv-docker-secret.md`. Convention 4.8 says "Provide a link to each file that exists in the repo" and convention 4.22 requires linking env files to their wiki pages.
3. **Line 80:** `Docker` (first mention in step 1.4) not linked to `wiki/docker.md`.
4. **Line 81:** `docker-compose.yml` (first mention in step 1.4 section) not linked. It was linked in step 1.2 (line 36) but convention requires linking on first mention per section.

### 4.9. Notes, tips, warnings

1. **Lines 79–81:** The `> [!NOTE]` alert is indented inside a list item (under step 3 of the numbered list). Convention explicitly states: "Do not indent alerts. GitHub-flavored Markdown alerts do not render correctly when indented." Use bold **Note:** as a fallback or restructure to place the alert at the top indentation level.

### 4.10. Images

Not applicable — no images in this file.

### 4.11. Collapsible hints and solutions

Not applicable — no hints or solutions in this file.

### 4.12. Commit message format

Not applicable — no commit messages specified in this file.

### 4.13. Diagrams

Not applicable — no diagrams in this file.

### 4.14. `<!-- TODO -->` comments

No issues found.

### 4.15. `<!-- no toc -->` comments

Not applicable.

### 4.16. Code snippets in explanations

No issues found. SQL code blocks use the `sql` language identifier.

### 4.17. Heading levels in section titles

Not applicable.

### 4.18. Inline formatting of technical terms

1. **Line 9:** `SQL` not wrapped in backticks. Convention lists `SQL` explicitly as requiring backticks.
2. **Line 13:** `React` not wrapped in backticks. It is a tool/library name.
3. **Line 68:** `PostgreSQL` is in bold (`**PostgreSQL**`) instead of backticks. Convention requires backticks for tool names.
4. **Line 80:** `Docker` not wrapped in backticks (inside the NOTE block: "through the Docker network"). Convention lists `Docker` explicitly.
5. **Line 132:** `React` not wrapped in backticks.
6. **Line 132:** `Chart.js` not wrapped in backticks.
7. **Line 133:** `Grafana` not wrapped in backticks.

### 4.19. Steps with sub-steps

Not applicable — no grouped sub-step patterns used.

### 4.20. Placeholders in docs

No issues found. `<your-vm-ip-address>` placeholder is used correctly with a link to its definition.

### 4.21. `docker compose up` commands

No issues found. Line 52 includes the `--build` flag.

### 4.22. Environment variable references

1. **Line 42:** `GF_SECURITY_ADMIN_USER` and `GF_SECURITY_ADMIN_PASSWORD` are referenced from `.env.docker.secret` without linking per the required pattern: `` [`VARIABLE_NAME`](../../../wiki/dotenv-docker-secret.md#variable_name) in [`.env.docker.secret`](../../../wiki/dotenv-docker-secret.md#what-is-envdockersecret) ``.

### 4.23. Horizontal rules

No issues found. Line 140 uses `---`.

### 4.24. Inline paths

No issues found.

### 4.25. Branch-on-remote references

Not applicable.

### 4.26. Example IP address

Not applicable — file uses a placeholder instead of an example IP.

### Section 1. Task document structure (tasks.md)

1. **Missing step:** Convention 4.23 (tasks.md) requires a "Check the task using the autochecker" step as the final step before `## 2. Acceptance criteria`. The file goes from step 1.7 directly to the acceptance criteria.
2. **Lines 136–138:** Step 1.7 is titled "Finish the task" (which per convention is for PR-based endings: "create PR, get review") but only contains "Close the issue." This does not match any of the three defined task ending types in convention 4.11. Either the title should change (for a "no code" ending) or the content should include PR steps (for a "code" ending).

---

## TODOs

No TODOs found.

---

## Empty sections

No empty sections found.

---

## Summary

| Category | High | Medium | Low | Total |
|---|---|---|---|---|
| Conceptual | 2 | 7 | 6 | 15 |
| Convention | — | — | — | 17 |
| TODOs | — | — | — | 0 |
| Empty sections | — | — | — | 0 |
| **Total** | | | | **32** |

**Overall assessment:** The task provides a clear, useful exercise for exploring Grafana as an alternative dashboard tool. The two most critical issues are (1) the missing autochecker step and (2) the unclear Git workflow status — the task modifies `docker-compose.yml` but neither includes the Git workflow nor clarifies that the change is local-only. Beyond those structural problems, the file has several missing backtick wrappings for tool names, an indented alert that won't render correctly, missing wiki links for tools that have wiki pages, and environment variable references that don't follow the linking convention. Infrastructure steps (docker compose, database connection) lack troubleshooting blocks and checkpoints.
