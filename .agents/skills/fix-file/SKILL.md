---
name: fix-file
description: Fix convention violations found by /review-file
argument-hint: "[path]"
---

Fix convention violations in a file using the report produced by `/review-file`.

## Steps

1. Determine the file path and whether **single-fix mode** is active:
   - If the IDE selection's file path (which may be an absolute path) contains `instructors/file-reviews/`, activate **single-fix mode**. Derive the target file path by extracting the portion after `instructors/file-reviews/` (e.g., `.../instructors/file-reviews/lab/tasks/required/task-1.md` → `lab/tasks/required/task-1.md`). Record the selected text as the anchor to locate the finding in step 7. `$ARGUMENTS` is not required.
   - If the IDE selection contains `**Suggested fix:**` (the selection may be just a few lines from a finding, or even only the suggested fix line), activate **single-fix mode** using the same file-path derivation above. The selected text is enough to locate the enclosing finding. `$ARGUMENTS` is not required.
   - Otherwise, parse `$ARGUMENTS` to get the file path. Accept:
     - Paths under `lab/tasks/` (e.g., `lab/tasks/setup.md`, `lab/tasks/required/task-2.md`)
     - Paths under `wiki/` (e.g., `wiki/web-development.md`)
     - Paths under `contributing/conventions/` (e.g., `contributing/conventions/writing/common.md`)
     - The repository root `AGENTS.md` file
     If the path is missing or does not match one of these patterns, ask the user.
2. Derive the report path: `instructors/file-reviews/<repo-root-path>`, where `<repo-root-path>` is the target file's path from the repository root (e.g., `instructors/file-reviews/lab/tasks/required/task-1.md` for `lab/tasks/required/task-1.md`, `instructors/file-reviews/wiki/web-development.md` for `wiki/web-development.md`). If the report file does not exist, tell the user to run `/review-file <path>` first and stop.
4. Read the report file.
5. Read the target file.
6. Read the convention files referenced in the report header so every fix is grounded in the actual convention text:
   - **For `lab/tasks/` files:**
     - [`contributing/conventions/writing/common.md`](../../../contributing/conventions/writing/common.md)
     - [`contributing/conventions/writing/tasks.md`](../../../contributing/conventions/writing/tasks.md)
   - **For `lab/tasks/setup.md` and `lab/tasks/setup-simple.md` (in addition to the above):**
     - [`contributing/conventions/writing/setup.md`](../../../contributing/conventions/writing/setup.md)
   - **For `wiki/` files:**
     - [`contributing/conventions/writing/common.md`](../../../contributing/conventions/writing/common.md)
     - [`contributing/conventions/writing/wiki.md`](../../../contributing/conventions/writing/wiki.md)
   - **For `contributing/conventions/` files:**
     - [`contributing/conventions/conventions.md`](../../../contributing/conventions/conventions.md)
   - **For `AGENTS.md`:**
     - [`contributing/conventions/agents/agents.md`](../../../contributing/conventions/agents/agents.md)
7. **If single-fix mode is active:** locate the enclosing finding in the report. Search for the anchor text; if the selection is only a sub-line (e.g., the `**Suggested fix:**` continuation), scan upward in the report to find the opening line of the numbered item (the `N. **[Severity]**…` line) that contains it. Apply the fix described in `**Suggested fix:**` — regardless of whether the finding is Conceptual or Convention. Skip only if the finding is a **TODO** (no content to supply), and note it in the summary. Then go directly to step 12.
8. **Conceptual findings** cannot be auto-fixed — they require content decisions that only the author can make. List them all as skipped in the summary.
9. Work through the report **Convention findings** one group at a time. For each violation, apply the minimal edit that resolves it. Use line numbers from the report as a starting guide, but always verify against the current file content (earlier fixes may shift lines).
10. Work through the report **Empty sections**. For each empty section that has no `<!-- TODO ... -->` marker, add `<!-- TODO fill in this section -->` directly below the heading. Empty sections that already contain a `<!-- TODO ... -->` cannot be auto-fixed — skip them and note them in the summary.
11. **TODOs** cannot be auto-fixed — they require content that only the author can supply. List them all as skipped in the summary.
12. **Update the report file.** For each numbered finding in the report, prepend a status marker to the line:
    - `~~` strikethrough for fixed items — wrap the entire line content: `1. ~~**Line 45** — …~~`
    - No change for skipped items — leave as-is.
13. **Update the Summary table.** Recount all remaining (non-strikethrough) findings in the report and update the `| Category | Count |` table under `## Summary`:
    - Decrement the count for each category that had a finding fixed. Convention findings are split by severity (`Convention [High]`, `Convention [Medium]`, `Convention [Low]`), so update the correct severity row.
    - Recalculate the **Total** row if present.
    - Keep the existing category rows and table structure — do not add or remove rows.
    - Rewrite the **Overall** assessment paragraph to reflect the current state (remaining issues only). If no issues remain, write: `**Overall**: No remaining issues.`

## Rules

- The report is the single source of truth for *what* to fix. Do not look for additional violations beyond those listed in the report.
- Each fix must satisfy the convention cited in the report. When in doubt, re-read the convention text.
- Make the smallest change that resolves each violation. Do not rewrite surrounding text, reorder sections, or make stylistic changes unrelated to a reported violation.
- Preserve the author's voice and intent. Rephrase only when required by a convention.
- If a reported violation is ambiguous or cannot be fixed without changing the meaning of the content, skip it and note it in the summary.

## Output

After all fixes are applied, print a summary with three sections:

**Fixed** — list each problem that was fixed, with a one-line description (e.g., "Convention: added blank line before alert on line 42").

**Skipped** — list each problem that was skipped, with the reason (e.g., "Conceptual: section 'Overview' needs rewriting — author decision required").

**Counts** — totals for fixed, skipped, and total problems from the report.
