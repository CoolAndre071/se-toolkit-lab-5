---
name: fix-file-by-conventions
description: Fix convention violations found by /review-file-by-conventions
argument-hint: "<path>"
---

Fix convention violations in a file using the report produced by `/review-file-by-conventions`.

## Steps

1. Parse `$ARGUMENTS` to get the file path. Accept paths under `lab/tasks/` (e.g., `lab/tasks/setup.md`, `lab/tasks/required/task-2.md`) or `wiki/` (e.g., `wiki/web-development.md`). If the path is missing or does not point to a file under one of these directories, ask the user.
2. Derive the report path: `instructors/file-reviews/<repo-root-path>`, where `<repo-root-path>` is the target file's path from the repository root (e.g., `instructors/file-reviews/lab/tasks/required/task-1.md` for `lab/tasks/required/task-1.md`, `instructors/file-reviews/wiki/web-development.md` for `wiki/web-development.md`). If the report file does not exist, tell the user to run `/review-file-by-conventions <path>` first and stop.
3. Read the report file.
4. Read the target file.
5. Read the convention files referenced in the report header so every fix is grounded in the actual convention text:
   - **For `lab/tasks/` files:**
     - `instructors/context/conventions/common.md`
     - `instructors/context/conventions/tasks.md`
   - **For `wiki/` files:**
     - `instructors/context/conventions/common.md`
     - `instructors/context/conventions/wiki.md`
6. **Conceptual findings** cannot be auto-fixed — they require content decisions that only the author can make. List them all as skipped in the summary.
7. Work through the report **Convention findings** one group at a time. For each violation, apply the minimal edit that resolves it. Use line numbers from the report as a starting guide, but always verify against the current file content (earlier fixes may shift lines).
8. Work through the report **Empty sections**. For each empty section that has no `<!-- TODO ... -->` marker, add `<!-- TODO fill in this section -->` directly below the heading. Empty sections that already contain a `<!-- TODO ... -->` cannot be auto-fixed — skip them and note them in the summary.
9. **TODOs** cannot be auto-fixed — they require content that only the author can supply. List them all as skipped in the summary.
10. **Update the report file.** For each numbered finding in the report, prepend a status marker to the line:
    - `~~` strikethrough for fixed items — wrap the entire line content: `1. ~~**Line 45** — …~~`
    - No change for skipped items — leave as-is.

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
