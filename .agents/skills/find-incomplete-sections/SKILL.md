---
name: find-incomplete-sections
description: Find empty sections and sections containing TODO markers in task and wiki files
argument-hint: "[path]"
---

Find all incomplete sections in markdown files — either empty headings (no content below them) or sections that contain only a `<!-- TODO ... -->` comment.

## Rules

- Search in `$ARGUMENTS` if provided; otherwise search in both `lab/tasks/` and `wiki/`.
- Only scan `.md` files.
- A section is **empty** if no non-blank, non-heading lines appear between its heading and the next heading (or EOF).
- A section is **TODO-only** if the only non-blank, non-heading lines between its heading and the next heading (or EOF) are `<!-- TODO ... -->` comments.
- Use a Bash script to detect both cases.
- Output results in the format `file.md:line: ## Section Name — (empty|TODO: <comment text>)`, one per line.
- Group results by file.
- After the list, print a summary: total count (split by empty vs TODO), and which files are most affected.
