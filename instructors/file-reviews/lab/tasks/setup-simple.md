# Review report: `lab/tasks/setup-simple.md`

- **Date:** 2026-03-07
- **Convention files used:**
  - `contributing/conventions/writing/tasks.md` (Section 5: D1–D12 conceptual dimensions; Sections 1, 4: task structure and design)
  - `contributing/conventions/writing/common.md` (4.1–4.26)
- **Note:** This is a setup file, not a `task-N.md` file. Task-only conventions (Section 1 template, acceptance criteria format) are skipped per review rules. All `common.md` conventions apply. Conceptual dimensions are evaluated where applicable.

---

## 1. Conceptual findings

### D1. Learning objective clarity

Not applicable — setup files do not require Purpose/Context sections.

### D2. Step-by-step completeness

1. **[High]** Lines 119–136 — **Duplicate instructions for the same file.** The text says "You need to set your credentials in both environment files" (line 120), then tells the student to open `.env.docker.secret` twice and set the same values. This appears to be a copy-paste error — one block should likely reference a different file, or the duplication should be removed. Students will be confused about whether they are editing two different files or the same file twice.
   - **Suggested fix:** Remove the duplicate block (lines 131–136), or clarify which two files are meant and update the second block to reference the correct file.

2. **[Medium]** Line 105 — **Compound instruction.** "Go to `VS Code Terminal`, check that the current directory is `se-toolkit-lab-5`, and install `Python` dependencies:" combines three actions in one step (navigate, verify, install).
   - **Suggested fix:** Split into separate numbered steps: (1) Go to the VS Code Terminal, (2) check the current directory, (3) install dependencies.

3. **[Low]** Lines 226–248 — **Sub-step 2 of section 1.5.2 combines many actions.** Opening nano, editing two credential fields, setting an API key, and saving the file are all inside one numbered step with mixed indentation.
   - **Suggested fix:** Consider splitting into more granular numbered steps or using the "Complete these steps:" sub-step pattern.

### D3. Student navigation

1. **[High]** Line 140 — **Broken internal link.** The link `[step 1.7](#17-set-up-the-autochecker)` points to anchor `#17-set-up-the-autochecker`, but the actual heading is "### 1.7 Set up Qwen Code" (line 315), which generates anchor `#17-set-up-qwen-code`. The link will not resolve.
   - **Suggested fix:** Update the link to `[step 1.7](#17-set-up-qwen-code)` and also update the link text to match ("Set up Qwen Code" instead of "Set up the autochecker").

2. **[Medium]** Line 18 / Line 315 — **Inconsistent heading numbering.** All other headings use a trailing period after the number (e.g., "### 1.1.", "### 1.6.") but "### 1.7 Set up Qwen Code" omits it. The corresponding ToC entry also lacks the period.
   - **Suggested fix:** Change to "### 1.7. Set up Qwen Code" and update the ToC entry.

### D4. Checkpoints and feedback loops

1. **[Low]** Section 1.3 (lines 95–141) — **No checkpoint after cloning and setting up the environment.** The student clones, installs dependencies, and creates env files with no verification step to confirm success.
   - **Suggested fix:** Add a checkpoint after `uv sync --dev` (e.g., expected output showing successful install) and/or after `cp` (e.g., "Verify the file exists").

2. **[Low]** Section 1.5.2 (lines 216–249) — **No checkpoint after environment setup on VM.** The student edits `.env.docker.secret` with no way to verify correctness before starting services.
   - **Suggested fix:** Add a brief verification step (e.g., "Run `cat .env.docker.secret` and verify your email and password are set").

### D5. Acceptance criteria alignment

Not applicable — setup files do not have acceptance criteria.

### D6. Difficulty and progression

No issues found.

### D7. Practical usability

1. **[High]** Lines 119–136 — **Duplicate/contradictory instructions.** Same as D2 finding #1. A student following literally will open the same file twice and set the same values, wasting time and creating confusion about whether a second file exists.

2. **[Medium]** Line 309 — **Typo: "CLTR X" should be "CTRL X".** Students unfamiliar with keyboard shortcuts may not recognize the typo and get stuck trying to save and exit nano.
   - **Suggested fix:** Change `CLTR X` to `CTRL X`.

### D8. LLM-independence

No issues found.

### D9. Git workflow coherence

Not applicable — setup files do not follow the git workflow pattern.

### D10. Conceptual gaps and misconceptions

1. **[Low]** Line 76 — **Russian text without context.** The text "(выше по течению)" may confuse non-Russian-speaking students. If the audience is multilingual, this should be explained or removed.
   - **Suggested fix:** Remove the Russian translation, or add "Russian:" before it for clarity.

### D11. Controlled AI steps

Not applicable — the setup file does not include AI-assisted steps.

### D12. Autochecker verifiability

Not applicable — setup files do not have acceptance criteria.

---

## 2. Convention findings

### 4.1. Instructions wording

1. Line 105 — Compound instruction: "Go to `VS Code Terminal`, check that the current directory is `se-toolkit-lab-5`, and install `Python` dependencies:" combines three actions in one step. Convention: "Never write 'Do A and do B.' Instead, split into two numbered steps."
2. Line 195 — Sentence "e.g. ssh my-vm-user@10.93.1.1" does not end with a period.
3. Line 197 — Sentence "If unable, see [how to connect to vm](../../../../wiki/vm.md#connect-to-the-vm)" does not end with a period.
4. Line 309 — Sentence fragment "`nano .env.docker.secret`, then when done close it with `CLTR X`" does not end with a period.
5. Line 350 — Sentence "read more here if interested: [AI coding agent setup](../../../../wiki/coding-agents.md)" does not end with a period.

### 4.2. Terminal commands

Convention requires every terminal command to use the "To \<intention\>," pattern followed by a `[run in the \`VS Code Terminal\`](../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):` link. The following instances violate this:

1. Lines 38–42 — `cd se-toolkit-lab-4`: uses "run in the VM:" with no "To \<intention\>," pattern and no wiki link.
2. Lines 44–49 — `docker compose ... down -v`: uses "run in the VM:" with no "To \<intention\>," pattern and no wiki link.
3. Lines 64–68 — `cd ~`: uses "Go back to the home directory:" with no "To" pattern and no wiki link.
4. Lines 99–101 — `git clone ...`: no "To \<intention\>," pattern and no wiki link.
5. Lines 107–109 — `uv sync --dev`: embedded in a compound instruction, no "To \<intention\>," pattern and no wiki link.
6. Lines 113–115 — `cp .env.docker.example .env.docker.secret`: no "To \<intention\>," pattern and no wiki link.
7. Lines 148–152 — `docker compose ... up --build`: has "To start the services," but missing wiki link.
8. Lines 168–171 — `docker compose ... down -v` and `docker compose ... up --build` (inside troubleshooting): no wiki link. (Troubleshooting blocks may be exempt, but no explicit exemption exists.)
9. Lines 191–193 — `ssh <vm-user>@<vm-ip>`: no "To \<intention\>," pattern and no wiki link.
10. Lines 204–206 — `git clone ...`: uses "replace \<github-username\> and run in the `VS Code Terminal`:" — no "To \<intention\>," pattern and no wiki link.
11. Lines 212–214 — `cd se-toolkit-lab-5`: uses "run in the `VS Code Terminal`:" — no "To \<intention\>," pattern and no wiki link.
12. Lines 222–223 — `cp .env.docker.example .env.docker.secret`: uses "run in the `VS Code Terminal` connected to your VM:" — no "To \<intention\>," pattern and no wiki link.
13. Lines 230–232 — `nano .env.docker.secret`: no "To \<intention\>," pattern and no wiki link.
14. Lines 255–257 — `docker compose ... up --build -d`: has "To start the services," but missing wiki link.
15. Lines 263–265 — `docker compose ... ps ...`: uses "run in the `VS Code Terminal`:" — no "To \<intention\>," pattern and no wiki link.
16. Lines 325–326 — Code block uses `` ```bash `` instead of `` ```terminal ``.
17. Lines 330–331 — Code block uses `` ```cmd `` instead of `` ```terminal ``.

Note: Line 287 (inside troubleshooting in section 1.5.3) correctly uses the full pattern with wiki link — this is the only correct instance.

### 4.3. Command Palette commands

Not applicable.

### 4.4. Options vs steps

No issues found.

### 4.5. Ordered lists

1. Line 103 — Uses `1.` for the second item in the list (should be `2.`). The list in section 1.3 starts with `1.` (line 97), then restarts with `1.` (line 103) instead of continuing with `2.`.
2. Line 336 — Uses `1.` for the third item in section 1.7 (should be `3.`). The list starts with `1.` (line 319), `2.` (line 320), then restarts with `1.` (line 336).

### 4.6. Mini-ToC

No issues found.

### 4.7. Table of contents

1. Line 18 — ToC entry "1.7 Set up Qwen Code" is missing the trailing period after the number (should be "1.7.").

### 4.8. Links and cross-references

1. Line 140 — Broken link: `#17-set-up-the-autochecker` does not match the heading anchor `#17-set-up-qwen-code`.
2. Line 197 — Link text "vm" is not capitalized. Should be "VM" (convention 4.18 says don't backtick acronyms, but they should be uppercase).

### 4.9. Notes, tips, warnings

1. Line 249 — Uses a plain blockquote (`> It is useful to remember...`) instead of a `> [!NOTE]` or `> [!TIP]` alert for explanatory information.
2. Line 302 — Uses a plain blockquote (`> You set caddy port...`) instead of a `> [!NOTE]` or `> [!TIP]` alert.
3. Lines 308–309 — Uses a plain blockquote (`> You can check both the...`) instead of a `> [!TIP]` alert.
4. Line 334 — Uses `> **Note**:` instead of `> [!NOTE]`.
5. Line 350 — Uses `> **Note**:` instead of `> [!NOTE]`.

### 4.10. Images

Not applicable — no images in this file.

### 4.11. Collapsible hints and solutions

No issues found — troubleshooting blocks use proper `<details>` structure.

### 4.12. Commit message format

Not applicable.

### 4.13. Diagrams

Not applicable.

### 4.14. `<!-- TODO -->` comments

Not applicable — no TODO comments in this file.

### 4.15. `<!-- no toc -->` comments

Not applicable.

### 4.16. Code snippets in explanations

Not applicable.

### 4.17. Heading levels in section titles

Not applicable.

### 4.18. Inline formatting of technical terms

1. Lines 5, 70 — "github" (lowercase, no backticks) should be `` `GitHub` `` in heading "Set up your fork (in github)" and corresponding ToC entry.
2. Line 320 — `` `Qwen code` `` uses lowercase "code", inconsistent with heading "Qwen Code" (line 315) and line 317 `` `Qwen Code` ``.
3. Line 336 — `` `Qwen code` `` again uses lowercase "code" — same inconsistency.

### 4.19. Steps with sub-steps

No issues found.

### 4.20. Placeholders in docs

1. Line 175 — Hardcoded port `42002` in URL `http://localhost:42002/docs`. Since the port comes from `.env.docker.secret`, a placeholder or note should be used per convention.

### 4.21. `docker compose up` commands

No issues found — all `docker compose up` commands include `--build`.

### 4.22. Environment variable references

1. Line 179 — References [`API_KEY`](../../../../wiki/dotenv-docker-secret.md#api_key) from `.env.docker.secret` without wiki links. Convention requires linking to the variable's wiki section and the file's wiki section.
2. Line 306 — References `API_KEY` without wiki links (same violation).

### 4.23. Horizontal rules

1. Line 352 — Uses `----` (4 dashes) instead of `---` (3 dashes).

### 4.24. Inline paths

No issues found.

### 4.25. Branch-on-remote references

Not applicable.

### 4.26. Example IP address

1. Line 195 — Uses `10.93.1.1` as example IP. Convention requires `192.0.2.1`.
2. Line 300 — Uses `10.93.x.x` as example IP. Convention requires `192.0.2.1`.

---

## 3. TODOs

No TODOs found.

---

## 4. Empty sections

No empty sections found.

---

## 5. Summary

| Category | Count |
|---|---|
| Conceptual — High | 3 |
| Conceptual — Medium | 3 |
| Conceptual — Low | 4 |
| Convention violations | 38 |
| TODOs | 0 |
| Empty sections | 0 |
| **Total** | **48** |

**Overall assessment:** The file has significant convention compliance issues, particularly around terminal command formatting (4.2) — almost every command block is missing the required "To \<intention\>," pattern and wiki link. There are also several substantive problems: a duplicate/copy-paste error in the autochecker credentials section (lines 119–136), a broken internal link (line 140), inconsistent heading numbering (missing period on 1.7), ordered list numbering restarts (lines 103, 336), and plain blockquotes used instead of GitHub-flavored alerts. The horizontal rule uses 4 dashes instead of 3, and example IPs use university-specific addresses instead of the standard `192.0.2.1`.
