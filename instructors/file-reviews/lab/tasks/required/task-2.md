# Review: `lab/tasks/required/task-2.md`

**Date:** 2026-03-06

**Convention files used:**

- `instructors/context/conventions/tasks.md` — task structure, design principles, conceptual review dimensions (D1–D12)
- `instructors/context/conventions/common.md` — writing conventions (4.1–4.26)

---

## Conceptual findings

### D1. Learning objective clarity

No issues found.

### D2. Step-by-step completeness

1. **[Medium]** Lines 39–43 (Step 1.1). The step combines three distinct concerns — issue title, branch naming, and a rhetorical question — instead of providing a single concrete action per step. "Do you remember the advice from Task 1?" is not an actionable instruction.
   **Suggested fix:** Follow the template: step 1.1 links to the Git workflow page with one sentence; step 1.2 creates the issue with the title. Move branch guidance into the Git workflow link or a NOTE.

2. **[Low]** Line 41. "Base it on the latest version on `main` and name it appropriately." is a compound instruction (two actions joined by "and").
   **Suggested fix:** Split into two separate instructions, or let the Git workflow wiki page handle branch naming.

3. **[Medium]** Line 252. "Open `Swagger UI` at `http://<your-vm-ip-address>:42002/docs`. Authorize with your `API_KEY`." combines two actions in one sentence.
   **Suggested fix:** Split into separate numbered steps: one for opening Swagger UI, one for authorizing.

### D3. Student navigation

1. **[Medium]** Line 37 (Step 1.1 heading). No link to the Git workflow wiki page. A student encountering this step has no way to navigate to the workflow instructions.
   **Suggested fix:** Add the template instruction: `Follow the [`Git workflow`](../../../wiki/git-workflow.md) to complete this task.`

### D4. Checkpoints and feedback loops

1. **[Medium]** Lines 82–86 (Step 1.3). The expected output is numbered as step 3, a separate step from the command (step 2). Convention requires checkpoints to be indented under the action step they verify, not numbered as separate steps.
   **Suggested fix:** Remove the `3.` numbering and indent the "You should see…" block under step 2.

2. **[Medium]** Lines 198–201 (Step 1.5). Same issue — the expected output is numbered as step 2 instead of being indented under the command step (step 1).
   **Suggested fix:** Remove the `2.` numbering and indent the "All 20 tests should pass:" block under step 1.

3. **[Low]** Lines 240–254 (Step 1.7). This is an infrastructure step (Docker deploy) with no troubleshooting block. Students deploying on a VM can hit port conflicts, stale containers, or network issues.
   **Suggested fix:** Add a `<details><summary>Troubleshooting</summary>` block after the deploy command covering common VM deployment failures.

### D5. Acceptance criteria alignment

No issues found.

### D6. Difficulty and progression

No issues found.

### D7. Practical usability

1. **[Low]** Lines 240–254 (Step 1.7). The deploy-and-verify step provides no troubleshooting block for infrastructure failures on the VM (see also D4 finding 3).
   **Suggested fix:** Add a troubleshooting block.

### D8. LLM-independence

No issues found. The task provides detailed query logic for manual implementation (steps 1.4.1–1.4.4), and the AI tip is explicitly optional.

### D9. Git workflow coherence

1. **[High]** Lines 37–43 (Step 1.1). The step does not follow the template. It lacks a link to the Git workflow wiki page, merges issue creation into the same step, and uses informal wording ("Follow the usual `Git workflow`", "Do you remember the advice from Task 1?").
   **Suggested fix:** Restructure to match the template: Step 1.1 = "Follow the `Git workflow`" with a link; Step 1.2 = "Create a `Lab Task` issue" with the title. Renumber subsequent steps.

2. **[Medium]** No separate "Create an issue" step. The template requires step 1.2 to always be "Create an issue" (key rule in Section 1.1 of tasks.md). The issue title is buried inside step 1.1.
   **Suggested fix:** Add a dedicated step 1.2: `### 1.2. Create a \`Lab Task\` issue` with `Title: \`[Task] Analytics Endpoints\``.

### D10. Conceptual gaps and misconceptions

No issues found.

### D11. Controlled AI steps

1. **[Low]** Lines 94–99. The AI tip provides a suggested prompt, but there is no checkpoint for verifying AI-generated output beyond "run the tests." While the tests serve as a strong verification mechanism, a brief note that students should review the generated code (not just check that tests pass) would reinforce good practice.
   **Suggested fix:** Add a sentence like "Review the generated code to make sure you understand the queries before moving on."

### D12. Autochecker verifiability

No issues found.

---

## Convention findings

### 4.1. Instructions wording

1. **[Fixed]** ~~**Line 41.** "Base it on the latest version on `main` and name it appropriately." — compound instruction ("base it… and name it…"). Convention: "Never write 'Do A and do B.' Instead, split into two numbered steps."~~
2. **[Fixed]** ~~**Line 43.** "Do you remember the advice from Task 1?" — rhetorical question, not an actionable instruction. Convention: instructions should be concrete actions.~~
3. **[Fixed]** ~~**Line 252.** "Open `Swagger UI` at `http://<your-vm-ip-address>:42002/docs`. Authorize with your `API_KEY`." — compound instruction (open and authorize).~~

### 4.2. Terminal commands

1. **[Fixed]** ~~**Lines 232–236.** `git push -u origin <task-branch>` — preceded by "Push your task branch:" instead of the required "To <intention>," pattern. Also missing the `[run in the \`VS Code Terminal\`](...)` wiki link.~~
2. **[Fixed]** ~~**Lines 241–248.** `cd se-toolkit-lab-5 && git fetch… && docker compose…` — preceded by "On your VM, pull your branch and restart the services:" instead of "To <intention>," pattern. Missing the wiki link.~~
3. **[Fixed]** ~~**Lines 210–214** (inside troubleshooting). `uv run pytest …::TestScores -v` — preceded by "Run a single test class to focus on one endpoint:" instead of "To <intention>," pattern. Missing the wiki link.~~

### 4.3. Command Palette commands

Not applicable.

### 4.4. Options vs steps

No issues found.

### 4.5. Ordered lists

No issues found.

### 4.6. Mini-ToC

No issues found.

### 4.7. Table of contents

No issues found.

### 4.8. Links and cross-references

1. **[Fixed]** ~~**Line 37.** Step 1.1 heading mentions `Git workflow` but the step body has no link to the Git workflow wiki page. The template requires `Follow the [\`Git workflow\`](../../../wiki/git-workflow.md) to complete this task.`~~
2. **[Fixed]** ~~**Line 252.** `Swagger UI` is backticked but not linked to a wiki section on first mention in this section (if a swagger wiki page exists).~~

### 4.9. Notes, tips, warnings

1. **[Fixed]** ~~**Lines 59–61.** `> [!NOTE]` is indented inside a list item (under step 1 of section 1.2). Convention: "Do not indent alerts. GitHub-flavored Markdown alerts do not render correctly when indented."~~

### 4.10. Images

Not applicable.

### 4.11. Collapsible hints and solutions

No issues found.

### 4.12. Commit message format

No issues found.

### 4.13. Diagrams

Not applicable.

### 4.14. `<!-- TODO -->` comments

No issues found.

### 4.15. `<!-- no toc -->` comments

No issues found.

### 4.16. Code snippets in explanations

No issues found.

### 4.17. Heading levels in section titles

No issues found.

### 4.18. Inline formatting of technical terms

1. **[Fixed]** ~~**Line 9.** "SQL aggregation queries" — `SQL` is a language name and should be backticked: `` `SQL` ``. Convention lists `` `SQL` `` explicitly.~~
2. **[Fixed]** ~~**Line 92.** "an SQL aggregation query" — same issue.~~
3. **[Fixed]** ~~**Line 254.** "a JSON array" — `JSON` is a format name and should be backticked: `` `JSON` ``. Convention lists `` `JSON` `` explicitly.~~
4. **[Fixed]** ~~**Line 271.** "a JSON array of 4 bucket objects" — same issue.~~
5. **[Fixed]** ~~**Line 272.** "a JSON array of task objects" — same issue.~~
6. **[Fixed]** ~~**Line 273.** "a JSON array of date objects" — same issue.~~
7. **[Fixed]** ~~**Line 274.** "a JSON array of group objects" — same issue.~~

### 4.19. Steps with sub-steps

No issues found.

### 4.20. Placeholders in docs

No issues found.

### 4.21. `docker compose up` commands

No issues found. Line 248 includes `--build`.

### 4.22. Environment variable references

1. **[Fixed]** ~~**Line 252.** `API_KEY` is referenced in prose but not linked to its wiki section and source `.env.*.secret` file, as required by the convention.~~

### 4.23. Horizontal rules

No issues found.

### 4.24. Inline paths

No issues found.

### 4.25. Branch-on-remote references

Not applicable.

### 4.26. Example IP address

Not applicable (placeholder is used, not an example IP).

### Section 1. Task document template (tasks.md)

1. **[Fixed]** ~~**Line 37.** Step 1.1 heading says "Follow the usual `Git workflow`" — "usual" is not in the template. Template heading is "Follow the `Git workflow`".~~
2. **[Fixed]** ~~**Lines 37–43.** Step 1.1 body does not match the template. Missing: `Follow the [\`Git workflow\`](../../../wiki/git-workflow.md) to complete this task.` Instead contains issue title, branch guidance, and a rhetorical question.~~
3. **[Fixed]** ~~**Missing step 1.2.** The template requires step 1.2 to be "Create a `Lab Task` issue" (key rule: "Step 1.2 is always 'Create an issue'"). Issue creation is merged into step 1.1. All subsequent step numbers are off by one.~~

### Section 4.18. Step checkpoints (tasks.md)

1. **[Fixed]** ~~**Lines 82–86.** Expected output in step 1.3 is numbered as a separate step (step 3) instead of being indented under the action step (step 2). Convention: "Checkpoints are part of the step, not separate steps."~~
2. **[Fixed]** ~~**Lines 198–201.** Expected output in step 1.5 is numbered as a separate step (step 2) instead of being indented under the action step (step 1). Same violation.~~

---

## TODOs

No TODOs found.

---

## Empty sections

No empty sections found.

---

## Summary

| Category | Count |
|----------|-------|
| Conceptual — High | 1 |
| Conceptual — Medium | 5 |
| Conceptual — Low | 3 |
| Convention violations | 18 (all fixed) |
| TODOs | 0 |
| Empty sections | 0 |
| **Total** | **27** |

**Overall assessment:** The task content is pedagogically solid — the learning objective is clear, the query logic guidance is detailed, and the test-driven approach is well structured. The main structural issue is that step 1.1 deviates significantly from the template by merging the Git workflow and issue creation steps, dropping the Git workflow link, and using informal wording. Several terminal commands are missing the "To <intention>," pattern and wiki links. Checkpoints are numbered as separate steps instead of being indented under the action they verify. Multiple instances of `SQL` and `JSON` are missing backticks per convention 4.18. A `> [!NOTE]` alert is indented inside a list, which breaks GitHub rendering.
