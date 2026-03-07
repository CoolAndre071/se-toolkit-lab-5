# Review: `lab/tasks/required/task-1.md`

- **Date:** 2026-03-06
- **Convention files used:**
  - `instructors/context/conventions/tasks.md` — task structure, design principles, conceptual review dimensions (D1-D12)
  - `instructors/context/conventions/common.md` — writing conventions (4.1-4.26)

---

## Conceptual findings

### D1. Learning objective clarity

No issues found.

The Purpose (line 9) states a concrete outcome: "Build an ETL pipeline that fetches data from an external API and loads it into the database." The Context (line 13) explains why — the database starts empty and needs to be populated from the Autochecker API. The task content delivers on the stated Purpose.

### D2. Step-by-step completeness

1. **[Medium]** Line 45 — "Fill the template properly" is a vague instruction. "Properly" is ambiguous — the student doesn't know what "properly" means for this issue template. **Suggested fix:** Either specify what to fill in (e.g., "In the description, write a short summary of what you will implement") or link to a wiki section that explains how to fill an issue template.

2. **[Low]** Line 197 — "Start the `Qwen code` coding agent in the terminal inside the project directory." provides no link or instruction on how to start the agent. A `wiki/coding-agents.md` wiki file exists but is not referenced. **Suggested fix:** Link to the relevant wiki section, e.g., `[Start the \`Qwen code\` coding agent](../../../wiki/coding-agents.md#...)`.

### D3. Student navigation

1. **[Low]** Lines 62-64 — Part A ("Explore the API") has three sub-sections (1.2.1-1.2.3) but no mini-ToC after the heading. Part B (line 155) correctly includes `<!-- no toc -->` and a mini-ToC. Convention 4.25 requires both parts to have a mini-ToC. **Suggested fix:** Add `<!-- no toc -->` and a bullet list linking to 1.2.1, 1.2.2, 1.2.3 after the `### 1.2.` heading, before the prose text.

### D4. Checkpoints and feedback loops

1. **[Medium]** Lines 131-135 and 151-153 — Two `> [!NOTE]` alerts are indented inside list items (3-space indent). Per convention 4.9, indented GitHub-flavored alerts do not render correctly. Students will miss important information about `has_more` pagination (line 133) and incremental sync (line 152). **Suggested fix:** Move alerts to the top indentation level (after the numbered list), or replace with bold-text fallback (e.g., **Note:**) inside the list.

### D5. Acceptance criteria alignment

No issues found.

Every criterion (lines 368-375) traces to a specific step or deliverable. No criteria are unmatched, and no deliverables are uncovered.

### D6. Difficulty and progression

No issues found.

The task follows a clear progression: Part A (observe/explore the API) then Part B (implement the pipeline). This matches the "start with observation, not coding" principle.

### D7. Practical usability

1. **[Low]** Line 340 — "Authorize with your `API_KEY`, then run `POST /pipeline/sync` once." is a compound instruction combining two actions. A student who misses the authorization step will get a confusing error. **Suggested fix:** Split into two numbered steps: one for authorization and one for running the endpoint.

### D8. LLM-independence

1. **[Low]** Lines 195-220 — Step 1.3.2 explicitly requires the `Qwen code` coding agent, which is an AI tool. Convention 4.16 says AI-required steps should be "a separate, clearly labeled part so students and reviewers can distinguish AI-required steps from AI-optional ones." The step is within Part B but not marked as AI-required. **Suggested fix:** Add a clear label (e.g., a `> [!NOTE]` or bold text) indicating this step requires AI, or note that students may implement manually as an alternative.

### D9. Git workflow coherence

1. **[Medium]** Lines 37-55 — The template requires Step 1.1 to be "Follow the `Git workflow`" (a one-line reference to the wiki) and Step 1.2 to be "Create a `Lab Task` issue". The file combines both into Step 1.1 ("Start with the usual `Git workflow`") with inline issue creation and branch creation instructions. This deviates from the template structure and is inconsistent with other tasks. **Suggested fix:** Split into two steps per the template — Step 1.1 references the Git workflow wiki, Step 1.2 creates the issue. Renumber subsequent steps.

2. **[Medium]** Lines 344-358 — The "Finish the task" step provides 7 detailed inline steps for PR creation instead of the template's two-step format with links to `git-workflow.md#create-a-pr` and `git-workflow.md#get-a-pr-review`. This duplicates wiki content and creates a maintenance burden. **Suggested fix:** Replace with the template format:
   ```
   1. [Create a PR](../../../wiki/git-workflow.md#create-a-pr) with your changes.
   2. [Get a PR review](../../../wiki/git-workflow.md#get-a-pr-review) and complete the subsequent steps in the `Git workflow`.
   ```

### D10. Conceptual gaps and misconceptions

1. **[Low]** Line 66 — "The API has HTTP Basic Auth" introduces the concept without a link. A `wiki/http-auth.md` wiki file exists. Students unfamiliar with HTTP Basic Auth have no reference to learn more. **Suggested fix:** Link to the wiki: "The API has [`HTTP` Basic Auth](../../../wiki/http-auth.md#...)".

### D11. Controlled AI steps

No issues found.

Step 1.3.2 provides an exact prompt (lines 199-200) and a concrete review checklist (lines 206-212). The runtime tests in steps 1.3.3-1.3.5 serve as verification checkpoints for the AI output.

### D12. Autochecker verifiability

No issues found.

All acceptance criteria (lines 368-375) map to machine-checkable conditions: issue title (repo check), endpoint responses (VM check), idempotency (VM check), PR state (repo check).

---

## Convention findings

### 4.1. Instructions wording

1. **Line 45** — "Fill the template properly." is vague. The word "properly" does not describe a concrete action. Convention requires clear, specific instructions.
2. **Line 340** — "Authorize with your `API_KEY`, then run `POST /pipeline/sync` once." is a compound instruction. Convention 4.1 says: "Never write 'Do A and do B.' Instead, split into two numbered steps."

### 4.2. Terminal commands

Every terminal command must follow the pattern: "To \<intention\>, [run in the \`VS Code Terminal\`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):" with a `terminal` fenced code block.

1. **Lines 49-55** — Missing "To \<intention\>," pattern. Says "run in the \`VS Code Terminal\`:" without the wiki link.
2. **Lines 72-78** — Has the "To" pattern (line 70) but "run in the \`VS Code Terminal\`:" (line 72) is missing the wiki link.
3. **Lines 99-105** — Has the "To" pattern (line 97) but "run in the \`VS Code Terminal\`:" (line 99) is missing the wiki link.
4. **Lines 141-147** — Has the "To" pattern (line 139) but "run in the \`VS Code Terminal\`:" (line 141) is missing the wiki link.
5. **Lines 225-228** — Missing "To \<intention\>," pattern, "run in the \`VS Code Terminal\`" instruction, and wiki link. Says "Deploy your changes locally:" directly before the code block.
6. **Lines 259-261** — Inside troubleshooting block. Missing "To \<intention\>," pattern, "run" instruction, and wiki link. Says "Check the container logs for the error:" before the code block.
7. **Lines 317-319** — Missing "To \<intention\>," pattern, "run" instruction, and wiki link. Says "Push your task branch:" before the code block.
8. **Lines 329-333** — Missing "To \<intention\>," pattern, "run" instruction, and wiki link. Says "On your VM, pull your branch and restart the services:" before the code block.

### 4.3. Command Palette commands

Not applicable — no Command Palette commands in this file.

### 4.4. Options vs steps

No issues found.

### 4.5. Ordered lists

No issues found. All ordered lists use sequential numbering (`1. 2. 3.`).

### 4.6. Mini-ToC

See finding under "Task template structure (Section 4.25)" below.

### 4.7. Table of contents

No issues found. All ToC entries (lines 17-33) match the actual headings in the document.

### 4.8. Links and cross-references

Convention: "Link to wiki sections whenever a concept or tool is mentioned for the first time in a section."

1. **Line 230** — `Swagger UI` first mention in section 1.3.3, not linked to `wiki/swagger.md`.
2. **Line 273** — `Swagger UI` first mention in section 1.3.4, not linked.
3. **Line 289** — `Swagger UI` first mention in section 1.3.5, not linked.
4. **Line 336** — `Swagger UI` first mention in section 1.3.7, not linked.

### 4.9. Notes, tips, warnings

Convention: "Do not indent alerts."

1. **Lines 131-135** — `> [!NOTE]` alert is indented (3 spaces) inside a list item in section 1.2.2.
2. **Lines 151-153** — `> [!NOTE]` alert is indented (3 spaces) inside a list item in section 1.2.3.

### 4.10. Images

Not applicable — no images in this file.

### 4.11. Collapsible hints and solutions

No issues found. The troubleshooting block (lines 249-269) uses correct `<details>` format.

### 4.12. Commit message format

No issues found. Line 312 uses `text` fenced code block with conventional commit format.

### 4.13. Diagrams

Not applicable.

### 4.14. `<!-- TODO -->` comments

Not applicable — no TODO comments found (see TODOs section below).

### 4.15. `<!-- no toc -->` comments

See finding under "Task template structure (Section 4.25)" below.

### 4.16. Code snippets in explanations

Not applicable.

### 4.17. Heading levels in section titles

Not applicable.

### 4.18. Inline formatting of technical terms

Convention: "Wrap names of tools, languages, formats, and protocols in backticks: `VS Code`, `Git`, `Docker`, `Python`, `SQL`, `JSON`, `CSV`, `SSH`, `WSL`."

1. **Line 39** — "a github issue" — "github" should be `` `GitHub` ``.
2. **Line 82** — "a JSON array" — "JSON" should be `` `JSON` ``.
3. **Line 107** — "a JSON object" — "JSON" should be `` `JSON` ``.
4. **Line 239** — "a JSON body" — "JSON" should be `` `JSON` ``.
5. **Line 369** — "a JSON body" — "JSON" should be `` `JSON` ``.

### 4.19. Steps with sub-steps

No issues found.

### 4.20. Placeholders in docs

Convention: "Use placeholders instead of hardcoded environment-specific values (e.g., URLs, ports from `.env`)."

1. **Line 230** — Hardcoded URL `http://localhost:42002/docs`. The port comes from `CADDY_PORT` in `.env.docker.secret`. Should use a placeholder.
2. **Line 336** — Port `42002` hardcoded in `http://<your-vm-ip-address>:42002/docs`. IP uses a placeholder but port does not.

### 4.21. `docker compose up` commands

No issues found. Both `docker compose up` commands (lines 227, 333) include `--build`.

### 4.22. Environment variable references

Convention: "When referencing an environment variable from a `.env.*.secret` file in prose, link it to its section in the wiki and link the file name to indicate the source."

1. **Line 232** — `` `CADDY_PORT` `` and `` `.env.docker.secret` `` not linked to wiki.
2. **Line 234** — `` `API_KEY` `` not linked to wiki.
3. **Line 253** — `` `AUTOCHECKER_EMAIL` ``, `` `AUTOCHECKER_PASSWORD` ``, and `` `.env.docker.secret` `` not linked to wiki.
4. **Line 267** — `` `AUTOCHECKER_API_URL` `` and `` `.env.docker.secret` `` not linked to wiki.
5. **Line 340** — `` `API_KEY` `` not linked to wiki (first mention in section 1.3.7).

### 4.23. Horizontal rules

No issues found. Line 364 uses `---`.

### 4.24. Inline paths

No issues found.

### 4.25. Branch-on-remote references

Not applicable.

### 4.26. Example IP address

Not applicable — the file uses `<your-vm-ip-address>` placeholder.

### Task template structure (tasks.md Section 1)

1. **Lines 37-55** — Step 1.1 should be titled "Follow the \`Git workflow\`" with a single-line body referencing the wiki. Step 1.2 should be "Create a \`Lab Task\` issue". Instead, the file merges issue creation and branch creation into step 1.1 ("Start with the usual \`Git workflow\`"), and uses step 1.2 for "Part A: Explore the API".
2. **Lines 344-358** — "Finish the task" step should contain two steps with links to `git-workflow.md#create-a-pr` and `git-workflow.md#get-a-pr-review`. Instead, it has 7 inline steps duplicating PR creation instructions.

### Multi-part tasks (tasks.md Section 4.25)

1. **Lines 62-64** — Part A is missing the required `<!-- no toc -->` comment and mini-ToC (bullet list linking to sub-sections 1.2.1, 1.2.2, 1.2.3). Part B (line 155) correctly includes both.

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
| Conceptual — High | 0 |
| Conceptual — Medium | 4 |
| Conceptual — Low | 5 |
| Convention violations | 31 |
| TODOs | 0 |
| Empty sections | 0 |
| **Total** | **40** |

**Overall assessment:** The task is well-structured pedagogically — it follows a clear observe-then-implement progression and provides good checkpoints and troubleshooting. The main issues are systematic convention gaps: terminal commands consistently miss the wiki link pattern (8 instances), environment variable references are never linked to the wiki (5 instances), and several `JSON`/`GitHub` mentions lack backtick formatting (5 instances). Structurally, steps 1.1/1.2 and the "Finish the task" step deviate from the task document template. Two indented alerts won't render correctly, which could cause students to miss important pagination and sync information.
