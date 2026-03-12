# Review: `lab/tasks/required/task-2.md`

- **Date:** 2026-03-10
- **Convention files used:**
  - `contributing/conventions/writing/tasks.md` — task structure, design principles, conceptual review dimensions (D1–D12)
  - `contributing/conventions/writing/common.md` — writing conventions (4.1–4.26)

---

## Conceptual findings

### D1. Learning objective clarity

No issues found.

### D2. Step-by-step completeness

1. **[Medium]** Lines 260–268 ([step 1.8, sub-step 1](../../../../../lab/tasks/required/task-2.md#18-deploy-and-verify)): The code block contains three commands from different tools — `cd` (shell navigation), `git fetch && git checkout && git pull` (version control), and `docker compose up` (container management) — grouped under a single compound intention "To pull your branch and restart the services on your VM." Per task convention Section 3 and common convention 4.1, commands from different tools must be separate numbered steps, each with its own "To..." intention and code block.
   **Suggested fix:** Split into separate numbered steps: (1) navigate to the project directory, (2) pull the branch, (3) restart Docker services. Each step gets its own "To..." intention, wiki link, and code block.

2. **[Low]** Lines 260–268 ([step 1.8, sub-step 1](../../../../../lab/tasks/required/task-2.md#18-deploy-and-verify)): The step runs commands "on your VM" via the `VS Code Terminal` but does not instruct the student to SSH into the VM first, nor reference a prior step or wiki section for connecting. This prerequisite may be covered in earlier tasks but is not referenced here.
   **Suggested fix:** Add an SSH connection step before the deploy commands, or add a cross-task reference to the step where SSH was established (e.g., `[Connect to your VM](./task-1.md#...)`).

### D3. Student navigation

No issues found.

### D4. Checkpoints and feedback loops

1. **[Medium]** Lines 295–301 ([step 1.8, sub-step 8](../../../../../lab/tasks/required/task-2.md#18-deploy-and-verify)): The step runs end-to-end tests (`uv run poe test-e2e`) but shows no expected output or checkpoint. Students cannot verify whether the test run succeeded or how many tests should pass.
   **Suggested fix:** Add expected output after the command showing the expected number of passing tests, similar to the unit test checkpoint in [step 1.6](../../../../../lab/tasks/required/task-2.md#16-run-the-tests-all-should-pass).

2. **[Medium]** Lines 260–268 ([step 1.8, sub-step 1](../../../../../lab/tasks/required/task-2.md#18-deploy-and-verify)): The Docker deployment command (`docker compose ... up --build -d`) has no checkpoint. Students cannot verify whether the containers started correctly before proceeding to Swagger UI.
   **Suggested fix:** Add a checkpoint after the command, e.g., "You should see the containers starting. Run `docker compose ps` to verify all services are running."

3. **[Medium]** Lines 260–268 ([step 1.8, sub-step 1](../../../../../lab/tasks/required/task-2.md#18-deploy-and-verify)): Infrastructure step (Docker on VM) has no troubleshooting block. Convention 4.19 in tasks.md requires collapsible troubleshooting for environment-dependent steps.
   **Suggested fix:** Add a `<details><summary><b>Troubleshooting (click to open)</b></summary>` block covering common VM deployment failures (port conflicts, stale containers, missing `.env.docker.secret`).

### D5. Acceptance criteria alignment

1. **[Low]** Lines 295–301, 316–323 ([step 1.8](../../../../../lab/tasks/required/task-2.md#18-deploy-and-verify) and [Acceptance criteria](../../../../../lab/tasks/required/task-2.md#2-acceptance-criteria)): Step 1.8 runs end-to-end tests but no acceptance criterion covers e2e test results. If e2e tests are expected to pass, add a criterion; if they are optional verification, clarify this in the step text.
   **Suggested fix:** Either add `- [ ] \`uv run poe test-e2e\` passes all tests.` to the acceptance criteria, or add a note in step 1.8 clarifying that e2e tests are a verification step only.

### D6. Difficulty and progression

No issues found.

### D7. Practical usability

1. **[Medium]** Lines 260–268 ([step 1.8, sub-step 1](../../../../../lab/tasks/required/task-2.md#18-deploy-and-verify)): The deploy step could silently fail (`docker compose up -d` returns immediately) with no checkpoint or troubleshooting guidance. Students may proceed to Swagger UI and encounter confusing errors without understanding the root cause.
   **Suggested fix:** See D4 findings 2–3.

### D8. LLM-independence

No issues found.

### D9. Git workflow coherence

No issues found.

### D10. Conceptual gaps and misconceptions

1. **[Low]** Line 143 ([step 1.5.1, query logic item 4](../../../../../lab/tasks/required/task-2.md#151-scores-histogram)): The `CASE WHEN` SQL syntax is referenced without explanation or link. Students unfamiliar with SQL case expressions may not know how to write them.
   **Suggested fix:** Add a brief NOTE or link to SQL `CASE WHEN` documentation.

### D11. Controlled AI steps

1. **[Low]** Lines 114–119 ([step 1.5, TIP block](../../../../../lab/tasks/required/task-2.md#15-implement-the-endpoints)): The AI tip provides an approximate prompt ("give it a prompt like:") rather than an exact or templated prompt. Convention 4.20 in tasks.md recommends exact prompts or templates with `<placeholders>` for reproducibility. Low severity since AI use is optional.
   **Suggested fix:** Rephrase to "use this prompt:" and present the prompt in a fenced code block.

### D12. Autochecker verifiability

No issues found.

---

## Convention findings

### 4.1. Instructions wording

1. ~~**[Medium]** Line 261: "To pull your branch and restart the services on your VM," is a compound instruction combining two distinct actions with "and." Convention 4.1: "Never write 'Do A and do B.' Instead, split into two numbered steps."~~

### 4.2. Terminal commands

1. ~~**[Medium]** Lines 264–268: Three commands from different tools (`cd`, `git`, `docker compose`) appear in a single code block. Convention 4.2 requires each terminal command to have its own "To..." intention pattern with a separate code block. This also violates task convention Section 3, which requires commands from different tools to be in separate numbered steps.~~

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

1. ~~**[Low]** Line 272: Placeholder `<your-vm-ip-address>` first appears in section 1.8 without a link. It is linked on line 292 (`[<your-vm-ip-address>](../../../wiki/vm.md#your-vm-ip-address)`), but convention 4.8 requires linking concepts at their first mention in a section.~~

### 4.9. Notes, tips, warnings

No issues found.

### 4.10. Images

Not applicable.

### 4.11. Collapsible hints and solutions

No issues found.

### 4.12. Commit message format

No issues found.

### 4.13. Diagrams

No issues found (see Section 1.1 below for the missing diagram content).

### 4.14. `<!-- TODO -->` comments

No issues found (TODOs reported in the TODOs section below).

### 4.15. `<!-- no toc -->` comments

No issues found.

### 4.16. Code snippets in explanations

No issues found.

### 4.17. Heading levels in section titles

No issues found.

### 4.18. Inline formatting of technical terms

No issues found.

### 4.19. Steps with sub-steps

No issues found.

### 4.20. Placeholders in docs

No issues found (see 4.8 above for the unlinked placeholder).

### 4.21. `docker compose up` commands

No issues found. Line 267 includes `--build`.

### 4.22. Environment variable references

No issues found.

### 4.23. Horizontal rules

No issues found.

### 4.24. Inline paths

No issues found.

### 4.25. Branch-on-remote references

Not applicable.

### 4.26. Example IP address

Not applicable (placeholder is used, not an example IP).

### Section 1.1. Key rules — Diagram (tasks.md)

1. **[Medium]** Lines 17–19: The `<h4>Diagram</h4>` section contains only a `<!-- TODO fill in this section -->` comment. The task involves actions across multiple environments (local development, VM deployment, GitHub PR and autochecker), so a Mermaid sequence diagram is required per convention 1.1: "Diagram is required whenever the task involves actions across multiple actors or environments."

### Section 1.1. Key rules — Structure (tasks.md)

No issues found.

### Section 3. Steps with sub-steps (tasks.md)

1. ~~**[Medium]** Lines 264–268: Commands from different tools (shell navigation, git version control, Docker container management) are grouped in one code block under one step. Per Section 3: "Never chain commands from different tools or concerns with `&&` — split them into separate numbered steps." While the three git commands on line 266 share the same tool, the `cd`, `git`, and `docker compose` commands must be separate numbered steps.~~

### Section 4. Task design principles (tasks.md)

No issues found (design issues covered in conceptual review above).

---

## TODOs

1. **Line 19:** `<!-- TODO fill in this section -->` — under the `<h4>Diagram</h4>` heading.

---

## Empty sections

1. **Line 17:** `<h4>Diagram</h4>` — contains only a `<!-- TODO -->` comment, no real content.

---

## Summary

| Category | Count |
|---|---|
| Conceptual [High] | 0 |
| Conceptual [Medium] | 5 |
| Conceptual [Low] | 4 |
| Convention [High] | 0 |
| Convention [Medium] | 1 |
| Convention [Low] | 0 |
| TODOs | 1 |
| Empty sections | 1 |
| **Total** | **12** |

**Overall:** The convention formatting issues in step 1.8 (compound instruction, mixed-tool code block) have been resolved by splitting into separate numbered steps, and the placeholder link was added at first mention. The remaining findings are conceptual (missing checkpoints, troubleshooting block, and e2e acceptance criterion in step 1.8) and the Diagram section which still contains only a TODO — since the task spans local, VM, and GitHub environments, a Mermaid sequence diagram is required per convention. These remaining items require author decisions.
