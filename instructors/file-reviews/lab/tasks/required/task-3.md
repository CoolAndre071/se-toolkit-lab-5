# Review: `lab/tasks/required/task-3.md`

**Date:** 2026-03-06

**Convention files used:**

- `instructors/context/conventions/tasks.md` — task structure, design principles, conceptual review dimensions (D1–D12)
- `instructors/context/conventions/common.md` — writing conventions (4.1–4.26)

---

## Conceptual findings

### D1. Learning objective clarity

1. **[Low]** Line 9 (Purpose). The Purpose contains two objectives joined with "and": "Add charts to the front-end to visualize the analytics data from Task 2, **and** learn to integrate a chart library into a React application." A single, concrete learning outcome is preferred.
   - **Suggested fix:** Pick one outcome, e.g., "Learn to integrate a chart library into a React application to visualize analytics data."

### D2. Step-by-step completeness

1. **[Medium]** Lines 114–120 (step 1.5). The step says "Update `frontend/src/App.tsx` to include navigation" but provides only vague bullet-point suggestions, not concrete step-by-step instructions. A beginner may not know how to implement this without AI assistance.
   - **Suggested fix:** Provide a concrete code snippet or placeholder template showing the state variable, the navigation buttons, and the conditional rendering. Alternatively, give an exact AI prompt as done in step 1.4.

### D3. Student navigation

No issues found.

### D4. Checkpoints and feedback loops

1. **[Medium]** Lines 44–66 (step 1.3). No checkpoint after installing the chart library. Students have no way to verify the install succeeded.
   - **Suggested fix:** Add expected output after `npm install` (e.g., "You should see `added N packages`") or a verification command like `npm ls chart.js`.

2. **[Medium]** Lines 112–120 (step 1.5). No checkpoint after adding navigation. Students don't verify that the navigation works until step 1.7.
   - **Suggested fix:** Add a brief checkpoint such as "Save the file. You will verify navigation works in step 1.7."

3. **[Low]** Lines 139–141 (step 1.6, sub-step 2). No expected output shown for a successful `npm run typecheck`. Students don't know what "passing" looks like.
   - **Suggested fix:** Show expected output, e.g., a clean exit with no errors.

### D5. Acceptance criteria alignment

No issues found.

### D6. Difficulty and progression

No issues found.

### D7. Practical usability

1. **[Medium]** Lines 117–120 (step 1.5). The manual approach provides only vague bullet points ("Add a state variable", "Add buttons or links", "Render the Items table or the Dashboard component"). A beginner without AI assistance would struggle to implement this.
   - **Suggested fix:** Provide a concrete code snippet or a placeholder template for `App.tsx` with the navigation logic, similar to the TIP in step 1.4.

### D8. LLM-independence

1. **[Medium]** Lines 100–110 (step 1.4 TIP). The manual fallback only shows the minimal `Chart.js` registration and a one-line rendering instruction. It does not cover fetching data from the API, building the `chartData` object, or handling loading/error states — all of which are required by the acceptance criteria. A student who doesn't use AI has no complete path to finish the component.
   - **Suggested fix:** Expand the TIP with a more complete code template, or add a placeholder file in the seed project that students can fill in.

### D9. Git workflow coherence

No issues found.

### D10. Conceptual gaps and misconceptions

No issues found.

### D11. Controlled AI steps

1. **[Low]** Line 80 (step 1.4, sub-step 2). The prompt is introduced with "Give it a prompt **like**:", making it a suggestion rather than an exact prompt. Per convention 4.20 (Controlled task environment), the prompt should be exact or templated with `<placeholders>` so the AI interaction is reproducible.
   - **Suggested fix:** Change "Give it a prompt like:" to "Give it this prompt:" or "Use this prompt:" to make it exact.

### D12. Autochecker verifiability

No issues found.

---

## Convention findings

### 4.1. Instructions wording

1. ~~**Line 215:** Compound instruction — "pull your branch and restart the services" joins two distinct actions with "and". Should be split or use "Complete these steps:".~~
2. ~~**Line 229:** Compound instruction — "Connect with your API key and verify the Dashboard page shows charts" joins two actions with "and". Should be two separate steps.~~

### 4.2. Terminal commands

1. ~~**Lines 62–66:** Terminal command `cd ..` missing the "To..." intention pattern and the `[run in the `VS Code Terminal`]` wiki link. Currently reads "Go back to the project root:".~~
2. ~~**Lines 153–157:** Same violation as above — `cd ..` without "To..." pattern or wiki link.~~
3. ~~**Lines 205–209:** Terminal command `git push -u origin <task-branch>` missing the "To..." intention pattern and wiki link. Currently reads "Push your task branch:".~~
4. ~~**Lines 215–221:** Three separate commands (`cd`, `git fetch && checkout && pull`, `docker compose up`) in a single code block, all missing the "To..." intention pattern and wiki link. Each command should have its own "To..." block.~~

### 4.3. Command Palette commands

Not applicable.

### 4.4. Options vs steps

No issues found.

### 4.5. Ordered lists

No issues found.

### 4.6. Mini-ToC

Not applicable.

### 4.7. Table of contents

No issues found.

### 4.8. Links and cross-references

1. **Line 38:** Links to `../../../wiki/git-workflow.md` without a section anchor. Convention says "Don't link to the top-level heading of a file. Link to a specific subsection instead." (Note: the task template in `tasks.md` section 1 uses this same pattern, so the template itself may need updating.)

### 4.9. Notes, tips, warnings

1. ~~**Lines 149–151:** `> [!TIP]` alert is indented inside list item 3 (step 1.6). Convention states "Do not indent alerts. GitHub-flavored Markdown alerts do not render correctly when indented (e.g., inside a list item)."~~
2. ~~**Lines 191–193:** `> [!NOTE]` alert is indented inside list item 6 (step 1.7). Same violation.~~

### 4.10. Images

Not applicable.

### 4.11. Collapsible hints and solutions

No issues found.

### 4.12. Commit message format

No issues found.

### 4.13. Diagrams

Not applicable.

### 4.14. `<!-- TODO -->` comments

Not applicable.

### 4.15. `<!-- no toc -->` comments

Not applicable.

### 4.16. Code snippets in explanations

No issues found.

### 4.17. Heading levels in section titles

Not applicable.

### 4.18. Inline formatting of technical terms

1. ~~**Line 9:** `React` is not backticked. Should be `` `React` ``.~~
2. ~~**Line 98:** `TypeScript` is not backticked. Should be `` `TypeScript` ``.~~

### 4.19. Steps with sub-steps

No issues found.

### 4.20. Placeholders in docs

No issues found.

### 4.21. `docker compose up` commands

No issues found.

### 4.22. Environment variable references

1. ~~**Line 175:** `CADDY_PORT` in `.env.docker.secret` is referenced in prose but neither the variable nor the file is linked to its wiki section. Should use the format: `` [`CADDY_PORT`](../../../wiki/dotenv-docker-secret.md#caddy_port) in [`.env.docker.secret`](../../../wiki/dotenv-docker-secret.md#what-is-envdockersecret) ``.~~
2. ~~**Line 227:** Same violation — `CADDY_PORT` in `.env.docker.secret` not linked.~~

### 4.23. Horizontal rules

No issues found.

### 4.24. Inline paths

No issues found.

### 4.25. Branch-on-remote references

Not applicable.

### 4.26. Example IP address

Not applicable (uses `<your-vm-ip-address>` placeholder, not a hardcoded example).

---

## TODOs

No TODOs found.

---

## Empty sections

No empty sections found.

---

## Summary

| Category                      | Count |
|-------------------------------|-------|
| Conceptual — High             | 0     |
| Conceptual — Medium           | 5     |
| Conceptual — Low              | 3     |
| Convention violations         | 13    |
| TODOs                         | 0     |
| Empty sections                | 0     |
| **Total**                     | **21**|

**Overall assessment:** The task follows the template structure correctly and covers a coherent learning objective. The main issues are: (1) several terminal commands lack the required "To..." intention pattern and wiki link (4 instances), (2) two indented alerts that won't render correctly on GitHub, (3) step 1.5 (Add navigation) is underspecified for students who don't use AI, and (4) the manual fallback in step 1.4 is too minimal to satisfy LLM-independence. The compound instructions at lines 215 and 229 should be split per convention 4.1.
