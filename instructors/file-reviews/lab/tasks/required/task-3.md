# Review: `lab/tasks/required/task-3.md`

- **Date:** 2026-03-10
- **Convention files used:**
  - `contributing/conventions/writing/tasks.md` — task structure, design principles, conceptual review dimensions (D1–D12)
  - `contributing/conventions/writing/common.md` — writing conventions (4.1–4.26)

---

## Conceptual findings

### D1. Learning objective clarity

No issues found.

### D2. Step-by-step completeness

1. **[Low]** Line 85: "Open the [coding agent](../../../../../wiki/coding-agents.md#what-is-a-coding-agent) in the `frontend/` directory" — the phrase "in the `frontend/` directory" is ambiguous (working directory vs. target directory). The wiki link points to `#what-is-a-coding-agent` (concept definition) rather than an operational section like `#open-a-chat-with-qwen-code`. Suggest: link to the operational wiki section and clarify the working directory context.

### D3. Student navigation

No issues found.

### D4. Checkpoints and feedback loops

1. **[Low]** Lines 48–72 ([step 1.3](../../../../../lab/tasks/required/task-3.md#13-install-the-chart-library)): No checkpoint after `npm install chart.js react-chartjs-2`. Students have no visual confirmation that the packages were installed successfully. Suggest: add expected terminal output showing packages added (e.g., "You should see output similar to: `added N packages`").
2. **[Low]** Lines 128–165 ([step 1.6](../../../../../lab/tasks/required/task-3.md#16-run-the-type-checker)): No expected output shown for a successful `npm run typecheck`. Students unfamiliar with `TypeScript` may not know what "passing" looks like. Suggest: add a note such as "If there are no type errors, the command produces no output and exits silently."

### D5. Acceptance criteria alignment

No issues found.

### D6. Difficulty and progression

No issues found.

### D7. Practical usability

No issues found.

### D8. LLM-independence

1. **[Medium]** Lines 106–116: The manual fallback (TIP) only covers a minimal bar chart setup (`react-chartjs-2` imports and `ChartJS.register`). Since the task requires at least two visualizations (line 77: "at least two of the following"), students working without AI lack guidance for a second chart type (line chart or table). Suggest: expand the TIP with a minimal example for at least one additional visualization type, or add a direct link to `Chart.js` documentation for line charts.

### D9. Git workflow coherence

No issues found.

### D10. Conceptual gaps and misconceptions

No issues found.

### D11. Controlled AI steps

1. **[Low]** Lines 118–127 ([step 1.5](../../../../../lab/tasks/required/task-3.md#15-add-navigation)): The AI path ("You can use an AI agent") has no specific prompt template, unlike step 1.4 which provides an exact prompt. The three sub-steps provide some constraint but less control than a prompt template. Suggest: add a prompt template for the AI path, similar to step 1.4.

### D12. Autochecker verifiability

No issues found.

---

## Convention findings

### Task document template (tasks.md Section 1)

No issues found.

### 4.1. Instructions wording

No issues found.

### 4.2. Terminal commands

1. ~~**[High]** Line 191: `npm install && npm run dev` chains two commands with different concerns (dependency installation and dev server startup) using `&&`. Per tasks.md Section 3, commands from different tools or concerns must not be chained — split into separate numbered steps.~~
2. ~~**[Low]** Line 238: `git fetch origin && git checkout <task-branch> && git pull` chains three `Git` commands with `&&`. While they all serve the goal of switching to the remote branch, tasks.md Section 3 discourages chaining — consider splitting into separate numbered steps.~~

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

No issues found.

### 4.9. Notes, tips, warnings

No issues found.

### 4.10. Images

Not applicable.

### 4.11. Collapsible hints and solutions

No issues found.

### 4.12. Commit message format

No issues found.

### 4.13. Diagrams

1. **[High]** Line 20: The Diagram section contains only `<!-- TODO fill in this section -->`. Per convention 1.1 (key rules for task documents), a Mermaid sequence diagram is required whenever the task involves actions across multiple actors or environments. This task spans Developer (local), VM, and GitHub — a diagram is required.

### 4.14. `<!-- TODO -->` comments

Not applicable (TODOs are tracked in the dedicated section below).

### 4.15. `<!-- no toc -->` comments

Not applicable.

### 4.16. Code snippets in explanations

No issues found.

### 4.17. Heading levels in section titles

Not applicable.

### 4.18. Inline formatting of technical terms

1. ~~**[Medium]** Lines 1, 9, 46, 50, 133, 169, 288: "front-end" / "Front-end" should be "frontend" / "Frontend". Convention 4.18 states: "frontend — the front-end service; write as plain text, not inline code (not 'front-end' or 'front end')." Affected locations:~~
   - ~~Line 1: `# Dashboard Front-end` → `# Dashboard Frontend`~~
   - ~~Line 9: "to the front-end" → "to the frontend"~~
   - ~~Line 46: `[Task] Dashboard Front-end` → `[Task] Dashboard Frontend`~~
   - ~~Line 50: "the front-end directory" → "the frontend directory"~~
   - ~~Line 133: "the front-end directory" → "the frontend directory"~~
   - ~~Line 169: "the front-end directory" → "the frontend directory"~~
   - ~~Line 288: "The front-end renders" → "The frontend renders"~~
2. ~~**[Medium]** Line 181: "back-end API" should be "backend API". Convention 4.18 states: "backend — the back-end service; write as plain text, not inline code (not 'back-end' or 'back end')."~~

### 4.19. Steps with sub-steps

1. **[Low]** Lines 122, 177: "Complete these steps:" — common.md 4.19 specifies "Complete the following steps:" while tasks.md Section 3 specifies "Complete these steps:". The task follows tasks.md but diverges from common.md.

### 4.20. Placeholders in docs

No issues found.

### 4.21. `docker compose up` commands

No issues found.

### 4.22. Environment variable references

No issues found.

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

1. **Line 20:** `<!-- TODO fill in this section -->` — Diagram section is incomplete.

---

## Empty sections

1. **Line 18:** `<h4>Diagram</h4>` — contains only a TODO comment; no real content between this heading and the next (`<h4>Table of contents</h4>`).

---

## Summary

| Category | Count |
| --- | --- |
| Conceptual [High] | 0 |
| Conceptual [Medium] | 1 |
| Conceptual [Low] | 4 |
| Convention [High] | 1 |
| Convention [Medium] | 0 |
| Convention [Low] | 1 |
| TODOs | 1 |
| Empty sections | 1 |
| **Total** | **9** |

**Overall:** The chained commands and "front-end"/"back-end" naming issues have been fixed. Remaining issues: (1) the missing Mermaid sequence diagram (Convention [High]), required since the task spans local, VM, and GitHub environments; (2) the "Complete these steps:" vs "Complete the following steps:" wording ambiguity (Convention [Low]); (3) five conceptual findings including an incomplete manual fallback path (D8), missing checkpoints (D4), and missing prompt template for the navigation AI step (D11); (4) one TODO and one empty section (Diagram).
