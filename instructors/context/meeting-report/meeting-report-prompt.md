# Meeting Report Prompt

## General rules

- **Language**: Always write the report in English. Use a neutral, professional tone. Translate quotes and terminology as needed. For domain-specific terms, keep the original alongside the English translation in parentheses (e.g., "sprint review (ревью спринта)") to avoid ambiguity.
- **Output format**: Write the report in Markdown using the heading structure defined below. Write each sentence on its own line so that individual lines can be referenced by line number.
- **Speaker references**: Use speaker labels from the transcript (e.g., Speaker A, Speaker B). If the transcript uses real names instead of labels, replace them with Speaker A/B/C in order of first appearance.
- **No personal information**: Never include real names, email addresses, phone numbers, physical addresses, social media handles, company names, project code names, client names, or any other personally identifiable information (PII) in the report. Redact or replace all such references with generic labels (e.g., "Speaker A," "the client," "the external tool," "Company X"). If a speaker mentions a specific person outside the meeting, refer to them by role (e.g., "the team lead," "the vendor contact").
- **Attendance**: Note if any speaker joined late, left early, or was silent throughout the meeting.
- **Skip noise**: Ignore connection issues, screen sharing problems, audio glitches, crosstalk, filler words, and other non-substantive artifacts. Focus only on meaningful content.
- **Unclear content**: If something in the transcript is inaudible or the meaning is ambiguous, flag it with [unclear] rather than guessing.
- **Do not infer decisions**: Only record decisions that were explicitly stated or clearly agreed upon. Do not infer consensus from silence. Proposals, hypotheticals, and "what if" suggestions that were not concluded belong in Open Questions, not Decisions.
- **Superseded decisions**: If a later discussion in the same meeting overrides an earlier agreement, only include the final version in Decisions. Note the evolution briefly (e.g., "Initially agreed on X, later revised to Y because...").
- **Cross-references**: When a decision leads to an action point, or an open question relates to a decision, reference it (e.g., "See Decision #5"). This applies across all sections.
- **Items as subsections**: Write each distinct item within a section — numbered decisions, open questions, action points, status update items, glossary terms, Q&A pairs, and discussions — as a Markdown subsection with a heading rather than a bullet list entry or blockquote. Write the content beneath the heading as prose paragraphs. This makes each item individually linkable via a Markdown anchor. Never use bold text to label items within a section.
- **Clean headings**: Headings must contain only the item title — no dates, consensus levels, priorities, severity tags, speaker labels, timestamps, or any other metadata. Metadata in headings breaks automatic table-of-contents generation and anchor links. Place all contextual metadata — speaker attribution, timestamp ranges, owners, priority, consensus level, and similar details — at the beginning of the first paragraph beneath the heading, highlighted with **bold**. Never append metadata to the heading or place it on a separate line before the paragraph.
- **Anchor links**: When cross-referencing a specific item in the report, link to its subsection anchor rather than using plain text (e.g., `[Decision #1: Use PostgreSQL](#1-use-postgresql-for-the-primary-data-store)` instead of "See Decision #1").
- **Timestamps**: If the transcript includes timestamps, reference them at key moments so readers can locate the corresponding point in a recording. Always wrap timestamps in square brackets — single points as `[14:32]` and ranges as `[01:10:15–01:12:08]`. Square brackets are reserved for transcript-navigation timestamps only. Deadlines, scheduled dates, and other future times mentioned by speakers should be written as plain text (e.g., "by March 15", "next Monday at 10:00").
- **Direct quotes vs paraphrasing**: Use direct quotes when the speaker's exact wording carries meaning — strong opinions, definitions, commitments, or memorable phrasing. Paraphrase for routine discussion.
- **Depth calibration**: Scale the report's depth proportionally to the meeting's length and substance. A 15-minute standup should not produce the same volume as a 2-hour planning session.
- **Off-topic digressions** (tools, mentoring, industry news, etc.) should still be captured in the relevant section (usually Discussions).
- **Recurring meetings**: If the transcript references a prior meeting or is clearly part of a series, note that context in the Agenda section.

## Instructions

Write a comprehensive meeting report based on the provided transcript.

Go through the transcript **line by line for substantive content** and make sure every point, detail, example, quote, and discussion is captured in the appropriate section. In the Decisions, Q&A, and Discussions sections, do not summarize — expand. Include specific examples, analogies, and arguments that speakers used. If speakers disagreed or evolved their position during the discussion, capture that progression. If speakers outright contradicted each other without resolution, note the disagreement explicitly.

## Sections

Each section below describes what belongs in it and how detailed it should be. Use these exact headings in the report.

### Table of contents

Immediately after the report title, include a table of contents listing every section heading below as a Markdown link. This gives readers a quick overview and lets them jump to the relevant part. Use an HTML heading (`<h2>Table of contents</h2>`) instead of a Markdown `##` heading so the TOC entry does not appear inside its own list. The TOC list itself must be in Markdown format (bullet list with `- [Link](#anchor)` syntax).

### Metadata

A structured block at the top of the report:

- **Date & time**: Meeting date and time in `DD.MM.YYYY, HH:MM - HH:MM` format (from transcript if available, otherwise `DD.MM.YYYY, HH:MM - HH:MM`)
- **Duration**: Approximate length of the meeting
- **Participants**: Number of speakers and their labels (e.g., "4 participants: Speaker A, B, C, D")
- **Recording**: Link to the meeting recording (if provided, otherwise `<link to the recording>`)
- **Transcript**: Link to `transcript-by-speaker.txt` in the `meeting-transcripts/` subdirectory
- **Files discussed**: List of files that were explicitly mentioned, reviewed, or discussed during the meeting (by file name or path as referenced in the transcript). Omit if no specific files were discussed.

### Executive summary

3-5 bullet points covering the most important outcomes of the meeting. This is for readers who will not read the full report. Focus on key decisions, blockers, and deadlines.

### Agenda

Brief summary (2-3 sentences) of what the meeting was about, who participated (by speaker label), and the overall purpose. If this is part of a recurring meeting series, note which iteration or reference prior sessions if mentioned.

### Status updates

What is already done, built, deployed, or available before this meeting. Include specifics: which artifacts exist (documents, prototypes, deployed services, repos), what tools were used or demonstrated during the meeting, and any issues encountered. Write each distinct status item as a subsection heading followed by prose — do not use bold labels in a bullet list.

### Decisions

**This is the largest section.** Number each decision. Every conclusion, agreement, or direction discussed should be its own numbered item — even small ones. Group decisions by topic, not by chronological order. For each decision, include:

- What was decided
- **Consensus level**: State **unanimous**, **majority**, or **contentious** (note dissenters by speaker label) at the start of the paragraph body, not in the heading
- The reasoning or discussion behind it (which speaker said what, what alternatives were considered)
- Specific examples, quotes, or analogies used (translated to English if needed)
- If a decision evolved during the meeting, show the progression
- Cross-references to related action points or open questions

Be exhaustive — capture every distinct decision, no matter how small. Cover all topics raised: naming, data model, scope, task structure, implementation approach, security, testing, deployment, tooling, terminology, ordering, scheduling, and anything else discussed.

**Important**: Only include conclusions that were explicitly agreed upon. Proposals and hypotheticals that were not resolved belong in Open Questions.

#### Example format

### 1. Use PostgreSQL for the primary data store

**Consensus: unanimous.**
Speaker A proposed PostgreSQL, citing its JSON support for flexible schemas. Speaker B initially suggested MongoDB but agreed after Speaker A demonstrated that PostgreSQL's JSONB columns would cover the same use case without adding a second database. Speaker C noted: "We already have operational expertise with Postgres, so this keeps the ops burden low." *(See Action Point #3)*

### Open questions

Things that were raised but not resolved — including proposals, hypotheticals, and "what if" suggestions that did not reach a conclusion. Include context on why they remain open, any preliminary opinions expressed, and any disagreements that prevented resolution. Cross-reference related decisions where applicable.

### Action points

Concrete next steps with owners (by speaker label) and deadlines (if mentioned). Write each action point as a subsection heading followed by a prose paragraph that states the owner, priority (**[blocking]**, **[high]**, **[medium]**, or **[low]**), scheduling details, and any relevant context. If a task was mentioned but no owner was assigned, say so in the prose. Do not place owner or priority on a separate bold-label line before the paragraph. Every action point originates from a decision, discussion, or open question — cross-reference the source item using an anchor link (e.g., "Stems from [Decision #2: Use PostgreSQL](#1-use-postgresql-for-the-primary-data-store)"). If a single decision spawns multiple action points, each one should link back independently.

### Key dates and deadlines

Dates, deadlines, and milestones mentioned during the meeting, even in passing. Write each distinct date as a subsection heading followed by a prose paragraph describing what the date refers to, who mentioned it, and any relevant context. Do not use a bullet list with bold labels.

### Glossary

If the meeting introduced, defined, or clarified any terms, acronyms, or project-specific jargon, include them here. Omit this section if no new terminology was introduced. Write each term as a subsection heading (e.g., `### Sprint Review`) followed by a prose paragraph with the definition. This makes each term individually referenceable via a Markdown anchor.

### Questions and answers

Questions that came up during the meeting and were answered on the spot. Write each Q&A pair as a subsection heading (e.g., `### Q: What mechanism can we use to...`) followed by prose paragraphs. The heading contains only the question — do not use bold `**Q:**` labels, and do not append speaker attribution or timestamps to the heading. State who asked, who answered, and the relevant timestamp range in the opening sentence of the paragraph instead. Include:

- Which speaker asked and which speaker answered
- The full answer with examples and analogies

When a speaker presents slides, gives a demo, or shares a screen for an extended monologue, write it as a `### Demo:` or `### Presentation:` subsection: summarize the content presented, then capture the discussion that followed in full detail.

### Discussions

Extended conversations, off-topic digressions (tools, mentoring experiences, industry news), and other substantive exchanges that are not direct Q&A pairs. Write each distinct discussion as a subsection heading (e.g., `### Discussion: Deployment strategy trade-offs`) followed by prose paragraphs. The heading contains only the topic — do not use bold `**Discussion:**` labels, and do not append speaker attribution or timestamps to the heading. State who participated and the relevant timestamp range in the opening sentence of the paragraph instead.

### Coverage check

After completing the report, review the transcript one more time and verify:

- Every speaker who contributed substantively is referenced at least once
- Every topic transition in the transcript is accounted for in a section
- No decisions were inferred — all are explicitly stated or agreed upon
- Proposals and hypotheticals are in Open Questions, not Decisions

If any gaps are found, go back and fill them in. Do not include this checklist in the final report.
