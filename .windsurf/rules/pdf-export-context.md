---
trigger: always_on
---

Use this rule when the current unresolved question involves exporting, reading, or extracting readable output from one PDF or a set of PDFs.

This repository already has a Windsurf skill and workflow for that job:

- `.windsurf/skills/read-pdfs/SKILL.md`
- `.windsurf/workflows/read-pdfs.md`

Those files exist to route work to `.agent/workflows/Export PDFs.md`, which is the source of truth.

Use the skill as the entry point unless the user explicitly names the workflow. Do not invent or follow a separate PDF export process while that guide is available.
