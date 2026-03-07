---
trigger: always_on
---

Use this rule when the current unresolved question is which `.windsurf/skills/*/SKILL.md` file owns the task.

Read only the skill that owns the current unresolved question. If the task changes, open the next matching skill.

If the user explicitly names a skill, use that skill.

If the user explicitly names a workflow, use the matching workflow directly.

Use this routing first:

- Record one lasting architectural or design decision: use `.windsurf/skills/adr/SKILL.md`.
- Review system shape, resilience, state ownership, dependency risk, or failure spread: use `.windsurf/skills/architecture-review/SKILL.md`.
- Turn accepted behaviour into a proof-ready specification: use `.windsurf/skills/behaviour-spec/SKILL.md`.
- Create or revise one reusable style rule: use `.windsurf/skills/code-style-rule/SKILL.md`.
- Clarify unclear intended behaviour with rules and examples: use `.windsurf/skills/example-mapping/SKILL.md`.
- Plan a behaviour-changing implementation sequence: use `.windsurf/skills/exec-plan/SKILL.md`.
- Diagnose one visible failure whose cause is not yet proven: use `.windsurf/skills/investigation-log/SKILL.md`.
- Fix one failing Elixir test: use `.windsurf/skills/fixing-a-failing-test/SKILL.md`.
- Review markdown files for inconsistent vocabulary: use `.windsurf/skills/keep-vocabulary-consistent/SKILL.md`.
- Export one PDF or a set of PDFs to readable Markdown or page images: use `.windsurf/skills/read-pdfs/SKILL.md`.
- Plan a behaviour-preserving structural change: use `.windsurf/skills/refactor-plan/SKILL.md`.
- Choose the right verification checks after Elixir changes: use `.windsurf/skills/run-checks/SKILL.md`.
- Run only Credo: use `.windsurf/skills/run-credo/SKILL.md`.
- Run only Dialyzer: use `.windsurf/skills/run-dialyzer/SKILL.md`.
- Run only tests: use `.windsurf/skills/run-tests/SKILL.md`.
- Set up a new Elixir project: use `.windsurf/skills/setup-elixir-project/SKILL.md`.

Use the skill as the entry point. Let the skill point to the workflow or guide that is the source of truth.
