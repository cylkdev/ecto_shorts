---
trigger: always_on
---

Use this rule when the current unresolved question is where a document artifact belongs, how to name it, or whether companion documents are still in sync after artifact-related changes.

Read `.agent/OUTPUTS.md`. Treat it as the source of truth for artifact destinations and filename patterns.

Read `.agent/AGENTS.md`. Treat it as the source of truth for the document-maintenance contract around active documents, companion updates, and required sync work.

Do not duplicate document artifact location tables, filename registries, or naming rules in Windsurf rules, workflows, skills, or repository snapshots. Point back to `.agent/OUTPUTS.md` instead.

Document artifact work is not complete until all of the following are true:

- The surrounding active document's `Task and Key Files` section records the concrete task, the key files, and every guide, workflow, catalog, rule, or artifact that must be created or updated.
- The surrounding active document's `Progress` section or checklist keeps the related document-maintenance task visible until every required companion update is finished.
- Any checked-in guide, workflow, rule, catalog, directory map, repository snapshot, or active document that disagrees with `.agent/OUTPUTS.md` or `.agent/AGENTS.md` is updated in the same change.
- No routing, naming, terminology, or inventory drift is left behind for a later cleanup pass.

When artifact routing or naming changes, inspect and update every affected companion document in the same change. This usually includes:

- `.agent/AGENTS.md` when root-guide inventory, guide-selection rules, output routing, or terminology routing changes.
- The owning root `.agent` guide and any `.windsurf/workflows/*.md` or `.windsurf/skills/*/SKILL.md` entry point that routes to that artifact type.
- `.agent/DEFINITIONS.md` when the change adds, removes, renames, splits, or clarifies a reusable term.
- `.agent/LANG.md` when the change affects canonical wording, spelling, casing, or variant routing.

If an existing artifact lives at a stale path or uses a stale filename pattern, move or rename it to match `.agent/OUTPUTS.md` and update all references in the same change.

If another checked-in document disagrees with `.agent/OUTPUTS.md` or `.agent/AGENTS.md`, follow those source files and repair the stale document immediately. Do not leave sync work implicit.
