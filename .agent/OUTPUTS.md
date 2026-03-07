# Document Artifact Output Rules

This file is the source of truth for where planning and document artifacts belong in this repository and how their filenames should be shaped.

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## How to Use OUTPUTS.md

Use this file whenever a guide, workflow, or directory map needs the output location or naming pattern for a document artifact.

Do not restate document artifact save locations anywhere else. Other documents should reference `.agent/OUTPUTS.md` instead of copying these rules.

If another checked-in document disagrees with this file, follow `.agent/OUTPUTS.md` and update the stale document in the same change.

Treat `.docs/` as the canonical base path for numbered planning artifacts even if those directories are currently absent from the working tree.

## Coordination Rules

When you use this file to choose an artifact destination or naming pattern, copy that decision into the surrounding active document's `Task and Key Files` section.

Keep the related document maintenance task visible in the surrounding active document's `Progress` section or checklist so artifact creation and sync work are not treated as implicit.

If using this file changes a guide, workflow, catalog, or active document, update that companion document in the same change.

## Safe Parallel Document Maintenance

When you update this document itself, let one coordinator own the final document edit. The coordinator decides the active scope, the canonical wording, and the final structure that lands in the checked-in guide.

After the change scope is stable, worker passes may inspect independent sections, companion files, or stale references in parallel. Each worker pass should return bounded facts such as outdated wording, missing sync updates, stale paths, or terminology drift.

Collect those worker-pass results before you edit this document. Do not update the guide from half-collected scans.

## Canonical Artifact Registry

- `ADR`: `.docs/adrs/NNNN-short-decision-title.md`
- `ArchitectureReview`: `.docs/architecture_reviews/NNNN-short-title.md`
- `BehaviourSpecDoc`: `.docs/behaviour_specs/NNNN-short-title.md`
- `ExampleMappingDoc`: `.docs/example_maps/NNNN-short-title.md`
- `InvestigationLog`: `.docs/investigation_logs/NNNN-short-title.md`
- `ExecPlan`: `.docs/exec_plans/NNNN-short-title.md`
- `RefactorPlan`: `.docs/refactor_plans/NNNN-short-title.md`
- `CodeStyleRuleDoc`: `.agent/styles/<category>/<descriptive title>.md`

## Shared Naming Rules

- Use zero-padded four-digit numbers such as `0001-...` for numbered planning artifacts.
- Keep the ADR naming pattern distinct: ADRs use `NNNN-short-decision-title.md`.
- All other numbered planning artifacts use `NNNN-short-title.md`.
- For `CodeStyleRuleDoc`, use `.agent/styles/AGENTS.md` to choose the category and keep that catalog aligned with the rule file.
