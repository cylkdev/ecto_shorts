---
trigger: always_on
---

Living documents keep a coding agent's reasoning visible and traceable across long sessions. Every document artifact produced during development must be written to a shared location so that any contributor — human or agent — can find, reference, and continue the work without searching multiple directories.

## Shared output location

All document artifacts live under `docs/` at the app root. Each document type has a dedicated subdirectory:

| Document Type       | Subdirectory               | Naming                        |
|---------------------|----------------------------|-------------------------------|
| ADR                 | `docs/adr/`               | `NNNN-short-title.md`        |
| ExecPlan            | `docs/plans/`             | `NNNN-short-title.md`        |
| ExampleMapPlan      | `docs/example_map_plans/` | `NNNN-short-title.md`        |
| RefactorPlan        | `docs/refactor/`          | `NNNN-short-title.md`        |
| SpecPlan            | `docs/specs/`             | `NNNN-short-title.md`        |
| Architecture Review | `docs/architecture_reviews/` | `NNNN-short-title.md`     |

`NNNN` is a zero-padded four-digit sequence number (0001, 0002, …).

## Filename conflict avoidance

Before creating a new document, scan the target subdirectory for the highest existing `NNNN` prefix and use the next number in the sequence. When the subdirectory is empty or does not yet exist, start at `0001`.

## Umbrella routing

- **Non-umbrella app**: write to `docs/` at the project root.
- **Umbrella app, work scoped to a child app**: write the document to `apps/<child>/docs/` and add a short reference entry in the umbrella root `docs/` that links to the child document and summarizes its relevance.
- **Umbrella app, cross-cutting work**: write to the umbrella root `docs/`.

## Mandatory usage

Every scenario below requires the specified document type. These are not optional — they ensure the agent accurately tracks decisions, plans, and discoveries across long sessions.

| Scenario | Required document | Format reference |
|----------|-------------------|------------------|
| An architecture decision is made (choosing between two or more plausible options that changes how the system is built or operated) | ADR | `.agent/ADRS.md` |
| Implementing a complex feature or significant system change with multiple steps | ExecPlan | `.agent/PLANS.md` |
| Clarifying a feature or story through structured examples before implementation | ExampleMapPlan | `.agent/EXAMPLE_MAP_PLANS.md` |
| Refactoring code (behaviour-preserving structural improvement) | RefactorPlan | `.agent/REFACTOR_PLANS.md` |
| Writing behaviour specifications with runnable proofs | SpecPlan | `.agent/SPEC_PLANS.md` |
| Reviewing the architecture of a system for failure modes, scalability risks, and operational resilience | Architecture Review | `.agent/ARCHITECTURE_REVIEW.md` |
