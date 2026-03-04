---
trigger: always_on
---

Living documents keep a coding agent's reasoning visible and traceable across long sessions. Write every document artifact to one shared place so any contributor can find it and continue the work without hunting through multiple directories.

## Requirements

Do not work in silence. Create the document artifacts before you start working and keep it updated as you work or make decisions.

## Shared output location

Store all document artifacts under `docs/` at the app root.

Use these subdirectories and naming rules.

- ADR documents go in `docs/adr/` and use the filename `NNNN-short-title.md`.
- ExecPlan documents go in `docs/plans/` and use the filename `NNNN-short-title.md`.
- ExampleMapPlan documents go in `docs/example_map_plans/` and use the filename `NNNN-short-title.md`.
- RefactorPlan documents go in `docs/refactor/` and use the filename `NNNN-short-title.md`.
- SpecPlan documents go in `docs/specs/` and use the filename `NNNN-short-title.md`.
- Architecture Review documents go in `docs/architecture_reviews/` and use the filename `NNNN-short-title.md`.

`NNNN` is a zero-padded four-digit sequence number, like `0001`, `0002`, and so on.

## Filename conflict avoidance

Before creating a new document, scan the target subdirectory and find the highest `NNNN` prefix already in use. Use the next number in the sequence.

If the subdirectory is empty or does not exist yet, start at `0001`.

## Umbrella routing

Use these rules to decide where `docs/` lives.

- If this is not an umbrella app, write to `docs/` at the project root.
- If this is an umbrella app and the work is scoped to one child app, write to `apps/<child>/docs/`. Also add a short reference entry in the umbrella root `docs/` that links to the child document and explains why it matters.
- If this is an umbrella app and the work is cross-cutting, write to the umbrella root `docs/`.

## Mandatory usage

Each scenario below requires a document. These are required so decisions, plans, and discoveries stay traceable across long sessions.

- If you make an architecture decision, write an ADR.
  The format reference is `.agent/ADRS.md`.
  An architecture decision is choosing between two or more plausible options that changes how the system is built or operated.

- If you implement a complex feature or a significant system change with multiple steps, write an ExecPlan.
  The format reference is `.agent/PLANS.md`.

- If you clarify a feature or story using structured examples before implementation, write an ExampleMapPlan.
  The format reference is `.agent/EXAMPLE_MAP_PLANS.md`.

- If you refactor code, write a RefactorPlan.
  The format reference is `.agent/REFACTOR_PLANS.md`.
  A refactor is a behaviour-preserving structural improvement.

- If you write behaviour specifications with runnable proofs, write a SpecPlan.
  The format reference is `.agent/SPEC_PLANS.md`.

- If you review a system's architecture for failure modes, scalability risks, or operational resilience, write an Architecture Review.
  The format reference is `.agent/ARCHITECTURE_REVIEW.md`.