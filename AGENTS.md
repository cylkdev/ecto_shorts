# Project

This is an application written using Elixir and Ecto.

EctoShorts is a standardized, data-driven API library that simplifies common Ecto database operations by providing a concise filter language for query building, CRUD actions, and changeset helpers, allowing developers to write shorter, more readable code when working with Ecto queries and database operations through modules like Actions, CommonFilters, CommonChanges, CommonSchema, CommonParams, CommonQuery, and Dynamics.

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

Use `.agent/LANG.md` as the source of truth for canonical project wording, spelling, casing, and routed variants. If the meaning is already clear and only the preferred wording is unclear, update that file instead of widening local prose.

## Documentation Working Rule

Whenever you use a planning or maintenance document in this repository, record the concrete task, the key files, and every document or catalog that must be created or updated in that document's `Task and Key Files` section.

When that work creates or updates a root planning artifact such as an `InvestigationLog`, `ExampleMappingDoc`, `BehaviourSpecDoc`, `ExecPlan`, `RefactorPlan`, `ADR`, or `ArchitectureReview`, also record the exact trigger and the full explicit reasoning in `Trigger for Using This Document`. That section must include the observed facts that triggered the document choice, the full explicit reasoning path from those facts to that document, the closest nearby document types that were rejected and why, and a short replication rule another person can follow.

If the active document has a `Progress` section or checklist, keep the related document maintenance task visible there until the companion documents stay in sync.

## Dependencies

### Database

**Ecto**
- Dependency: `{:ecto, "~> 3.0"}`
- Purpose: Database access and query building

**Ecto SQL**
- Dependency: `{:ecto_sql, "~> 3.10"}`
- Purpose: Database access and query building
