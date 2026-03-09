# Project Language Registry

This file is the source of truth for canonical word choices used across this repository. Keep preferred project words, phrases, spelling, casing, and routing notes here so future work can restart from this file and the working tree instead of rescanning the whole repository to infer the current language.

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used by the standalone documentation system in this repository. If a reusable language-maintenance term is missing, add it there instead of defining it locally in this document.

## Purpose / Big Picture

Use this file when the unresolved question is which word, phrase, spelling, or capitalization the repository should use for an already-understood concept. Record the canonical wording, the conflicting or older variants, where the wording applies, and any scope note that keeps the choice safe to reuse.

This file does not replace `.agent/DEFINITIONS.md`. `DEFINITIONS.md` owns meaning. `LANG.md` owns wording. When a change affects both meaning and wording, update both files in the same change and then align the dependent documents.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:

- Keep this file self-contained enough that a beginner can choose the right word from the registry and understand when `.agent/DEFINITIONS.md` must change as well.
- Keep one canonical wording per concept family unless the working tree proves two distinct contexts really need different wording.
- Record conflicting, older, or rejected variants and route them back to the canonical wording instead of leaving the relationship implicit.
- Update this file in real time when naming decisions, module-family labels, user-facing phrases, spelling, or casing change in the working tree.
- Reflect meaningful wording changes in `Change Log` so later readers can see when repository language moved.
- Prefer wording that is already supported by current repository evidence such as module docs, README language, tests, or accepted checked-in guides.
- If a wording change also changes the concept boundary or meaning, update `.agent/DEFINITIONS.md` first or in the same change, then align this file and the dependent documents.

## How to Use LANG.md

1. Decide whether the question is about meaning or wording. If it is about meaning, start with `.agent/DEFINITIONS.md`. If the meaning is already clear and only the preferred wording is unclear, use this file.
2. Find the concept family in `Canonical Language Registry`.
3. Reuse the listed canonical wording exactly, including spelling, casing, and hyphenation, unless the registry itself says the wording is scoped more narrowly.
4. If the working tree uses a conflicting variant for the same concept, update this file before or in the same change as the dependent files so the routing stays explicit.
5. When a reusable concept is missing from both `.agent/LANG.md` and `.agent/DEFINITIONS.md`, define the concept in `.agent/DEFINITIONS.md`, record the preferred wording here, and then update the dependent documents.
6. If repository evidence shows a wording choice does not belong in shared language, keep it local to the one file that needs it instead of widening this registry.

## Coordination Rules

When you use this file to settle wording, copy the concrete language decision into the surrounding active document's `Task and Key Files` section.

Keep the related document maintenance task visible in the surrounding active document's `Progress` section or checklist so vocabulary-sync work is not treated as implicit.

If a wording decision changes guide routing, terminology routing, or vocabulary workflow behaviour, update the companion routing documents in the same change. In this repository that usually means `.agent/AGENTS.md`, `AGENTS.md`, `.agent/DEFINITIONS.md`, or `.agent/workflows/Consistent Vocabulary.md`.

## Scope Boundaries

Keep canonical project wording here when it is reusable across multiple files or document families. Good fits include product naming, module-family labels, user-facing capability names, casing, hyphenation, and variant-routing notes.

Do not copy glossary-style concept definitions into this file. Keep those in `.agent/DEFINITIONS.md`.

Do not widen one-off wording into this registry unless the same choice is already reusable across multiple files or the mismatch is already causing drift.

## Canonical Language Registry

### Product and Package Names

- `EctoShorts`: Use this exact casing for the project name, library name, and module namespace in prose.
- `ecto_shorts`: Use this exact snake_case form only for literal package names, Mix application names, config keys, filenames, or code examples that require the actual identifier.
- `data-driven`: Hyphenate this phrase in prose.

### Core Capability Language

- `filter language`: Use this phrase for the data-driven query input format accepted by `EctoShorts.CommonFilters` and the read/query APIs built on top of it. Reserve `DSL` for direct references to Ecto's macro DSL or executable example artifacts such as `test/examples/ecto_query_dsl.exs`.
  Route these variants here when they mean the same thing: `filtering language`, `query DSL`, `filters DSL`, `query language`.
- `query building`: Use this phrase in public prose for turning data into an `Ecto.Query`.
  Route these variants here when they mean the same thing: `query composition`, `query construction`.
- `filter params`: Use this phrase for maps or keyword lists that describe query conditions or query operations.
  If the input is broader than filtering alone, `params` is still acceptable. Avoid switching to `query params` or plain `filters` for the same concept inside one document.
- `changeset helpers`: Use this phrase for `EctoShorts.CommonChanges` behaviour.
- `CRUD operations`: Use this phrase for create, read, update, and delete work. Keep `CRUD` uppercase in prose.
- `bulk operations`: Use this phrase for `insert_all/3`, `update_all/4`, and `delete_all/3`.
  Do not call these `batch` operations.
- `batch operations`: Use this phrase for keyed fetch and grouping helpers such as `batch/5` and `batch_preload/4`.
  Do not call these `bulk` operations.
- `transactional operations`: Use this phrase in narrative prose for `Ecto.Multi`-backed work that must succeed or fail together.
  `Multi operations` is acceptable only when the text is intentionally naming the `Multi` section, the `Ecto.Multi` abstraction, or the `*_many` API family directly.

### Ecto Input and Data Terms

- `schema module`: Use this phrase when the input is an `Ecto.Schema` module such as `MyApp.Post`.
- `schema struct`: Use this phrase when the input is a schema struct instance such as `%MyApp.Post{}`.
- `query`: Use this phrase for an `Ecto.Query` value.
- `queryable`: Use this phrase only when the broader Ecto meaning is intended and the input may be a schema module, schema struct, source tuple, or query.
- `Repo`: Use this capitalized form when naming the Ecto concept or a concrete repo module such as `MyApp.Repo`.
  Use `repo` in prose only when you mean the configured repo generically.

### Module Family Names

- `EctoShorts.Actions`: The main entry point for CRUD, bulk, batch, and transactional operations. Use `Actions` only after the surrounding example or paragraph has already established the alias or module name.
- `EctoShorts.CommonFilters`: The module family that turns params into an `Ecto.Query`.
- `EctoShorts.CommonChanges`: The module family for changeset helpers.
- `EctoShorts.CommonSchema`: The module family for schema introspection, source normalization, and schema-based helper behaviour.
- `EctoShorts.CommonParams`: The module family for preparing params for bulk operations.
- `EctoShorts.CommonQuery`: The module family for query inspection helpers.
- `EctoShorts.Dynamics`: The module family for building `Ecto.Query.dynamic/2` expressions from data.
- `EctoShorts.Generator`: The internal module family for compile-time clause generation.
- `EctoShorts.Testing`: The module family for test helpers.
- `EctoShorts.SchemaHelpers`: An actual module that still exists in the codebase.
  Use this exact name only when the text is intentionally about that module or its API. Do not use `SchemaHelpers` as the general label for the main schema utility family when `CommonSchema` is the intended concept.

### Documentation System Wording

- `Trigger for Using This Document`: Use this exact section title for the root planning artifact section that records document-selection trigger facts and full explicit reasoning.
  Route these variants here when they mean the same thing: `Document Trigger`, `Why This Document`, `Why this guide was chosen`, `Trigger for this document`.

### Writing and Naming Preferences

- `plain maps and keyword lists`: Prefer this phrase when describing the main non-macro input style of the library.
- `build queries from data`: Prefer this phrase over macro-heavy wording when describing the library's core approach.
- `automatic rollback`: Prefer this phrase when describing transactional failure behaviour.
- `minimal boilerplate`: Prefer this phrase when the intended point is reduced repetitive setup.

## Change Log

- 2026-03-07: Created `.agent/LANG.md` as the source of truth for canonical project wording, spelling, casing, and routing notes. Seeded the registry from the current README, module docs, and root instructions.
- 2026-03-07: Added `Trigger for Using This Document` as the canonical section title for root planning artifact trigger records and routed common variants back to it.
