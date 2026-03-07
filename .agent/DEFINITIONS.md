# Definitions

This file is the source of truth for shared definitions used by the standalone documentation system in this repository. Keep glossary-style definitions here, keep canonical wording decisions in `.agent/LANG.md`, and keep every other in-scope document's `## Definitions` section pointer-only.

This Definitions guide is a living document. Keep it up to date as reusable terms are added, definitions are clarified, naming decisions are finalized, scope boundaries change, and terminology drift is discovered in the working tree.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:

- Keep this file self-contained enough that a beginner can understand the current shared vocabulary and how to maintain it from the working tree alone.
- Keep this file safe to restart. If a naming migration or definition rewrite is in progress, leave enough context here to show what changed and what still needs to be synchronized.
- Keep shared definitions canonical. When two files drift, update this file first or in the same change, then align the dependent documents to it.
- Revise this file in real time as reusable terms are added, renamed, split, merged, or clarified. Do not defer glossary maintenance to a later cleanup pass.
- Reflect meaningful terminology changes in `Change Log` so a later reader can see when the shared vocabulary moved.
- Keep categories and wording concrete. Prefer one canonical term per concept and route older variants back to that term.
- Keep meaning and wording separated. Use `.agent/DEFINITIONS.md` for concept definitions and `.agent/LANG.md` for canonical word choice, spelling, and casing. Update both in the same change when both move.

## How to use DEFINITIONS.md

1. Use this file for reusable glossary-style terms that belong across multiple in-scope documents.
2. Update `.agent/DEFINITIONS.md` as soon as the concept meaning is clear. Do not wait until the end of a larger documentation pass.
3. Update `.agent/LANG.md` in the same change when the preferred wording, spelling, or casing also changes.
4. Update dependent documents in the same change so their pointer-only `## Definitions` sections continue to point at current terminology.
5. When a revision changes meaning, not just phrasing, update every affected entry here and record the change in `Change Log`.
6. If repository evidence shows the term does not belong in the shared vocabulary, remove or avoid the shared entry and keep the explanation local to the one document that needs it.

## Safe Parallel Document Maintenance

When you update this document itself, let one coordinator own the final document edit. The coordinator decides the active scope, the canonical wording, and the final structure that lands in the checked-in guide.

After the change scope is stable, worker passes may inspect independent sections, companion files, or stale references in parallel. Each worker pass should return bounded facts such as outdated wording, missing sync updates, stale paths, or terminology drift.

Collect those worker-pass results before you edit this document. Do not update the guide from half-collected scans.

## Context & Orientation

This file owns reusable glossary-style terms for the standalone documentation system rooted in `.agent/` plus the repository root `AGENTS.md`. `.agent/LANG.md` owns canonical wording, spelling, casing, and routed variants for those concepts when the meaning is already understood.

All other in-scope documents should keep `## Definitions` pointer-only and should send shared terminology-definition changes back here instead of redefining terms locally.

If a term is reusable across multiple standalone documents, add or revise it here in the same change that introduces the new meaning. If only the wording changes while the meaning stays the same, update `.agent/LANG.md` and keep the definition stable here. If a term is only needed once and does not belong in the shared vocabulary, rewrite the local sentence instead of widening this file.

It should always be possible for a beginner to restart terminology maintenance from `.agent/DEFINITIONS.md` and the current working tree alone.

## Definitions

### Documentation System Terms

- `ADR`: A living working document that records one lasting architectural or design decision, including its drivers, options, chosen outcome, consequences, and validation path.
- `ArchitectureReview`: A living working document that reviews system shape, resilience, scaling, dependency risk, and failure behaviour.
- `Behaviour specification`: A written description of expected behaviour at a user-observable boundary using observable outcomes, scenarios, or concrete inputs and outputs.
- `BehaviourSpecDoc`: A living working document that turns accepted behaviour into a proof-ready specification at one user-observable boundary.
- `CodeStyleRuleDoc`: A living working document that teaches one reusable code-writing rule or anti-pattern through explanation and examples.
- `Document maintenance task`: An explicit tracked task in an active document's checklist or `Progress` section that keeps the active document, companion documents, catalogs, or artifact notes synchronized with the real work.
- `Example mapping`: A short set of concrete rules and examples used to remove ambiguity before implementation. In this repository, the checked-in artifact for that work is an `ExampleMappingDoc`.
- `ExampleMappingDoc`: A living working document that clarifies intended behaviour with concrete rules, examples, and acceptance-test targets at one user-observable boundary.
- `ExecPlan`: A living working document that defines the exact implementation sequence for a behaviour-changing result.
- `InvestigationLog`: A living working document that diagnoses one visible problem, records the evidence, and decides the next safe action.
- `RefactorPlan`: A living working document that defines a safe structural change without intentionally changing observable behaviour.
- `Rule`: A reusable instruction that tells the reader what they must or must not do.
- `Skill`: A reusable instruction file that tells a reader how to perform one class of task.
- `Source of truth`: The canonical location that owns a shared definition, instruction, or path for the documentation system.
- `Task and Key Files`: A required section in an active document that records the concrete task, the key files or boundaries that matter right now, and every document, catalog, or artifact that must be created or updated in the same change.
- `Trigger for Using This Document`: A required section in each root planning artifact that records the exact observed trigger facts, the full explicit reasoning for choosing that document, the nearby document types that were rejected, and a short replication rule another contributor can follow.
- `Workflow`: A reusable step-by-step process for completing one class of task.

### Coordination and Writing Terms

- `Abstract process verb`: A verb that names a general responsibility or outcome without telling the reader exactly what action to take next. Examples include `handle`, `address`, `manage`, and `ensure`.
- `Asset`: A supporting file or folder stored beside a document or skill, such as an example, template, reference snippet, or script.
- `Collect`: Merge bounded worker-pass results back into the shared mailbox before choosing the next decision or edit.
- `Concept`: One thing the documents are trying to name, such as a process, artifact, role, or architectural idea.
- `Concrete action verb`: A verb that names a specific action the reader can directly perform or observe. Examples include `run`, `open`, `copy`, `write`, `choose`, and `verify`.
- `Constraint`: A rule the reader must not break.
- `Context-specific capture verb`: A capture verb that is acceptable because the sentence names both what to capture and where it belongs, such as `Record the decision in the Decision Log.`
- `Coordinator`: The single owner of the active scope, the current decision, and the order in which worker-pass results are collected and acted on.
- `Executor-facing`: Writing aimed at the person doing the task, using actions that person can actually take.
- `Explanation`: A sentence whose job is to help the reader understand something rather than tell the reader to perform an action.
- `Instruction`: A sentence whose job is to tell the reader to perform an action.
- `Mailbox`: The working place where worker-pass results are recorded before the coordinator decides what to do next.
- `Template`: A reusable fill-in-the-blank structure that keeps documents or skills consistent.
- `Trigger`: The observable condition that tells the reader a rule, skill, workflow, or document should be used. In root planning artifacts, the trigger record must include the visible reasoning that connects those conditions to the document choice.
- `Validation`: Observable proof that a document, skill, or workflow was followed correctly and produced the required result.
- `Vocabulary inconsistency`: A situation where two or more words or phrases are used to refer to the same concept.
- `Worker pass`: One bounded scan, inspection, or writing task that can run independently after the coordinator fixes the current scope.

### Testing and Behaviour Terms

- `Boundary`: The place where one module, public API, process, command, or external surface hands work to something else.
- `Boundary test`: An automated test that exercises the system through a user-observable boundary and checks only what went in and what came out.
- `Changeset`: The Ecto data structure that stores proposed changes and validation errors.
- `Child application`: One app inside an umbrella project. It can have its own `mix.exs`, `lib/`, and `test/`.
- `Concentric circles`: A mental model for outside-in TDD where the outer circle is the boundary test and the inner circle is the focused test for missing business logic.
- `ExUnit`: Elixir's built-in test framework. Run it with `mix test`.
- `Feature test`: The outermost boundary test for a feature. When it passes, the feature is done at the user-observable level.
- `Flaky failure`: A failure that does not happen every run or changes with order, time, randomness, or external-system state.
- `Focused test`: A narrower test used after a boundary test reveals a missing behaviour that is easier to drive at a smaller seam.
- `Outside-in testing`: Starting from the outermost user-observable boundary and working inward toward the implementation details only when the boundary failure shows that inner logic is missing.
- `Query`: The Ecto expression that describes which records to read.
- `Red / Green / Refactor`: The three-step TDD cycle where a test fails for one missing fact, the smallest change makes it pass, and the code is then improved without changing behaviour.
- `Repo`: The module that runs Ecto database reads and writes.
- `Seam`: The point where it becomes easier to continue the work with focused tests instead of only the outer boundary test.
- `Setup failure`: A failure caused by missing dependencies, compile errors, missing config, missing infrastructure, or another prerequisite problem that blocks the real test failure.
- `Stacktrace`: The list of function calls that led to a failure.
- `Umbrella project`: A project with one top-level `mix.exs` and multiple child applications under `apps/`.
- `User-observable boundary`: Any place where a human or caller can observe behaviour from outside the implementation, such as a public function, HTTP endpoint, CLI command, message handler, job side effect, or file output.

### GraphQL and Absinthe Terms

- `Connection`: A Relay-style pagination wrapper that contains `edges` and `pageInfo` metadata.
- `import_fields`: An Absinthe macro used inside `query do`, `mutation do`, or `subscription do` blocks to pull a named object's fields into the root operation.
- `import_types`: An Absinthe macro used in the root schema module to register a type module so its types are available throughout the schema.
- `Input object`: A named group of fields used as an argument to a query or mutation.
- `Nested field`: A field defined on an object type rather than directly on a root operation.
- `Object type`: A named group of fields that describes a domain entity in an Absinthe schema.
- `Payload`: An object type returned by a mutation, typically including result data and `user_errors`.
- `Resolver`: A function that fetches or computes the data for a field.
- `Root field`: A field defined directly on a root operation.
- `Root operation`: One of the three entry points in a GraphQL schema: `query`, `mutation`, or `subscription`.
- `use MyAppWeb, :absinthe_schema`: A macro call that imports the Absinthe schema DSL into the root schema module.
- `use MyAppWeb, :absinthe_schema_notation`: A macro call that imports the Absinthe notation DSL into type, query, mutation, and subscription modules.

### Refactoring and Elixir Terms

- `Canonical path`: The one repository path that should be treated as the source-of-truth path for a document, directory, or catalog entry.
- `Canonical wording`: The single approved word or phrase for one concept family in repository prose.
- `Catalog`: The full library of related reference documents in one directory, such as the smell and technique documents under `.agent/refactor/`.
- `Category`: A folder that groups similar smells, techniques, or rules.
- `Code smell`: A recurring sign that code structure is causing confusion, duplication, coupling, or change pain.
- `Deprecated variant`: An older or rejected word or phrase that should route back to the canonical wording.
- `Function`: A named piece of behaviour inside a module.
- `Language registry`: A maintained source-of-truth document that records canonical wording, spelling, casing, and routed variants.
- `Module`: A named container for functions.
- `Refactoring`: Changing code structure without intentionally changing external behaviour.
- `Refactoring technique`: A repeatable, named way to improve structure, such as `Extract Function`.
- `Struct`: A fixed-shape data structure in Elixir with a known set of fields.
- `Variant routing`: A note that tells the reader which alternate words or phrases should map back to the canonical wording.

## Changelog

- 2026-03-07: Added `Task and Key Files` and `Document maintenance task` so active documents must record the concrete task, key files, and required document-sync work explicitly.
- 2026-03-07: Added living-document maintenance rules, restartability guidance, coordinator/worker-pass collection guidance, and change-log discipline adapted from `.agent/PLANS.md` so shared definitions are updated in real time.
- 2026-03-07: Split meaning from wording by routing canonical word choice, spelling, and casing to `.agent/LANG.md` while keeping glossary-style concept definitions in `.agent/DEFINITIONS.md`.
- 2026-03-07: Expanded `Trigger` to cover document selection and added `Trigger for Using This Document` as the required root planning artifact section for recording exact trigger facts and full explicit reasoning.
