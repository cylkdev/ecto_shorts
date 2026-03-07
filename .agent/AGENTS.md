# Planning Directory

This file is the map for the planning guides that live at the root of `.agent/`. Treat the reader as a complete beginner to this repository: they have only the current working tree and this directory map. There is no memory of prior guide discussions and no external context.

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Purpose / Big Picture

Use this file when you need to decide which root `.agent` guide to open first, what that guide produces, where its shared source-of-truth rules live, and what guide or work state usually comes next.

This file does not replace the guides it points to. It helps a beginner choose the right one quickly and find `.agent/OUTPUTS.md`, `.agent/DEFINITIONS.md`, or `.agent/LANG.md` when shared-routing questions come up.

## How to Use AGENTS.md

When you need the fastest route to the correct planning guide, use `Choose by Task`. When you need the detailed answer for one guide, use `Pick the Right Guide`. When the work will likely move across more than one guide, use `How the Main Guides Fit Together`.

Keep this file open while you work on the planning toolkit. If a guide is added, renamed, moved, or materially changed, update this map in the same change so a beginner can still restart from here.

When you open this file or any root guide it points to, update the surrounding active document immediately. Record the concrete task, the key files, and every guide, catalog, or artifact rule that must be created or updated in that document's `Task and Key Files` section.

When one of the seven root planning artifact guides produces or updates an `InvestigationLog`, `ExampleMappingDoc`, `BehaviourSpecDoc`, `ExecPlan`, `RefactorPlan`, `ADR`, or `ArchitectureReview`, require that artifact to include `Trigger for Using This Document` immediately after `Task and Key Files`. That section must record the exact trigger facts, the full explicit reasoning path that made this the correct document, the closest nearby document types that were rejected and why, and a replication rule a later contributor can reuse.

Keep the related document maintenance task visible in that same document's `Progress` section or checklist. If the root guide inventory, guide-selection rules, output routing, or terminology routing changes, list `.agent/AGENTS.md` itself among the required updates.

## Safe Parallel Document Maintenance

When you update this document itself, let one coordinator own the final document edit. The coordinator decides the active scope, the canonical wording, and the final structure that lands in the checked-in guide.

After the change scope is stable, worker passes may inspect independent sections, companion files, or stale references in parallel. Each worker pass should return bounded facts such as outdated wording, missing sync updates, stale paths, or terminology drift.

Collect those worker-pass results before you edit this document. Do not update the guide from half-collected scans.

## Choose by Task

Use this quick chooser when you need the fastest path through the planning toolkit.

- When you are building or changing behaviour and the intended behaviour is still unclear, use `EXAMPLE_MAPPING.md` first. When the intended behaviour is accepted, use `BEHAVIOURS.md`. When the proof path and implementation target are already clear, use `PLANS.md`.
- When you are refactoring without intentionally changing behaviour, use `REFACTOR_PLANS.md`.
- When you are diagnosing a visible problem whose cause is not yet proven, use `INVESTIGATION_LOGS.md`.
- When you are reviewing architecture, risk, or failure spread, use `ARCHITECTURE_REVIEW.md`.
- When you are adding or revising a style rule, use `.agent/styles/AGENTS.md` first, then `CODE_STYLE_RULES.md`.
- When you need to resolve shared term meaning or add a reusable definition, use `DEFINITIONS.md`.
- When you need to resolve shared wording, spelling, casing, or conflicting variants for an already-understood concept, use `LANG.md`.
- When you need canonical output locations or naming rules for planning and document artifacts, use `OUTPUTS.md`.
- When you need repository-specific validation, linting, coverage, database, or test commands, use `PROJECT.md`.

A simple rule of thumb usually works:

- Unknown cause: investigate first.
- Known boundary but unclear intended behaviour: map the behaviour first.
- Accepted behaviour but no proof-ready specification yet: write a behaviour specification.
- Clear behaviour specification and ready to change code: plan the implementation.
- Same behaviour, better structure: write a refactor plan.

## Important Path Rules

The planning guides covered by this file live directly under `.agent/`. Use the exact root-level paths shown in `Directory Layout` and `Pick the Right Guide`. Treat those root-level names as the source of truth for guide selection.

## Directory Layout

Current layout:

```text
.agent/
├── AGENTS.md
├── ADRS.md
├── ARCHITECTURE_REVIEW.md
├── BEHAVIOURS.md
├── CODE_STYLE_RULES.md
├── DEFINITIONS.md
├── EXAMPLE_MAPPING.md
├── INVESTIGATION_LOGS.md
├── LANG.md
├── OUTPUTS.md
├── PLANS.md
├── PROJECT.md
└── REFACTOR_PLANS.md
```

At the time this file was updated, this map covers thirteen Markdown files at the root of `.agent/`. Eight are planning guides. `DEFINITIONS.md` is the shared definitions guide. `LANG.md` is the shared language guide. `OUTPUTS.md` is the shared document-artifact output guide. `PROJECT.md` is the shared repository command guide. `AGENTS.md` is the directory map that points to them.

## Pick the Right Guide

Use these entries when you need the detailed answer for one guide instead of the fast chooser above. Each entry tells you what question the guide answers, when to use it, what it produces, where its canonical output rule lives, what usually comes next, what examples exist now, and which companion docs matter.

For the seven root planning artifact guides, the produced artifact must also include `Trigger for Using This Document` using the canonical wording from `.agent/LANG.md`.

### `ADRS.md`

Use this guide when a lasting architectural or design decision needs a durable record.

- Answers: Why was this option chosen, and what tradeoffs come with it?
- Use it when: One important decision needs to stay explicit over time.
- Produces: One `ADR` for one decision.
- Output goes in: See `.agent/OUTPUTS.md` for the canonical ADR output location and naming rules.
- Next step: Usually back to implementation work, refactor work, or architecture review follow-up.
- Examples now: Check the canonical ADR directory named in `.agent/OUTPUTS.md`. No finished ADRs are checked in there today.
- Companion docs: `PLANS.md`, `REFACTOR_PLANS.md`, `ARCHITECTURE_REVIEW.md`.

### `ARCHITECTURE_REVIEW.md`

Use this guide when the question is about system shape, risk, scaling, restart, or failure spread.

- Answers: Can this design hold up under load, failure, restart, and partial outage?
- Use it when: You are reviewing Elixir or OTP architecture for resilience, dependency risk, or failure behaviour.
- Produces: One `ArchitectureReview` document.
- Output goes in: See `.agent/OUTPUTS.md` for the canonical ArchitectureReview output location and naming rules.
- Next step: Usually an `ADR`, an `ExecPlan`, or a `RefactorPlan`.
- Examples now: Check the canonical ArchitectureReview directory named in `.agent/OUTPUTS.md`. No finished architecture reviews are checked in there today.
- Companion docs: `ADRS.md`, `PLANS.md`, `REFACTOR_PLANS.md`.

### `BEHAVIOURS.md`

Use this guide when intended behaviour is accepted, but implementation should not start until the proof-ready specification is explicit.

- Answers: How do I turn accepted behaviour into a proof-ready behaviour specification?
- Use it when: The boundary is known, the intended behaviour is accepted, and the proof path still needs to be written down.
- Produces: One `BehaviourSpecDoc`.
- Output goes in: See `.agent/OUTPUTS.md` for the canonical BehaviourSpecDoc output location and naming rules.
- Next step: Usually `PLANS.md` when implementation planning should begin.
- Examples now: Check the canonical BehaviourSpecDoc directory named in `.agent/OUTPUTS.md`. No finished behaviour specifications are checked in there today.
- Companion docs: `EXAMPLE_MAPPING.md`, `PLANS.md`.

### `CODE_STYLE_RULES.md`

Use this guide when the work is to create or revise a reusable style rule, not merely to find one.

- Answers: How do I write a new style-rule document?
- Use it when: You need to create or revise a style-rule document, not just find an existing rule.
- Produces: One code-style reference document and the supporting written artifacts it defines.
- Output goes in: See `.agent/OUTPUTS.md` for the canonical CodeStyleRuleDoc output location and naming rules.
- Next step: Update `.agent/styles/AGENTS.md` so the style catalog matches the working tree.
- Examples now: `.agent/styles/ecto/Prefer Separate Steps When Building an Ecto Query.md` and `.agent/styles/code_related_anti_patterns/Complex Else Clauses in With.md`.
- Companion docs: `.agent/styles/AGENTS.md`.

### `DEFINITIONS.md`

Use this guide when the unresolved question is what a shared term means or where reusable definitions should be maintained.

- Answers: What does this shared term mean, and where should repository-wide definitions live?
- Use it when: A reusable term is missing, definition drift is discovered, or a guide needs shared meaning instead of a local definition.
- Produces: One maintained definition registry for the documentation system.
- Output goes in: `.agent/DEFINITIONS.md`.
- Next step: Return to the owning guide or catalog once the shared meaning is explicit.
- Examples now: `.agent/DEFINITIONS.md` itself is the checked-in source of truth for shared definitions.
- Companion docs: `AGENTS.md`, `LANG.md`, `OUTPUTS.md`, `PROJECT.md`, the root planning guides, and the companion catalogs.

### `LANG.md`

Use this guide when the unresolved question is which shared word, phrase, spelling, or capitalization the repository should use for an already-understood concept.

- Answers: Which word should this repository use, and where should shared wording decisions live?
- Use it when: Vocabulary drift is discovered, a wording choice needs a canonical form, or a guide needs shared phrasing instead of local wording.
- Produces: One maintained language registry for the repository.
- Output goes in: `.agent/LANG.md`.
- Next step: Return to the owning guide or catalog once the canonical wording is explicit.
- Examples now: `.agent/LANG.md` itself is the checked-in source of truth for shared wording, spelling, casing, and routed variants.
- Companion docs: `AGENTS.md`, `DEFINITIONS.md`, `OUTPUTS.md`, `PROJECT.md`, the root planning guides, and `.agent/workflows/Consistent Vocabulary.md`.

### `EXAMPLE_MAPPING.md`

Use this guide when the user-observable boundary is known, but the intended behaviour still needs to be clarified with examples and rules.

- Answers: What should happen at this user-observable boundary?
- Use it when: The boundary is known, but the intended behaviour is still unclear or disputed.
- Produces: One `ExampleMappingDoc`.
- Output goes in: See `.agent/OUTPUTS.md` for the canonical ExampleMappingDoc output location and naming rules.
- Next step: Usually `BEHAVIOURS.md` once the behaviour is clear enough to turn into a proof-ready specification.
- Examples now: Check the canonical ExampleMappingDoc directory named in `.agent/OUTPUTS.md`. No finished example maps are checked in there today.
- Companion docs: `INVESTIGATION_LOGS.md`, `PLANS.md`.

### `INVESTIGATION_LOGS.md`

Use this guide when a visible problem exists, but its cause is not yet proven.

- Answers: What is failing, and what facts prove it?
- Use it when: You need diagnosis and evidence before you can safely clarify behaviour or change code.
- Produces: One `InvestigationLog`.
- Output goes in: See `.agent/OUTPUTS.md` for the canonical InvestigationLog output location and naming rules.
- Next step: Usually `EXAMPLE_MAPPING.md`, `BEHAVIOURS.md`, `PLANS.md`, or `REFACTOR_PLANS.md`, depending on what the diagnosis resolves.
- Examples now: Check the canonical InvestigationLog directory named in `.agent/OUTPUTS.md`. No finished investigation logs are checked in there today.
- Companion docs: `EXAMPLE_MAPPING.md`, `BEHAVIOURS.md`, `PLANS.md`, `REFACTOR_PLANS.md`.

### `PLANS.md`

Use this guide when the desired behaviour and proof path are already clear enough to plan implementation directly.

- Answers: How do I implement a clear change from start to finish?
- Use it when: The change is behaviour-changing and the diagnosis, behaviour, and proof path are already explicit enough to implement.
- Produces: One `ExecPlan`.
- Output goes in: See `.agent/OUTPUTS.md` for the canonical ExecPlan output location and naming rules.
- Next step: Implementation work, and sometimes `ADRS.md` if the change creates a lasting decision.
- Examples now: Check the canonical ExecPlan directory named in `.agent/OUTPUTS.md`. No finished exec plans are checked in there today.
- Companion docs: `INVESTIGATION_LOGS.md`, `EXAMPLE_MAPPING.md`, `BEHAVIOURS.md`, `ADRS.md`.

### `OUTPUTS.md`

Use this file when the unresolved question is where a planning or document artifact should be saved or how it should be named.

- Answers: Where does each planning or document artifact belong, and what filename pattern should it use?
- Use it when: A guide, workflow, or directory map needs the canonical artifact destination or naming pattern.
- Produces: One maintained output-location registry for document artifacts.
- Output goes in: `.agent/OUTPUTS.md`.
- Next step: Return to the guide or workflow that owns the main task once the destination rule is explicit.
- Examples now: `.agent/OUTPUTS.md` itself is the checked-in source of truth for artifact destinations and naming rules.
- Companion docs: `AGENTS.md`, the root planning guides, and the artifact-oriented `.windsurf/workflows/*.md`.

### `PROJECT.md`

Use this guide when the unresolved question is which repository-specific command to run, where to run it, or how wide validation should be.

- Answers: Which repository command should I run from which directory, with which prerequisites, and how wide should validation be?
- Use it when: Another guide or workflow already owns the main task, but the correct project command surface is still unclear.
- Produces: One maintained project command reference.
- Output goes in: `.agent/PROJECT.md`.
- Next step: Return to the guide or workflow that owns the main task once the command choice is explicit.
- Examples now: `.agent/PROJECT.md` itself is the checked-in project command guide.
- Companion docs: `PLANS.md`, `REFACTOR_PLANS.md`, `INVESTIGATION_LOGS.md`, `.agent/workflows/TDD_BDD.md`.

### `REFACTOR_PLANS.md`

Use this guide when the job is to improve structure without intentionally changing observable behaviour.

- Answers: How do I change structure without changing behaviour?
- Use it when: You need a safe refactor rather than a new feature or bug-fix behaviour change.
- Produces: One `RefactorPlan`.
- Output goes in: See `.agent/OUTPUTS.md` for the canonical RefactorPlan output location and naming rules.
- Next step: Refactor work, and sometimes `ADRS.md` if the refactor creates a lasting design decision.
- Examples now: Check the canonical RefactorPlan directory named in `.agent/OUTPUTS.md`. No finished refactor plans are checked in there today.
- Companion docs: `.agent/refactor/AGENTS.md`, `ADRS.md`.

## How the Main Guides Fit Together

Use this section when the work spans more than one guide. The planning guides are meant to hand work forward as uncertainty becomes smaller.

### Main path

Most feature work and bug-fix work follows the same shape. When the cause is not yet proven, begin with diagnosis. When the boundary is known but the expected behaviour is still unclear, move to behaviour clarification. When the behaviour is accepted but the proof path is not yet explicit, move to proof-ready specification. When the proof path and change target are ready, move to implementation planning.

1. `INVESTIGATION_LOGS.md` when you do not yet know what is really wrong.
2. `EXAMPLE_MAPPING.md` when you know the boundary but still need to make the expected behaviour explicit.
3. `BEHAVIOURS.md` when the behaviour is accepted and needs a proof-ready specification.
4. `PLANS.md` when the behaviour specification is clear and you are ready to plan the code change.

Choose one owning guide first. If that guide needs repository-specific commands, let it stay the coordinator, fan out any read-only companion checks you need, and use `PROJECT.md` to collect the shared command choice before you continue.

### Refactor path

Use the refactor branch when the observable behaviour should stay the same.

1. `INVESTIGATION_LOGS.md` if the real problem is still unknown.
2. `REFACTOR_PLANS.md` when the goal is to improve structure without changing behaviour.

### Special guides

Use the special guides when the question is not part of the main feature or refactor flow.

- `ADRS.md` records why one lasting decision was made.
- `ARCHITECTURE_REVIEW.md` reviews system-level shape, failure spread, and operational risk.
- `CODE_STYLE_RULES.md` records reusable code-writing guidance after you have found the correct style category in `.agent/styles/AGENTS.md`.
- `DEFINITIONS.md` records the shared definitions that the rest of the documentation system should reuse instead of redefining locally.
- `LANG.md` records the shared wording that the rest of the repository should reuse instead of reinventing locally.
- `OUTPUTS.md` records the canonical save locations and filename patterns for document artifacts.
- `PROJECT.md` records the repository-specific command surface that the other guides should cite instead of duplicating.

## Shared Sources of Truth

Use this section when the main question is where a planning or document artifact belongs, where reusable definitions or wording should live, or which guide owns a shared rule.

- `.agent/OUTPUTS.md` is the only source of truth for planning and document artifact destinations and naming rules.
- `.agent/DEFINITIONS.md` is the only source of truth for reusable documentation-system definitions.
- `.agent/LANG.md` is the only source of truth for reusable repository wording, spelling, casing, and routed variants.
- Root planning guides and artifact-oriented `.windsurf/workflows/*.md` files should reference `.agent/OUTPUTS.md` instead of restating save locations.
- Root planning guides, catalog guides, and repository-level instruction documents should reference `.agent/DEFINITIONS.md` instead of keeping local glossary copies.
- Root planning guides, catalog guides, repository-level instruction documents, and vocabulary workflows should reference `.agent/LANG.md` instead of recreating canonical word choices locally.
- `.agent/PROJECT.md` remains the source of truth for repository commands, not artifact destinations.
- `.agent/styles/AGENTS.md` still chooses the style-rule category, but the CodeStyleRuleDoc save location itself is defined in `.agent/OUTPUTS.md`.

## Companion Catalogs Outside This Directory

Use these companion catalogs when the guide you chose depends on a checked-in sub-catalog.

- `.agent/refactor/AGENTS.md` helps you choose code smells and refactor techniques while working with `REFACTOR_PLANS.md`.
- `.agent/skills/AGENTS.md` helps you choose or maintain reusable skills when the work belongs under `.agent/skills/`.
- `.agent/styles/AGENTS.md` helps you choose the correct style-rule category while working with `CODE_STYLE_RULES.md`.

## Simple Terms

Use these short definitions when the planning-guide vocabulary is still unfamiliar.

- Guide: A file that explains how to do one kind of work.
- Artifact: The document or output a guide produces.
- User-observable boundary: The nearest place where behaviour can be observed by a human from the outside.
- Coordinator: The single pass that owns sequencing and next-step decisions.
- Worker pass: One bounded, independently runnable inspection or writing task.
- Mailbox: The section where worker-pass results are recorded before the next decision.
- Collect: Merge worker-pass results back into the mailbox before branching.
- Diagnosis: Proving what is happening and why.
- Handoff: The point where one guide stops and another guide takes over.
- Validation: The checks that prove the result is correct.

## Contributor Maintenance Rules

This file is only useful if it stays aligned with both the root `.agent/` directory and the current working tree. Update it in the same change whenever the planning toolkit changes.

- If a guide is added, removed, renamed, moved, or materially changed, update this file in the same change.
- Record the concrete task, key files, and required guide, catalog, or artifact updates in the surrounding active document before you rely on this file for routing.
- Keep the shared `Trigger for Using This Document` contract aligned across `ADRS.md`, `ARCHITECTURE_REVIEW.md`, `BEHAVIOURS.md`, `EXAMPLE_MAPPING.md`, `INVESTIGATION_LOGS.md`, `PLANS.md`, and `REFACTOR_PLANS.md`.
- Keep the root-level `.agent/` guide paths accurate.
- Keep every root guide listed in both `Directory Layout` and `Pick the Right Guide`.
- Keep `DEFINITIONS.md`, `LANG.md`, and `OUTPUTS.md` listed anywhere this file explains shared definitions, shared wording, or artifact destinations.
- Keep output-location notes pointed at `.agent/OUTPUTS.md`. If a guide or workflow restates a save path, collapse it back to the shared source of truth.
- If a document directory named in `.agent/OUTPUTS.md` exists for one of these guide outputs, keep an `AGENTS.md` file in that directory. If the directory exists and is missing one, create it in the same change.
- Replace stale example paths as soon as the working tree changes.
- Keep the prose beginner-friendly, direct, and concrete.

## Contributor Maintenance Checklist

Use this checklist every time you update this file or any root planning guide.

1. Confirm the current root-level `.agent/*.md` file list from the working tree.
2. Confirm the planning guides still live directly under `.agent/`.
3. Confirm every root guide still appears in `Directory Layout` and `Pick the Right Guide`.
4. Confirm the `Choose by Task` chooser still matches the main jobs people do in this repository, including when to use `DEFINITIONS.md`, `LANG.md`, `OUTPUTS.md`, and `PROJECT.md`.
5. Confirm the surrounding active document still records the concrete task, key files, and required guide, catalog, or artifact updates for the work you are doing.
6. Confirm each guide entry still answers the same seven questions: what it answers, when to use it, what it produces, where its canonical output rule lives when applicable, what comes next, what examples exist, and which companion docs matter.
7. Confirm the seven root planning artifact guides still require `Trigger for Using This Document` as the canonical section title and keep it positioned immediately after `Task and Key Files`.
8. Confirm every named guide path, companion path, and example note still matches the working tree.
9. Confirm output-location statements point to `.agent/OUTPUTS.md` instead of restating artifact directories.
10. Confirm definition-routing statements point to `.agent/DEFINITIONS.md` and wording-routing statements point to `.agent/LANG.md` instead of restating shared glossary or language content.
11. Confirm every relevant document directory named in `.agent/OUTPUTS.md` still has an `AGENTS.md` file if the directory exists. If one is missing, add it in the same change.
12. Confirm the companion catalog notes for `.agent/refactor/AGENTS.md`, `.agent/skills/AGENTS.md`, and `.agent/styles/AGENTS.md` are still correct.
