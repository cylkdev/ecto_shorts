# Rewrite `AGENTS.md` in the original prose-first style

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds. This document is the governing artifact for the task it describes. If the task later narrows to a specific boundary, test, or contract question, that later reasoning must still be recorded here unless the task is explicitly split into a separate ExecPlan.

This plan must be maintained in accordance with `.agent/PLANS.md`.

## Purpose / Big Picture

After this change, a zero-context contributor should be able to read `AGENTS.md` from top to bottom and quickly understand what EctoShorts is, which modules matter, when to use them, and where to verify live behavior, without the document losing the human-readable prose style of the original file. The visible outcome is a rewritten `AGENTS.md` that keeps the original voice and structure while carrying more practical guidance than the earlier short overview.

## In Scope

This plan covers rewriting `AGENTS.md` only.

It includes restoring the original prose-heavy tone and overall reading experience.

It includes preserving the most useful live guidance discovered during the earlier reference-style rewrite, as long as that guidance is re-expressed in a more natural voice.

It includes light navigation aids only where they materially improve findability for a zero-context reader.

## Out of Scope

This plan does not authorize edits to runtime code, tests, or module docs in `lib/`.

This plan does not authorize turning `AGENTS.md` into a full API reference, HexDocs replacement, or dense cheat sheet.

This plan does not authorize a repo-wide documentation sweep outside `AGENTS.md`.

## Progress

- [x] (2026-03-15 23:39Z) Re-read `.agent/RULES.md`, `.agent/PLANS.md`, `.agent/guides/MODULE_SPECIFICATIONS.md`, `.agent/guides/FUNCTION_SPECIFICATIONS.md`, and `.agent/guides/TESTING_PRINCIPLES.md` before implementation resumed.
- [x] (2026-03-15 23:40Z) Confirmed the current `AGENTS.md` is too segmented for the user’s requested style and that no repo-root ExecPlan for this AGENTS rewrite already exists.
- [x] (2026-03-15 23:41Z) Created this repo-root ExecPlan to govern the rewrite.
- [ ] Rewrite `AGENTS.md` so it matches the original prose-heavy style while preserving the most useful live guidance from the current draft.
- [ ] Review the final `AGENTS.md` against the style sample, repo evidence, and scope limits.

## Milestones

### Milestone 1: Replace the segmented reference shape with a prose-first guide

This milestone rewrites `AGENTS.md` so the opening once again reads like the original sample: a short introduction, a compact component list, a paragraph describing the typical `Actions -> CommonFilters -> Repo` flow, and a paragraph about configuration. The file should then continue in prose, using only a small number of headings and short lists where they actually help a newcomer navigate.

Completion means a reader can still find the major modules and proof surfaces quickly, but the experience feels like reading an expanded overview rather than scanning a handbook.

### Milestone 2: Review the rewritten file against the live repo and style target

This milestone validates the rewritten document by reading it as a beginner would. The check is not whether every live feature is listed; the check is whether the document sounds like the original sample, stays readable top to bottom, and still routes the reader to the right module and proof surface without drifting into unsupported claims.

Completion means the final document is prose-first, behavior-preserving in tone, grounded in live repo evidence, and limited to the agreed scope.

## Surprises & Discoveries

- Observation: The previous rewrite improved findability but changed the reading experience too much.
  Evidence: The user explicitly provided the original opening sample and asked that the style be followed so humans can read it easily.

- Observation: The most useful additions from the previous rewrite are still valid, but they need a different presentation.
  Evidence: The current draft already identifies the right module families, repo locations, and proof surfaces. The problem is not factual coverage; it is presentation density.

## Decision Log

- Decision: Treat the user-provided original sample as the style anchor for the rewrite.
  Rationale: The user explicitly asked for that writing style to be followed exactly in spirit, with only light navigation added.
  Date/Author: 2026-03-15 / Cascade

- Decision: Keep the strongest additions from the current draft, but rewrite them into prose instead of preserving the segmented reference layout.
  Rationale: The user wants the original reading experience back, not a loss of the useful repo knowledge already gathered.
  Date/Author: 2026-03-15 / Cascade

## Outcomes & Retrospective

Implementation is not complete yet.

The main lesson so far is that a documentation improvement can still be wrong if it changes the reading mode the user explicitly wanted to preserve. The revised rewrite must optimize for the original human-readable flow first and findability second.

## Context and Orientation

`AGENTS.md` sits at the repo root and functions as a fast orientation document for humans and coding agents. It is not the main README and it is not module documentation. Its job is to help someone dropped into the repository understand the project, identify the important public modules, and know where to verify live behavior before making changes.

The current `AGENTS.md` already contains accurate material gathered from `README.md`, `lib/ecto_shorts/actions.ex`, `lib/ecto_shorts/common_filters.ex`, the other main public modules under `lib/ecto_shorts/`, and the proof surfaces under `test/ecto_shorts/` and `examples/`. The rewrite should preserve the substance that came from those live sources, but it must present that substance in a form that feels like the original overview rather than a segmented reference document.

The key public modules that need to remain discoverable in `AGENTS.md` are `EctoShorts.Actions`, `EctoShorts.CommonFilters`, `EctoShorts.CommonChanges`, `EctoShorts.CommonSchema`, `EctoShorts.CommonParams`, `EctoShorts.CommonQuery`, `EctoShorts.DynamicBuilders`, and `EctoShorts.Testing`. The file should also still point a reader to the main proof surfaces in `README.md`, `test/ecto_shorts/`, and `examples/`.

## Module Specifications

### `AGENTS.md` as a repo-level navigation artifact

`AGENTS.md` is the repo-level orientation boundary for this task. A reader should use it when they need a short, human-readable explanation of what EctoShorts is, how the main public modules relate to one another, which module to start with for a task, and where to confirm live behavior. They should not use it as a substitute for module docs, tests, or the README when they need exhaustive detail.

The shared rule for this artifact is that it must stay accurate to the live repo surface while remaining concise and readable. It should name the main modules, representative function families where helpful, and the best proof surfaces, but it should not attempt to duplicate full module docs or exhaustively list every supported shape.

## Function Specifications

### `AGENTS.md` opening flow

The opening of `AGENTS.md` should accept a reader with no prior context and return a clear mental model of the project. It should explain what the library does, introduce the main components in a compact list, describe the normal `Actions -> CommonFilters -> Repo` workflow, and explain repo configuration and overrides.

The opening succeeds when a beginner can read it without needing to jump to another section to understand the basic shape of the library.

### `AGENTS.md` proof guidance

The closing guidance in `AGENTS.md` should tell the reader where to confirm live behavior when the overview is not enough. It should direct them to `README.md`, the key files in `lib/ecto_shorts/`, the main test directories under `test/ecto_shorts/`, and the runnable examples under `examples/`.

The guidance succeeds when a reader can answer “where do I verify this?” without being overwhelmed by a long catalog.

## Plan of Work

Rewrite `/Users/kurthogarth/Documents/GitHub/ecto_shorts/AGENTS.md` in one pass. Keep the current title and restore the original style of the introductory paragraphs and component list. Replace the heavily segmented sections with a smaller number of prose-first sections. Fold the useful content from the current draft into readable paragraphs, using short lists only when they materially improve clarity.

Keep the opening broad and familiar. Then expand `## Overview` so it explains not just the basic read and write story, but also where the lower-level modules fit, how a contributor should think about common query or workflow tasks, and where the repo’s main proof surfaces live. End with light navigation guidance that points readers to the right files without turning the document into a dense map.

After the rewrite, re-read the full file and tighten any section that sounds too reference-heavy, too list-heavy, or too unlike the original sample.

## Example Mappings

### Story: A zero-context reader can understand EctoShorts from `AGENTS.md`

The rewritten `AGENTS.md` should let a newcomer understand the library and find the right next file without feeling like they are reading generated reference material.

#### Rules:

- The file must preserve the original prose-first tone.
- The file must still mention the main public modules and their roles.
- The file must still tell the reader where to verify live behavior.
- The file must remain concise and readable top to bottom.

#### Examples:

A reader opens `AGENTS.md` and reads the introduction.
The reader understands that `EctoShorts.Actions` is the normal starting point for application-facing work.

A reader wants to inspect query construction without executing it.
The reader learns from `AGENTS.md` that `EctoShorts.CommonFilters` is the module to read next.

A reader needs stronger proof before changing behavior.
The reader learns from `AGENTS.md` to check `README.md`, `test/ecto_shorts/`, and `examples/`.

#### Open Questions:

- **Q:** Should the file preserve every detail from the current segmented rewrite? **A:** No. It should preserve the most useful live guidance, but not at the cost of the requested reading style.
- **Q:** Should the rewrite add more structure than the original file had? **A:** Yes, but only lightly, and only where it improves findability without making the document read like a reference sheet.

## Concrete Steps

From `/Users/kurthogarth/Documents/GitHub/ecto_shorts`, edit `AGENTS.md` only.

Read the current file once more as a style and content source. Replace the segmented sections with prose-first sections in the voice of the original sample. Keep the most important module, workflow, and proof-surface guidance. Then read the final file from top to bottom and trim anything that feels too dense or too mechanical.

## Validation and Acceptance

Validation for this task is primarily review rather than automated tests, because the change is documentation-only and does not alter runtime behavior.

The main review claim is: a beginner can read `AGENTS.md`, understand the project’s core shape, identify the right main module for common tasks, and know where to verify behavior next.

Acceptance is reached when the rewritten file both sounds like the original sample and remains grounded in the live repo surface already reviewed.

## Idempotence and Recovery

This task changes one documentation file. Re-running the rewrite is safe as long as each pass re-reads the current `AGENTS.md` and preserves the agreed style target.

If the rewrite becomes too segmented again, the recovery path is to convert those sections back into prose rather than adding more headings or lists.

## Artifacts and Notes

The draft planning artifact at `/Users/kurthogarth/.windsurf/plans/extend-agents-execution-manual-4b0a9b.md` captured the same task earlier. This repo-root ExecPlan supersedes that draft for implementation because repo rules require the governing artifact to live in `./plans`.

## Interfaces and Dependencies

This plan should touch only one repo file during implementation:

- `AGENTS.md`

The rewrite depends on the live project framing already established in these existing sources, which may be cited or reflected in prose but should not be edited as part of this task:

- `README.md`
- `lib/ecto_shorts/actions.ex`
- `lib/ecto_shorts/common_filters.ex`
- `lib/ecto_shorts/common_changes.ex`
- `lib/ecto_shorts/common_params.ex`
- `lib/ecto_shorts/common_schema.ex`
- `lib/ecto_shorts/common_query.ex`
- `lib/ecto_shorts/dynamic_builders.ex`
- `lib/ecto_shorts/testing.ex`
- `test/ecto_shorts/`
- `examples/`

Plan note (2026-03-15 23:41Z): This repo-root ExecPlan supersedes the draft under `.windsurf/plans` as the governing plan for the `AGENTS.md` rewrite because repo rules require the governing artifact to live in `./plans`.
