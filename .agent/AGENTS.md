# Planning Guide Directory Map

This file is the map for the planning guides that live at the root of `.agent/`.

Start here if you are new to this codebase and need to:
- build a feature
- fix a problem
- refactor code
- review architecture
- add or change a style rule

Contributors must keep this file up to date.
If the planning toolkit changes, this file must change in the same update.

## What This Directory Is For

This directory holds guides for different kinds of work.

Each guide answers one main question.
The guides work together, but they are not the same.

Use this file first to answer:
- Which guide should I open?
- What will it produce?
- Where will that output go?
- What guide will I probably need next?

## Start Here

Use this quick chooser when you want the fastest path.

- Implement a feature:
  Start with `EXAMPLE_MAPPING.md` if the intended behaviour is still unclear.
  Continue with `BEHAVIOURS.md` once the intended behaviour is accepted.
  Use `PLANS.md` when the behaviour specification and proof path are already clear.
- Refactor safely:
  Start with `REFACTOR_PLANS.md`.
- Solve a failing behaviour:
  Start with `INVESTIGATION_LOGS.md`.
- Review architecture:
  Start with `ARCHITECTURE_REVIEW.md`.
- Add or revise a style rule:
  Start with `.agent/styles/AGENTS.md`, then use `CODE_STYLE_RULES.md`.

Simple rule:
- Unknown cause: investigate first.
- Known cause but unclear behaviour: map the behaviour first.
- Accepted behaviour but no proof-ready specification yet: write a behaviour specification.
- Clear behaviour specification and ready to change code: plan the implementation.
- Same behaviour, better structure: write a refactor plan.

## Important Path Rules

- The planning guides live directly under `.agent/`.
- Open these guides from `.agent/`.
- Use the exact guide paths shown in `Directory Layout` and `Pick the Right Guide`.

## Directory Layout

Current layout:

```text
.agent/
├── AGENTS.md
├── ADRS.md
├── ARCHITECTURE_REVIEW.md
├── BEHAVIOURS.md
├── CODE_STYLE_RULES.md
├── EXAMPLE_MAPPING.md
├── INVESTIGATION_LOGS.md
├── PLANS.md
└── REFACTOR_PLANS.md
```

At the time this file was written:
- these `9` Markdown files are tracked by this directory map
- `8` of them are planning guides
- the planning guides sit side by side at the root of `.agent/`

## Pick the Right Guide

### `ADRS.md`

- Answers: Why did we choose this option, and what tradeoffs come with it?
- Use it when: You need a lasting record of one important decision.
- Produces: One ADR for one decision.
- Output goes in: `docs/adrs/NNNN-short-decision-title.md`
- Next step: Usually back to implementation, refactoring, or architecture review work.
- Examples now: `docs/adrs/AGENTS.md` only; no finished ADRs are checked in at this path today
- Companion docs: `PLANS.md`, `REFACTORING.md`, `ARCHITECTURE_REVIEW.md`

### `ARCHITECTURE_REVIEW.md`

- Answers: Can this design hold up under load, failure, restart, and partial outage?
- Use it when: You are reviewing Elixir or OTP architecture for risk, reliability, or failure spread.
- Produces: One ArchitectureReview document.
- Output goes in: `docs/architecture_reviews/NNNN-short-title.md` according to the guide
- Next step: Usually an ADR, an ExecPlan, or a RefactoringPlan.
- Examples now: `docs/architecture_reviews/AGENTS.md` only; no finished architecture reviews are checked in at this path today
- Companion docs: `ADRS.md`, `PLANS.md`, `REFACTORING.md`

### `BEHAVIOURS.md`

- Answers: How do I turn accepted behaviour into a proof-ready behaviour specification?
- Use it when: You are turning accepted behaviour into a concrete spec and runnable proof path before implementation planning.
- Produces: One BehaviourSpecDoc, plus the spec and proof files it defines.
- Output goes in: `docs/behaviour_specs/NNNN-short-title.md`
- Next step: Usually `PLANS.md` when the behaviour specification is ready to implement.
- Examples now: None found in the working tree
- Companion docs: `EXAMPLE_MAPPING.md`, `PLANS.md`

### `CODE_STYLE_RULES.md`

- Answers: How do I write a new style-rule document?
- Use it when: You need to create or revise a style-rule document, not just find an existing rule.
- Produces: One code-style reference document and the written artifacts used to check it.
- Output goes in: `.agent/styles/<category>/...`, following `.agent/styles/AGENTS.md`
- Next step: Update `.agent/styles/AGENTS.md` so the style catalog stays correct.
- Examples now: `.agent/styles/ecto/Prefer Separate Steps When Building an Ecto Query.md`, `.agent/styles/code_related_anti_patterns/Complex Else Clauses in With.md`
- Companion docs: `.agent/styles/AGENTS.md`

### `EXAMPLE_MAPPING.md`

- Answers: What should happen at this visible boundary?
- Use it when: The boundary is known, but the intended behaviour is still unclear or disputed.
- Produces: One ExampleMappingDoc with rules, examples, blockers, and test mapping.
- Output goes in: `docs/example_maps/NNNN-short-title.md`
- Next step: Usually `BEHAVIOURS.md` when the behaviour is clear enough to turn into a proof-ready specification.
- Examples now: `docs/example_maps/README.md` only; no finished example maps are checked in today
- Companion docs: `INVESTIGATION_LOGS.md`, `PLANS.md`

### `INVESTIGATION_LOGS.md`

- Answers: What is failing, and what facts prove it?
- Use it when: A visible problem exists, but the cause is not yet proven.
- Produces: One InvestigationLog with failure, facts, interpretation, and next safe action.
- Output goes in: No fixed checked-in output folder is defined by the guide today.
- Next step: Usually `EXAMPLE_MAPPING.md`, `PLANS.md`, or `REFACTOR_PLANS.md`
- Examples now: None found in the working tree
- Companion docs: `EXAMPLE_MAPPING.md`, `PLANS.md`, `REFACTOR_PLANS.md`

### `PLANS.md`

- Answers: How do I implement a clear change from start to finish?
- Use it when: The desired behaviour and proof path are already clear enough to build and validate.
- Produces: One ExecPlan.
- Output goes in: `docs/plans/NNNN-short-title.md`
- Next step: Implementation work, and sometimes `ADRS.md` if the change creates a lasting decision.
- Examples now: `docs/plans/0001-common-filters-warning-no-raise.md`, `docs/plans/0002-thin-adapter-refactor.md`
- Companion docs: `INVESTIGATION_LOGS.md`, `EXAMPLE_MAPPING.md`, `ADRS.md`

### `REFACTOR_PLANS.md`

- Answers: How do I change structure without changing behaviour?
- Use it when: You need a safe refactor, not a new feature.
- Produces: One RefactorPlan.
- Output goes in: `docs/refactor_plans/NNNN-short-title.md`
- Next step: Refactor work, and sometimes `ADRS.md` if the refactor creates a lasting design decision.
- Examples now: `docs/refactor_plans/AGENTS.md` only; no finished refactor plans are checked in at this path today
- Companion docs: `.agent/refactor/AGENTS.md`, `ADRS.md`

## How the Main Guides Fit Together

### Main path

Use this path for most feature and bug-fix work:

1. `INVESTIGATION_LOGS.md`
   Use this when you do not yet know what is really wrong.
2. `EXAMPLE_MAPPING.md`
   Use this when you know the boundary, but you still need to make the expected behaviour clear.
3. `BEHAVIOURS.md`
   Use this when the behaviour is accepted and you need a proof-ready specification.
4. `PLANS.md`
   Use this when the behaviour specification is clear and you are ready to make the change.

Use the refactor branch instead when behaviour should stay the same:

1. `INVESTIGATION_LOGS.md` if the real problem is still unknown.
2. `REFACTOR_PLANS.md` when the job is to improve structure without changing behaviour.

### Special guides

- `ADRS.md`
  Use this when a choice needs a lasting record of why it was made.
- `ARCHITECTURE_REVIEW.md`
  Use this for system-level risk, failure, restart, and scale review.
- `BEHAVIOURS.md`
  Use this for proof-ready behaviour specification and runnable proof mapping.
- `CODE_STYLE_RULES.md`
  Use this to write a new style-rule document after you find the right category in `.agent/styles/AGENTS.md`.

## Current Output Locations and Examples

These are facts from the current working tree.
They are not new policy.

- `docs/plans/` exists and has checked-in ExecPlan examples.
- `docs/refactor_plans/` exists, but it only has a directory guide right now.
- `docs/example_maps/` exists, but it only has a README right now.
- `docs/behaviour_specs/` exists, but it only has a directory guide right now.
- `docs/architecture_reviews/` exists, but it only has a directory guide right now.
- `docs/adrs/` exists, but it only has a directory guide right now.
- InvestigationLogs do not have a fixed checked-in output folder today.
- BehaviourSpecDocs live in `docs/behaviour_specs/` according to `BEHAVIOURS.md`.
- Style-rule documents live in `.agent/styles/`, not alongside the planning guides at the root of `.agent/`.

## Companion Catalogs Outside This Directory

- `.agent/refactor/AGENTS.md`
  Use this with `REFACTOR_PLANS.md` to find code smells and refactor techniques.
- `.agent/styles/AGENTS.md`
  Use this before `CODE_STYLE_RULES.md` to find the right rule category and keep the style catalog up to date.

## Simple Terms

- Guide: a file that tells you how to do one kind of work.
- Artifact: the thing the guide produces, such as a plan, log, ADR, or in-code docs.
- Visible boundary: the nearest place where behaviour can be seen from the outside.
- Diagnosis: proving what is happening and why.
- Handoff: the point where one guide stops and another guide takes over.
- Validation: the checks that prove the result is correct.

## Contributor Maintenance Rules

Contributors must keep this file current.

- If a guide is added, removed, renamed, moved, or changed in a meaningful way, update this file in the same change.
- Keep the section order exactly as it is now.
- Keep the root-level `.agent/` paths as the canonical paths unless the real filesystem changes.
- Keep every guide listed in `Pick the Right Guide`.
- Keep the output locations accurate to both the guide text and the working tree.
- If a guide still has no fixed output location, say that clearly. Do not invent one here.
- If a new checked-in example appears, add it here.
- If a path warning becomes outdated because the repo was cleaned up, update the warning.
- Write for a beginner. Use short sentences and simple words.

## Contributor Maintenance Checklist

Use this checklist every time you update these planning guides or this file.

1. Confirm the current root-level `.agent/*.md` planning-guide file list from the working tree.
2. Confirm the planning guides still live directly under `.agent/`.
3. Confirm all nine current guides are listed in `Pick the Right Guide`.
4. Confirm the `Start Here` chooser still covers the main jobs people do in this repo.
5. Confirm each guide entry still says:
   what it answers,
   when to use it,
   what it produces,
   where the output goes,
   what comes next.
6. Confirm every output-location statement matches both the guide text and the filesystem.
7. Confirm missing conventions are still called out as missing.
8. Confirm the example paths listed here still exist.
9. Confirm the companion catalog notes for `.agent/refactor/AGENTS.md` and `.agent/styles/AGENTS.md` are still correct.
