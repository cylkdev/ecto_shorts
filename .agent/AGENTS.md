# Planning Directory

This file is the map for the planning guides that live at the root of `.agent/`. Treat the reader as a complete beginner to this repository: they have only the current working tree and this directory map. There is no memory of prior guide discussions and no external context.

## Purpose / Big Picture

Use this file when you need to decide which root `.agent` guide to open first, what that guide produces, where its output currently lives, and what guide or work state usually comes next.

This file does not replace the guides it points to. It helps a beginner choose the right one quickly and notice when the checked-in map, the companion guide text, and the current working tree no longer match.

## How to Use AGENTS.md

Start here before you open a planning guide. Use `Start Here` when you want the quickest path. Use `Pick the Right Guide` when you need the detailed answer for one guide. Use `How the Main Guides Fit Together` when the work will likely move across more than one guide.

Keep this file open while you work on the planning toolkit. If a guide is added, renamed, moved, or materially changed, update this map in the same change so a beginner can still restart from here.

## Start Here

Use this quick chooser when you want the fastest path through the planning toolkit.

- Build or change behaviour. Start with `EXAMPLE_MAPPING.md` if the intended behaviour is still unclear. Continue with `BEHAVIOURS.md` once the intended behaviour is accepted. Use `PLANS.md` when the proof path and implementation target are already clear.
- Refactor without intentionally changing behaviour. Start with `REFACTOR_PLANS.md`.
- Diagnose a visible problem. Start with `INVESTIGATION_LOGS.md`.
- Review architecture, risk, or failure spread. Start with `ARCHITECTURE_REVIEW.md`.
- Add or revise a style rule. Start with `.agent/styles/AGENTS.md`, then use `CODE_STYLE_RULES.md`.

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
├── EXAMPLE_MAPPING.md
├── INVESTIGATION_LOGS.md
├── PLANS.md
└── REFACTOR_PLANS.md
```

At the time this file was updated, this map covers nine Markdown files at the root of `.agent/`. Eight are planning guides. `AGENTS.md` is the directory map that points to them.

## Pick the Right Guide

Use these entries when you need the detailed answer for one guide instead of the fast chooser above. Each entry tells you what question the guide answers, when to use it, what it produces, where its output currently goes, what usually comes next, what examples exist now, and which companion docs matter.

### `ADRS.md`

Use this guide when a lasting architectural or design decision needs a durable record.

- Answers: Why was this option chosen, and what tradeoffs come with it?
- Use it when: One important decision needs to stay explicit over time.
- Produces: One `ADR` for one decision.
- Output goes in: `docs/adrs/NNNN-short-decision-title.md`.
- Next step: Usually back to implementation work, refactor work, or architecture review follow-up.
- Examples now: `docs/adrs/AGENTS.md` only. No finished ADRs are checked in at this path today.
- Companion docs: `PLANS.md`, `REFACTOR_PLANS.md`, `ARCHITECTURE_REVIEW.md`.

### `ARCHITECTURE_REVIEW.md`

Use this guide when the question is about system shape, risk, scaling, restart, or failure spread.

- Answers: Can this design hold up under load, failure, restart, and partial outage?
- Use it when: You are reviewing Elixir or OTP architecture for resilience, dependency risk, or failure behaviour.
- Produces: One `ArchitectureReview` document.
- Output goes in: `docs/architecture_reviews/NNNN-short-title.md`.
- Next step: Usually an `ADR`, an `ExecPlan`, or a `RefactorPlan`.
- Examples now: `docs/architecture_reviews/AGENTS.md` only. No finished architecture reviews are checked in at this path today.
- Companion docs: `ADRS.md`, `PLANS.md`, `REFACTOR_PLANS.md`.

### `BEHAVIOURS.md`

Use this guide when intended behaviour is accepted, but implementation should not start until the proof-ready specification is explicit.

- Answers: How do I turn accepted behaviour into a proof-ready behaviour specification?
- Use it when: The boundary is known, the intended behaviour is accepted, and the proof path still needs to be written down.
- Produces: One `BehaviourSpecDoc`.
- Output goes in: `docs/behaviour_specs/NNNN-short-title.md`.
- Next step: Usually `PLANS.md` when implementation planning should begin.
- Examples now: `docs/behaviour_specs/AGENTS.md` only. No finished behaviour specifications are checked in at this path today.
- Companion docs: `EXAMPLE_MAPPING.md`, `PLANS.md`.

### `CODE_STYLE_RULES.md`

Use this guide when the work is to create or revise a reusable style rule, not merely to find one.

- Answers: How do I write a new style-rule document?
- Use it when: You need to create or revise a style-rule document, not just find an existing rule.
- Produces: One code-style reference document and the supporting written artifacts it defines.
- Output goes in: `.agent/styles/<category>/...`, following `.agent/styles/AGENTS.md`.
- Next step: Update `.agent/styles/AGENTS.md` so the style catalog matches the working tree.
- Examples now: `.agent/styles/ecto/Prefer Separate Steps When Building an Ecto Query.md` and `.agent/styles/code_related_anti_patterns/Complex Else Clauses in With.md`.
- Companion docs: `.agent/styles/AGENTS.md`.

### `EXAMPLE_MAPPING.md`

Use this guide when the visible boundary is known, but the intended behaviour still needs to be clarified with examples and rules.

- Answers: What should happen at this visible boundary?
- Use it when: The boundary is known, but the intended behaviour is still unclear or disputed.
- Produces: One `ExampleMappingDoc`.
- Output goes in: `docs/example_maps/NNNN-short-title.md`.
- Next step: Usually `BEHAVIOURS.md` once the behaviour is clear enough to turn into a proof-ready specification.
- Examples now: `docs/example_maps/AGENTS.md` only. No finished example maps are checked in at this path today.
- Companion docs: `INVESTIGATION_LOGS.md`, `PLANS.md`.

### `INVESTIGATION_LOGS.md`

Use this guide when a visible problem exists, but its cause is not yet proven.

- Answers: What is failing, and what facts prove it?
- Use it when: You need diagnosis and evidence before you can safely clarify behaviour or change code.
- Produces: One `InvestigationLog`.
- Output goes in: `INVESTIGATION_LOGS.md` still says there is no fixed checked-in output folder today. The working tree also contains `docs/investigation_logs/AGENTS.md`, so the guide text and repository layout are currently out of sync.
- Next step: Usually `EXAMPLE_MAPPING.md`, `BEHAVIOURS.md`, `PLANS.md`, or `REFACTOR_PLANS.md`, depending on what the diagnosis resolves.
- Examples now: `docs/investigation_logs/AGENTS.md` only. No finished investigation logs are checked in at this path today.
- Companion docs: `EXAMPLE_MAPPING.md`, `BEHAVIOURS.md`, `PLANS.md`, `REFACTOR_PLANS.md`.

### `PLANS.md`

Use this guide when the desired behaviour and proof path are already clear enough to plan implementation directly.

- Answers: How do I implement a clear change from start to finish?
- Use it when: The change is behaviour-changing and the diagnosis, behaviour, and proof path are already explicit enough to implement.
- Produces: One `ExecPlan`.
- Output goes in: `PLANS.md` says completed ExecPlans belong under `docs/plans/NNNN-short-title.md`. The current working tree instead contains `docs/exec_plans/AGENTS.md` and no `docs/plans/` directory, so this output location is currently mismatched.
- Next step: Implementation work, and sometimes `ADRS.md` if the change creates a lasting decision.
- Examples now: `docs/exec_plans/AGENTS.md` only. The older `docs/plans/0001...` example paths cited in previous versions of this map are not present in the current working tree.
- Companion docs: `INVESTIGATION_LOGS.md`, `EXAMPLE_MAPPING.md`, `BEHAVIOURS.md`, `ADRS.md`.

### `REFACTOR_PLANS.md`

Use this guide when the job is to improve structure without intentionally changing observable behaviour.

- Answers: How do I change structure without changing behaviour?
- Use it when: You need a safe refactor rather than a new feature or bug-fix behaviour change.
- Produces: One `RefactorPlan`.
- Output goes in: `docs/refactor_plans/NNNN-short-title.md`.
- Next step: Refactor work, and sometimes `ADRS.md` if the refactor creates a lasting design decision.
- Examples now: `docs/refactor_plans/AGENTS.md` only. No finished refactor plans are checked in at this path today.
- Companion docs: `.agent/refactor/AGENTS.md`, `ADRS.md`.

## How the Main Guides Fit Together

Use this section when the work spans more than one guide. The planning guides are meant to hand work forward as uncertainty becomes smaller.

### Main path

Most feature work and bug-fix work follows the same shape. Start with diagnosis if the cause is not proven. Move to behaviour clarification when the boundary is known but the expected behaviour is still unclear. Move to proof-ready specification when the behaviour is accepted but the proof path is not yet explicit. Move to implementation planning when the proof path and change target are ready.

1. `INVESTIGATION_LOGS.md` when you do not yet know what is really wrong.
2. `EXAMPLE_MAPPING.md` when you know the boundary but still need to make the expected behaviour explicit.
3. `BEHAVIOURS.md` when the behaviour is accepted and needs a proof-ready specification.
4. `PLANS.md` when the behaviour specification is clear and you are ready to plan the code change.

### Refactor path

Use the refactor branch when the observable behaviour should stay the same.

1. `INVESTIGATION_LOGS.md` if the real problem is still unknown.
2. `REFACTOR_PLANS.md` when the goal is to improve structure without changing behaviour.

### Special guides

Use the special guides when the question is not part of the main feature or refactor flow.

- `ADRS.md` records why one lasting decision was made.
- `ARCHITECTURE_REVIEW.md` reviews system-level shape, failure spread, and operational risk.
- `CODE_STYLE_RULES.md` records reusable code-writing guidance after you have found the correct style category in `.agent/styles/AGENTS.md`.

## Current Output Locations and Examples

Use this section as the repository-fact view. It records what is checked in now, not what an older guide version may still say.

- `docs/adrs/`, `docs/architecture_reviews/`, `docs/behaviour_specs/`, `docs/example_maps/`, and `docs/refactor_plans/` exist and currently contain only their directory guides.
- `docs/exec_plans/` exists in the working tree, but `PLANS.md` still says completed ExecPlans belong under `docs/plans/`.
- `docs/investigation_logs/` exists in the working tree, but `INVESTIGATION_LOGS.md` still says there is no fixed checked-in output folder today.
- No finished checked-in examples currently exist for ADRs, architecture reviews, behaviour specs, example maps, investigation logs, exec plans, or refactor plans.
- Style-rule documents live under `.agent/styles/`, not alongside the root planning guides.

## Companion Catalogs Outside This Directory

Use these companion catalogs when the guide you chose depends on a checked-in sub-catalog.

- `.agent/refactor/AGENTS.md` helps you choose code smells and refactor techniques while working with `REFACTOR_PLANS.md`.
- `.agent/styles/AGENTS.md` helps you choose the correct style-rule category while working with `CODE_STYLE_RULES.md`.

## Simple Terms

Use these short definitions when the planning-guide vocabulary is still unfamiliar.

- Guide: A file that explains how to do one kind of work.
- Artifact: The document or output a guide produces.
- Visible boundary: The nearest place where behaviour can be observed from the outside.
- Diagnosis: Proving what is happening and why.
- Handoff: The point where one guide stops and another guide takes over.
- Validation: The checks that prove the result is correct.

## Contributor Maintenance Rules

This file is only useful if it stays aligned with both the root `.agent/` directory and the current working tree. Update it in the same change whenever the planning toolkit changes.

- If a guide is added, removed, renamed, moved, or materially changed, update this file in the same change.
- Keep the root-level `.agent/` guide paths accurate.
- Keep every root guide listed in both `Directory Layout` and `Pick the Right Guide`.
- Keep output-location notes aligned with both the companion guide text and the working tree. If they disagree, say so explicitly.
- If a document directory under `docs/` exists for one of these guide outputs, keep an `AGENTS.md` file in that directory. If the directory exists and is missing one, create it in the same change.
- Replace stale example paths as soon as the working tree changes.
- Keep the prose beginner-friendly, direct, and concrete.

## Contributor Maintenance Checklist

Use this checklist every time you update this file or any root planning guide.

1. Confirm the current root-level `.agent/*.md` file list from the working tree.
2. Confirm the planning guides still live directly under `.agent/`.
3. Confirm every root guide still appears in `Directory Layout` and `Pick the Right Guide`.
4. Confirm the `Start Here` chooser still matches the main jobs people do in this repository.
5. Confirm each guide entry still answers the same seven questions: what it answers, when to use it, what it produces, where output goes, what comes next, what examples exist, and which companion docs matter.
6. Confirm every named path and example path still exists, or is explicitly marked as a current mismatch.
7. Confirm output-location statements still match the companion guide text, or explicitly call out where the companion guide and working tree disagree.
8. Confirm every relevant document directory under `docs/` still has an `AGENTS.md` file. If one is missing, add it in the same change.
9. Confirm the companion catalog notes for `.agent/refactor/AGENTS.md` and `.agent/styles/AGENTS.md` are still correct.
