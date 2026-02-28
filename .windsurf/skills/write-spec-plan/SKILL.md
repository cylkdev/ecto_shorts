---
name: write-spec-plan
description: Guides the creation of a self-contained SpecPlan for producing behaviour specifications with runnable proofs. Use this skill when you need to define and automate behaviour specs before or during implementation.
---

## When to use

Use this skill when you need to write behaviour specifications — example-driven scenarios that define what the system must do from a user-observable point of view. Use it when you want to define Given/When/Then scenarios, map them to runnable tests, and prove the behaviour end-to-end.

## When not to use

Do **not** use this skill for implementing features directly. Use the `write-exec-plan` skill for that. Do **not** use this skill for clarifying requirements through Example Mapping without automation. Use the `write-example-map-plan` skill for that. Do **not** use this skill for behaviour-preserving refactors. Use the `refactor` skill for that.

## What to do

Read `.agent/SPEC_PLANS.md` and load it into your context. If it already exists in context, read it again to refresh your memory. Follow it exactly.

### 1. Understand the behaviour

State what the user can do and observe. Phrase it as user-visible behaviour, not internal implementation.

### 2. Research the codebase

Identify where behaviour specs and tests live in this repo. Note existing tooling, test conventions, step definition patterns, and shared fixtures.

### 3. Write the SpecPlan

Create a new `.md` file at the path specified by the `document-artifacts` rule in `.windsurf/rules/document-artifacts.md`.

Follow the skeleton and all requirements in `.agent/SPEC_PLANS.md`. The plan must be self-contained.

At minimum the plan must include:

- **Purpose / Big Picture** — what the user gains and how to see it working.
- **Context and Orientation** — key files, modules, and terms defined for a novice.
- **Behaviour Vocabulary** — domain terms with clear definitions mapped to system data.
- **Behaviour Spec Draft** — the initial spec in the target format with concrete example values.
- **Traceability Map** — spec files, test files, step definitions, and shared fixtures.
- **Validation and Acceptance** — exact commands and expected outputs.
- **Milestones** — independently verifiable steps.
- **Progress** — checklist updated as work proceeds.
- **Decision Log**, **Surprises & Discoveries**, **Outcomes & Retrospective** — living sections.

### 4. Draft the behaviour spec

Write scenarios using domain language. Each scenario must describe one outcome with concrete inputs and expected observable results. Cover the main behaviour and the most important failure mode.

### 5. Implement the proof

Write or update step definitions, fixtures, and test files so the spec is runnable. Each scenario must fail before the feature is implemented and pass after.

### 6. Validate end-to-end

Run the spec. Confirm every scenario passes. Run the Code Check Workflow. Fix any issues. Repeat until all pass.

### 7. Close the plan

Update the **Outcomes & Retrospective** section with a summary of what was specified, what was proven, and any remaining gaps.
