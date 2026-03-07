---
name: write-exec-plan
description: Guides the creation and execution of a self-contained ExecPlan for delivering a working feature or system change. Use this skill when implementing complex features or significant changes.
---

## When to use

Use this skill when you need to implement a complex feature, significant system change, or any work with multiple steps that benefits from a structured plan. Examples: adding a new module with behaviour contracts, integrating a third-party library, building a new API surface, or migrating an existing subsystem.

## When not to use

Do **not** use this skill for simple bug fixes, small refactors, or single-function changes. Use the `refactor` skill for behaviour-preserving refactors. Use the `write-adr` skill for architecture decision records. Use the `write-docs` skill for code-level documentation.

## What to do

Read `.agent/PLANS.md` and load it into your context. If it already exists in context, read it again to refresh your memory. Follow it exactly.

### 1. Understand the goal

State what the user can do after the change that they cannot do now. Phrase this as observable behaviour, not internal implementation.

### 2. Research the codebase

Identify the key files, modules, and functions involved. Map how they fit together. Note any existing patterns, conventions, or contracts that the new work must follow.

### 3. Write the ExecPlan

Create a new `.md` file at the path specified by the `document-artifacts` rule in `.windsurf/rules/document-artifacts.md`.

Follow the skeleton and all requirements in `.agent/PLANS.md`. The plan must be self-contained: a novice with only the plan and the working tree must be able to implement it end-to-end.

At minimum the plan must include:

- **Purpose / Big Picture** - what the user gains and how to see it working.
- **Context and Orientation** - key files, modules, and terms defined for a novice.
- **Plan of Work** - the sequence of edits, naming files and functions precisely.
- **Concrete Steps** - exact commands, working directories, and expected output.
- **Validation and Acceptance** - how to exercise the system and what to observe.
- **Interfaces and Dependencies** - modules, behaviours, and function signatures that must exist.
- **Milestones** - independently verifiable steps.
- **Progress** - checklist updated as work proceeds.
- **Decision Log**, **Surprises & Discoveries**, **Outcomes & Retrospective** - living sections.

### 4. Execute the plan

Work through each milestone in order. After each milestone:

1. Run `mix format` to ensure formatting is correct.
2. Run `mix test` to confirm the milestone works.
3. Update the **Progress** section in the plan.

Do not skip validation between milestones.

### 5. Final validation

Run the Code Check Workflow. Fix any issues. Repeat until all pass.

### 6. Close the plan

Update the **Outcomes & Retrospective** section with a summary of what was achieved, what remains, and lessons learned.
