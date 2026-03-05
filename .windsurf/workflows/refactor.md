---
description: Guides a safe, behavior-preserving refactor from smell identification through implementation using a RefactorPlan. Use this workflow when refactoring code to improve structure, reduce duplication, or address a code smell.
---

// turbo-all

## Goal

You will identify a code smell, select a refactoring technique, write a RefactorPlan, execute it milestone by milestone, and verify that observable behavior is preserved throughout.

## Requirements

- Read `.agent/REFACTOR_PLANS.md` and follow it to the _letter_ when writing the RefactorPlan.

- Read the files in `.agent/code-styles` and use those guidelines when writing code.

## Prerequisites

- You must be in the root directory of an Elixir Mix project.

- You must be able to run `mix test`, `mix credo --strict`, and `mix dialyzer`.

## What to do

1. Identify the smell.
   - Browse the code smell catalog in `.agent/refactor/code_smells/`. The categories are:
     - `bloaters/` - code that has grown too large to work with easily.
     - `change_preventers/` - code that makes changes difficult.
     - `couplers/` - code with excessive coupling between modules.
     - `dispensables/` - code that could be removed without loss.
     - `abstraction_abusers/` - patterns that misuse abstraction mechanisms.
   - Read the matching smell file. Use its signs, symptoms, and examples to confirm the diagnosis.

2. Select the technique.
   - Browse the technique catalog in `.agent/refactor/techniques/` to find the technique that addresses the smell.
   - Read the matching technique file. Confirm the technique preserves the existing behavior.

3. Write the RefactorPlan.
   - Create a new `.md` file at the path specified by the `document-artifacts` rule in `.windsurf/rules/document-artifacts.md`.
   - Follow the skeleton and all requirements in `.agent/REFACTOR_PLANS.md`.
   - The plan must be self-contained: a novice with only the plan and the working tree must be able to execute it end-to-end.
   - At minimum the plan must include:
     - **Purpose / Big Picture** - why the refactor matters.
     - **Behavior Boundary** - the exact observable behavior that must not change.
     - **Code Smell Identified** - the smell, where it appears, and a brief summary.
     - **Refactoring Technique Selected** - the technique and why it fits.
     - **Plan of Work** - the sequence of edits, naming files and functions precisely.
     - **Validation and Acceptance** - commands to run and expected outputs.
     - **Milestones** - independently verifiable steps.
     - **Progress** - checklist updated as work proceeds.
     - **Decision Log**, **Surprises & Discoveries**, **Outcomes & Retrospective** - living sections.

4. Execute the plan.
   - Work through each milestone in order.
   - After each milestone:
     1. Run `mix format` to ensure formatting is correct.
     2. Run `mix test` to confirm behavior is preserved.
     3. Update the **Progress** section in the RefactorPlan.
   - Do not skip validation between milestones.

5. Final validation.
   - Run the full quality check suite:
     - `mix credo --strict`
     - `mix dialyzer`
     - `mix test`
   - Fix any issues. Repeat until all three pass.

6. Close the plan.
   - Update the **Outcomes & Retrospective** section with a summary of what changed, what was preserved, and any follow-up opportunities.
