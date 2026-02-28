---
name: write-adr
description: Runs the project's ADR workflow after an architecture decision by creating a new MADR file, filling it using `.agent/ADRS.md`, and adding validation and consequences so the decision is reviewable and enforceable. Use this skill after implementing a feature or making a change to the codebase.
---

## When to use

Use this skill when you make or discover an architecture decision - a choice between two or more plausible options that changes how the system is built or operated. Examples: choosing a library, defining a data model boundary, selecting a concurrency strategy, or establishing a module contract.

## When not to use

Do **not** use this skill for code-level documentation such as `@moduledoc`, `@doc`, or `@typedoc`. Use the `write-docs` skill for that.

Do **not** create an ADR for bug fixes or small refactors with no lasting architectural impact.

## What to do

Read `.agent/ADRS.md` and load it into your context. If it already exists in context, read it again to refresh your memory. Follow it exactly.

### 1. Decide if an ADR is required

An ADR is required if you are choosing between two or more plausible options and the choice changes how the system is built or operated. If the change is purely cosmetic, a bug fix, or a small refactor with no lasting architectural impact, skip the ADR.

### 2. Create a new ADR file

Create a new `.md` file at the path specified by the `document-artifacts` rule in `.windsurf/rules/document-artifacts.md`. Pick the next unused zero-padded sequence number in the target subdirectory.

### 3. Fill in the MADR skeleton

Copy the skeleton from `.agent/ADRS.md` into the new file. Fill in every section that applies. Keep the ADR self-contained and understandable to a novice who only has the repository and this ADR.

### 4. Make the decision testable

In "Validation", include at least one concrete way to confirm the decision was applied correctly. Prefer a command to run, a test to run, or an observable runtime behaviour.

### 5. Ensure the ADR is reviewable

- In "Decision Drivers", state the constraints and quality goals that actually mattered.
- In "Considered Options", list only realistic options and put the chosen option first.
- In "Decision Outcome", justify the choice by directly referencing the decision drivers.
- In "Consequences", state at least one good consequence and one bad consequence.

### 6. Set the status correctly

Use `proposed` if it is not yet agreed. Use `accepted` once it is agreed and should be followed. If the decision is replaced, create a new ADR and update the old ADR status to `superseded by NNNN`.

### 7. Record assumptions

If you made assumptions because repository context was missing, state them in "More Information" as explicit assumptions.