 # <Short, action-oriented description>

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

If PLANS.md file is checked into the repo, reference the path to that file here from the repository root and note that this document must be maintained in accordance with PLANS.md.

## Purpose / Big Picture

Explain in a few sentences what someone gains after this change and how they can see it working. State the user-visible behavior you will enable.

## Status

Set a status to reflect the current state of the plan. The status can be one of:

- **Proposed:** The work is documented as a proposal, but has not yet begun.
- **In Progress:** The work has started and is currently underway.
- **Complete:** The planned work has been fully finished with nothing remaining.
- **Abandoned:** The work was stopped before completion and will not continue.

## Progress

Use a list with checkboxes to summarize granular steps. Every stopping point must be documented here, even if it requires splitting a partially completed task into two (“done” vs. “remaining”). This section must always reflect the actual current state of the work.

Include an item to update this ExecPlan after each task.
Include an item to update to update document artifacts after each task.

Use `[ ]` to indicate a task been completed.
Use `[~]` to indicate a task is in progress.
Use `[x]` to indicate a task has been completed.

Example:

  - [x] (2025-10-01 13:00Z) Example completed step.
  - [ ] Example incomplete step.
  - [ ] Example partially completed step (completed: X; remaining: Y).

Use timestamps so a future contributor can see the sequence of work.

## Milestones

Use milestones for non-trivial work.

A milestone is a concrete outcome that drives execution. It should make clear what will be delivered, how the system should behave when the work is done, and what completion criteria will be used to confirm it is finished.

Use milestones to break the work into ordered tasks and subtasks. This creates a shared understanding of what must happen first, what depends on earlier work, and what still remains. Each milestone should be specific enough to guide implementation from start to finish.

For each milestone:

- Name the outcome.
- Describe the expected end-to-end behavior.
- State the completion criteria.
- List the tasks required to reach that outcome.
- Split large tasks into subtasks when needed so progress and stopping points are clear.

Example:

    ### Milestone 1: Define email verification behavior

    This milestone defines the expected signup and verification behavior before implementation begins. It is complete when the required user flow, edge cases, and completion criteria are documented.

    #### Task 1.1: Confirm signup and verification behavior

    - Determine when the verification email is sent.
    - Determine whether unverified users can sign in.
    - Determine what happens when a verification link expires.
    - Determine how resend verification works.

    #### Task 1.2: Review affected systems

    - Review the authentication flow.
    - Review the email delivery integration.
    - Review the user model fields related to account state.
    - Review the routes and pages involved in signup and activation.

    ### Milestone 2: Implement backend support for email verification

    This milestone adds the backend behavior required to support verification. It is complete when the system can create verification tokens, store them, send verification emails, and validate verification requests correctly.

    #### Task 2.1: Add data model support

    - Add email verification status.
    - Add token or token reference storage.
    - Add token expiry storage.

    #### Task 2.2: Implement token lifecycle

    - Generate secure verification tokens.
    - Store tokens safely.
    - Set expiration times.
    - Connect token creation to signup.

    #### Task 2.3: Send verification emails

    - Create the email content.
    - Add the verification link.
    - Send the email after signup.
    - Verify delivery in development.

## Surprises & Discoveries

Document unexpected behaviors, bugs, optimizations, or insights discovered during implementation. Provide concise evidence.

- Observation: …
  Evidence: …

## Decision Log

Record every decision made while working on the plan in the format:

- Decision: …
  Rationale: …
  Date/Author: …

## Outcomes & Retrospective

Summarize outcomes, gaps, and lessons learned at major milestones or at completion. Compare the result against the original purpose.

## Context and Orientation

Describe the current state relevant to this task as if the reader knows nothing. Name the key files and modules by full path. Define any non-obvious term you will use. Do not refer to prior plans.

## Plan of Work

Describe, in prose, the sequence of edits and additions. For each edit, name the file and location (function, module) and what to insert or change. Keep it concrete and minimal.

## Concrete Steps

State the exact commands to run and where to run them (working directory). When a command generates output, show a short expected transcript so the reader can compare. This section must be updated as work proceeds.

## Validation and Acceptance

Describe how to start or exercise the system and what to observe. Phrase acceptance as behavior, with specific inputs and outputs. If tests are involved, say "run <project’s test command> and expect <N> passed; the new test <name> fails before the change and passes after>".

## Idempotence and Recovery

If steps can be repeated safely, say so. If a step is risky, provide a safe retry or rollback path. Keep the environment clean after completion.

## Artifacts and Notes

Include the most important transcripts, diffs, or snippets as indented examples. Keep them concise and focused on what proves success.

## Interfaces and Dependencies

Be explicit and opinionated. Name the Hex packages, OTP applications, modules, and external services to use, and explain why each one is required. Specify the structs, behaviours, callbacks, and function signatures that must exist by the end of the milestone. Prefer stable, descriptive names such as AppName.ModuleName.function/3 or AppName.ModuleName.Behaviour.

For example:

In module `AppName.ModuleName` (at path `lib/app_name/module_name.ex`), define:

    defmodule AppName.ModuleName do
      @callback plan(observed :: AppName.Observed.t()) :: [AppName.Action.t()]
    end

## Example Mappings

Use Example Mapping to quickly define what a feature or change should do, spot missing details, and turn the results into clear next steps: rules become acceptance criteria, examples become test cases, and open questions become follow-up tasks. One story can usually be mapped in minutes.

Example:

    ### Story: <Short action-oriented description>

    Create a function `add/2` that adds two numbers.

    **Rules:**

    - Returns the sum of two numbers.
    - Accepts arguments: `add(integer() | float(), integer() | float())`
    - Returns: `integer() | float()`
    - Accept integers or floats for both inputs, and return the numeric sum.

    **Examples:**

        AppName.Calculator.add(1, 2)
        3

        AppName.Calculator.add(0, 5)
        5

        AppName.Calculator.add(-1, 1)
        0

        AppName.Calculator.add(2.5, 1.5)
        4.0

    **Open Questions:**

    - **Q:** Should it allow both integers and floats? **A:** ...
    - **Q:** What should happen if someone passes a string like "2"? **A:** ...
    - **Q:** Should invalid input raise an error or return something else? **A:** ...
    - **Q:** Do we need to handle very large numbers? **A:** ...

## Behaviour Specifications

...

## Executable Tests

...

## In Scope

...

## Out of Scope

...