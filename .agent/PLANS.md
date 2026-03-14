# ExecPlans

This document describes the requirements for an execution plan ("ExecPlan"), a design document that a coding agent can follow to deliver a working feature or system change. Treat the reader as a complete beginner to this repository: they have only the current working tree and the single ExecPlan file you provide. There is no memory of prior plans and no external context.

## How to use ExecPlans and PLANS.md

When authoring an executable specification (ExecPlan), follow PLANS.md to the letter. Be thorough in reading and re-reading source material so the specification is accurate. When creating a spec, start from the skeleton and flesh it out as you do your research.

When implementing an executable specification (ExecPlan), do not prompt the user for next steps; simply proceed to the next milestone. Keep all sections up to date, add or split entries in the list at every stopping point to affirmatively state the progress made and next steps, and resolve ambiguities autonomously.

When discussing an executable specification (ExecPlan), record decisions in a log in the spec for posterity. It should be unambiguously clear why any change to the specification was made. ExecPlans are living documents, and it should always be possible to restart from only the ExecPlan and no other work.

When researching a design with challenging requirements or significant unknowns, use milestones to implement proof of concepts, toy implementations, or focused spikes that validate whether the proposal is feasible. Research deeply, and include prototypes when they help guide a fuller implementation.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:

* Every ExecPlan must be fully self-contained. Self-contained means that in its current form it contains all knowledge and instructions needed for a novice to succeed.
* Every ExecPlan is a living document. Contributors are required to revise it as progress is made, as discoveries occur, and as design decisions are finalized. Each revision must remain fully self-contained.
* Every ExecPlan must enable a complete novice to implement the feature end-to-end without prior knowledge of this repo.
* Every ExecPlan must produce a demonstrably working behavior, not merely code changes to meet a definition.
* Every ExecPlan must define every term of art in plain language or not use it.

Purpose and intent come first. Begin by explaining, in a few sentences, why the work matters from a user's perspective: what someone can do after this change that they could not do before, and how to see it working. Then guide the reader through the exact steps to achieve that outcome, including what to edit, what to run, and what they should observe.

The agent executing your plan can list files, read files, search, run the project, and run tests. It does not know any prior context and cannot infer what you meant from earlier milestones. Repeat any assumption you rely on. Do not point to external blogs or docs; if knowledge is required, embed it in the plan itself in your own words. If an ExecPlan builds upon a prior ExecPlan and that file is checked in, incorporate it by reference. If it is not, you must include all relevant context from that plan.

## Formatting

Format and envelope are simple and strict. Each ExecPlan must be one single fenced code block labeled as `md` that begins and ends with triple backticks. Do not nest additional triple-backtick code fences inside; when you need to show commands, transcripts, diffs, or code, present them as indented blocks within that single fence. Use indentation for clarity rather than code fences inside an ExecPlan to avoid prematurely closing the ExecPlan's code fence. Use two newlines after every heading, use `#` and `##` and so on, and use correct syntax for ordered and unordered lists.

When writing an ExecPlan to a Markdown (`.md`) file where the content of the file is only the single ExecPlan, omit the triple backticks.

Write in plain prose. Prefer sentences over lists. Avoid checklists, tables, and long enumerations unless brevity would obscure meaning. Checklists are permitted only in the `Progress` section, where they are mandatory. Narrative sections must remain prose-first.

## Guidelines

Self-containment and plain language are paramount. If you introduce a phrase that is not ordinary English, define it immediately and remind the reader how it manifests in this repository by naming the files, modules, commands, or runtime behavior where it appears. Do not say "as defined previously" or "according to the architecture doc." Include the needed explanation here, even if you repeat yourself.

Avoid common failure modes. Do not rely on undefined jargon. Do not describe the letter of a feature so narrowly that the resulting code compiles but does nothing meaningful. Do not outsource key decisions to the reader. When ambiguity exists, resolve it in the plan itself and explain why you chose that path. Err on the side of over-explaining user-visible effects and under-specifying incidental implementation details.

When correctness depends on internal flow, do not stop at the public story. Add the internal boundary contracts and the internal structure walkthrough that govern the touched private functions and modules. State which internal boundary owns each transformation, what each private handoff accepts and returns, which intermediate shapes are allowed, which are forbidden, and how the touched code works end to end. Use concrete examples so a reader can follow the internal path without reverse-engineering the implementation.

Anchor the plan with observable outcomes. State what the user can do after implementation, the commands to run, and the outputs they should see. Acceptance should be phrased as behavior a human can verify rather than internal attributes. If a change is internal, explain how its impact can still be demonstrated, for example by running tests that fail before and pass after, or by showing a small interactive example that uses the new behavior.

Specify repository context explicitly. Name files with full repository-relative paths, name functions and modules precisely, and describe where new files should be created. If touching multiple areas, include a short orientation paragraph that explains how those parts fit together so a novice can navigate confidently. When running commands, show the working directory and exact command line. When outcomes depend on environment, state the assumptions and provide alternatives when reasonable.

Be idempotent and safe. Write the steps so they can be run multiple times without causing damage or drift. If a step can fail halfway, include how to retry or adapt. If a migration or destructive operation is necessary, spell out backups or safe fallbacks. Prefer additive, testable changes that can be validated as you go.

Validation is not optional. Include instructions to run tests, to start the system if applicable, and to observe it doing something useful. Describe comprehensive testing for any new features or capabilities. Include expected outputs and error messages so a novice can tell success from failure. Where possible, show how to prove that the change is effective beyond compilation, for example through a small end-to-end scenario, a CLI invocation, an interactive Elixir session, or a command transcript. State the exact test commands appropriate to the project's toolchain and how to interpret their results.

Capture evidence. When your steps produce terminal output, short diffs, or logs, include them inside the single fenced block as indented examples. Keep them concise and focused on what proves success. If you need to include a patch, prefer file-scoped diffs or small excerpts that a reader can recreate by following your instructions rather than pasting large blobs.

## Milestones

Milestones are narrative, not bureaucracy. If you break the work into milestones, introduce each with a brief paragraph that describes the scope, what will exist at the end of the milestone that did not exist before, the commands to run, and the acceptance you expect to observe. Keep it readable as a story: goal, work, result, proof. Progress and milestones are distinct: milestones tell the story, progress tracks granular work. Both must exist. Never abbreviate a milestone merely for the sake of brevity, and do not leave out details that could be crucial to a future implementation.

Each milestone must be independently verifiable and incrementally implement the overall goal of the execution plan.

## Living plans and design decisions

* ExecPlans are living documents. As you make key design decisions, update the plan to record both the decision and the thinking behind it. Record all decisions in the `Decision Log` section.
* Each repo-tracked code task has one governing ExecPlan. As the task reveals narrower questions, corrections, discoveries, or subproblems, update that governing ExecPlan instead of creating a competing planning artifact. Create a separate ExecPlan only when the work has explicitly become a separate task with a separate scope.
* ExecPlans must contain and maintain a `Progress` section, a `Surprises & Discoveries` section, a `Decision Log`, and an `Outcomes & Retrospective` section. These are not optional.
* When you discover optimizer behavior, performance tradeoffs, unexpected bugs, or semantics that shape your approach, capture those observations in the `Surprises & Discoveries` section with short evidence snippets. Test output is ideal.
* If you change course mid-implementation, document why in the `Decision Log` and reflect the implications in `Progress`. Plans are guides for the next contributor as much as checklists for you.
* At completion of a major task or the full plan, write an `Outcomes & Retrospective` entry summarizing what was achieved, what remains, and lessons learned.

## Prototyping milestones and parallel implementations

It is acceptable, and often encouraged, to include explicit prototyping milestones when they de-risk a larger change. Keep prototypes additive and testable. Clearly label the scope as prototyping, describe how to run and observe results, and state the criteria for promoting or discarding the prototype.

Prefer additive code changes followed by subtractions that keep tests passing. Parallel implementations are fine when they reduce risk or enable tests to continue passing during a larger migration. Describe how to validate both paths and how to retire one safely with tests. When working with multiple new libraries or feature areas, consider creating spikes that evaluate feasibility independently, proving that the dependency or feature performs as expected in isolation.

## Skeleton of a Good ExecPlan

    # <Short, action-oriented description>

    This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds. This document is the governing artifact for the task it describes. If the task later narrows to a specific boundary, test, or contract question, that later reasoning must still be recorded here unless the task is explicitly split into a separate ExecPlan.

    If PLANS.md file is checked into the repo, reference the path to that file here from the repository root and note that this document must be maintained in accordance with PLANS.md.

    ## Purpose / Big Picture

    Explain in a few sentences what someone gains after this change and how they can see it working. State the user-visible behavior you will enable.

    ## In Scope

    Use this section to define the exact work required for the plan to be considered complete. Its presence is an execution trigger: every item listed here must be implemented, validated, and reflected in the proof described elsewhere in the plan.

    Include only the changes that are necessary to deliver the feature within the current system boundary. Each item should describe required work, not background context, so that different implementers would make materially the same changes.

    ## Out of Scope

    Use this section to define the changes that must not be included while executing the plan. Its presence is an execution trigger: items listed here are explicitly excluded, even if they seem related or convenient during implementation.

    Include nearby or tempting changes that could expand the work without being necessary to deliver the planned behavior. This section exists to prevent scope drift and to make clear which follow-on work requires a separate plan or an explicit update to this one.

    ## Progress

    Use a list with checkboxes to summarize granular steps. Every stopping point must be documented here, even if it requires splitting a partially completed task into two (“done” vs. “remaining”). This section must always reflect the actual current state of the work.

        - [x] (2025-10-01 13:00Z) Example completed step.
        - [ ] Example incomplete step.
        - [ ] Example partially completed step (completed: X; remaining: Y).

    Use timestamps to measure rates of progress.

    ## Milestones

    Use Milestones for non-trivial work that needs more than one meaningful checkpoint to complete. A milestone is a concrete outcome that drives execution by stating what will be delivered, how the system or project state should differ when that stage is done, and what completion criteria will confirm it.

    Break the work into ordered milestones, then into tasks and subtasks within each milestone. This should make the sequence of work, dependencies, progress made, and work still remaining clear enough that someone can continue from the plan alone. Each milestone should be specific enough to guide execution from start to finish, and the plan should be kept current as the work changes so completed progress, material decisions, and the next milestone are always explicit. Finishing one milestone means proceeding to the next unless a blocker is recorded. When the path is uncertain, milestones should also be used to structure investigation, validation, and prototype work that establishes feasibility before full implementation.

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

    ## Internal Boundary Contracts

    Use this section when the work changes, preserves, or depends on private helper boundaries, private module handoffs, staged normalization, decomposition, translation, or other internal data-shape flow. For each touched boundary, name the upstream caller, the boundary itself, the accepted input shape, the produced output shape, the invariants preserved, the transformations owned there, and the transformations that do not belong there. If a boundary must not accept an incidental intermediate shape, say that directly. Include concrete examples at each important handoff. Show the value or structure as it arrives, what the boundary may do to it, and what leaves the boundary.

    ## Internal Structure Walkthrough

    Use this section when the work depends on how private pieces collaborate. Walk through the touched code path end to end in execution order. Name each function or module in the path, what it receives, what it changes, what it leaves alone, and why that step exists. Show the main success path and any important omitted-input, invalid-input, or preserved-behavior path where internal ownership matters. Make clear which structure is stable and which intermediate forms are incidental and must not become de facto contracts.

    ## Example Mappings

    Use an Example Mapping when the work includes at least one observable rule, example, or unresolved decision that can be written before implementation. If the feature or change defines required behavior, behavior when input is omitted, behavior for invalid input, concrete call-and-result examples, or questions that still need an answer, capture it in an Example Mapping.

    Produce exactly four sections in this order and keep the headings in this format: `### Story: <short title>`, a one or two sentence description of the requested behavior; `#### Rules:`, a bullet list of required behaviors, unchanged behavior when input is omitted, and explicit handling of invalid input; `#### Examples:`, concrete input/output pairs where each example shows the function call first and the exact expected result immediately after it, including at least one valid case, one omitted-input case, and one invalid-input case; and #### Open Questions:, a bullet list written as `**Q:** ... **A:** ...`, answering known decisions directly and using `...` only when the answer is still unknown.

    Rules are the acceptance criteria, examples are the executable expectations, and open questions are follow-up tasks. The mapping is complete only if a reader with no additional context can identify the public entry point, the accepted inputs, the exact expected outputs or errors, and any remaining decisions from the mapping alone.
    Use this section for caller-visible behavior. Do not use it as a substitute for internal handoff examples. Put private-boundary examples in `Internal Boundary Contracts` and `Internal Structure Walkthrough`.

    ## Behaviour Specifications

    Use Behaviour Specifications when the work includes at least one observable outcome at a public interface. If the change affects accepted input, returned values, persisted data, rendered output, error handling, or preservation of existing behavior, capture it here and treat it as a source of truth for completion.

    Write each specification from the caller's perspective, not in terms of internal modules, implementation steps, or query mechanics. Start each feature with `### Feature: <short title>`, then describe behavior with `Scenario` or `Scenario Outline` blocks using `Given`, `When`, and `Then`. Use `Scenario Outline` with an `Examples` table when the same rule must hold across multiple inputs. Include the success path, behavior when optional input is omitted, invalid-input behavior, and any required unchanged behavior.
    Do not carry private helper contracts, internal module handoffs, or code-shape walkthroughs in this section. Keep caller-visible behavior here and put internal collaboration rules in the internal sections.

    Each Then must describe an exact outcome that can be verified by an ExUnit assertion, command output, or direct observation. If a statement cannot be checked that way, rewrite it until it can.

    ## Executable Tests

    Use Executable Tests when the plan includes at least one observable behavior that can be expressed as an ExUnit assertion against real project code. This section is the instruction to create, update, or adopt runnable tests and to treat those tests as part of the completion criteria. If a rule says a function returns a specific value or tuple, filters a result set, persists a change, leaves existing behavior unchanged when input is omitted, or rejects invalid input, it belongs in this section.

    Executable tests are the behavioral proof for the plan. They must be real ExUnit tests in this codebase, not examples or pseudocode. Each test should prove one rule with the minimum required setup, call the real public API or entry point, and assert the exact promised outcome. Replace placeholders with actual modules, functions, schemas, fixtures, factories, and test helpers from the project without changing the behavior being proved. Use idiomatic Elixir and ExUnit conventions: define a real test module, use the appropriate case such as DataCase, name tests by behavior, keep setup focused, and use precise assertions and pattern matching. The work is not complete until each behavior claimed in this section is covered by runnable tests in this project.

    ## Concrete Steps

    State the exact commands to run and where to run them (working directory). When a command generates output, show a short expected transcript so the reader can compare. This section must be updated as work proceeds.

    ## Validation and Acceptance

    Describe how to start or exercise the system and what to observe. Phrase acceptance as behavior, with specific inputs and outputs. If tests are involved, say "run <project’s test command> and expect <N> passed; the new test <name> fails before the change and passes after>".

    ## Idempotence and Recovery

    If steps can be repeated safely, say so. If a step is risky, provide a safe retry or rollback path. Keep the environment clean after completion.

    ## Artifacts and Notes

    Include the most important transcripts, diffs, or snippets as indented examples. Keep them concise and focused on what proves success.

    ## Interfaces and Dependencies

    Be prescriptive. Name the libraries, modules, and services to use and why. Specify the types, behaviours, and function signatures that must exist at the end of the milestone. Prefer stable names and paths such as `AppName.ModuleName.function/3` or `AppName.ModuleName.Behaviour`.

If you follow the guidance above, a single, stateless agent, or a human novice, can read your ExecPlan from top to bottom and produce a working, observable result. That is the bar: self-contained, self-sufficient, novice-guiding, and outcome-focused.

When you revise a plan, ensure your changes are comprehensively reflected across all sections, including the living document sections, and write a note at the bottom of the plan describing the change and the reason why. ExecPlans must describe not just the what but the why for almost everything.
