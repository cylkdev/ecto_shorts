# BehaviourSpecifications (BehaviourSpecs)

This document describes the requirements for a behaviour specification (“BehaviourSpec”), an implementation and verification contract that a coding agent can follow to deliver one working, user-visible behaviour.

Treat the reader as a complete beginner to this repository: they have only the current working tree and the single BehaviourSpec file you provide. There is no memory of prior specs and no external context.

A BehaviourSpec is not a brainstorming document. It is not a place to discover what the feature means. A BehaviourSpec starts after the behaviour meaning is already clear enough to specify one boundary, one primary action, and one observable outcome.

If the behaviour is still ambiguous, stop and create or update an ExampleDoc first. Do not hide unresolved behaviour questions inside implementation steps.

## Boundary of Responsibility

Use a BehaviourSpec when the work is ready to be implemented and verified end-to-end.

A BehaviourSpec defines:

- the user-visible boundary where the behaviour is exercised
- the starting conditions required for a deterministic run
- the primary action at that boundary
- the expected observable outcome
- optional failure outcomes at the same boundary
- the commands and tests used to prove the behaviour works

A BehaviourSpec does not replace an ExampleDoc. If domain rules are still undecided, resolve them in an ExampleDoc and then return to the BehaviourSpec.

## Relationship to ExampleDocs

A BehaviourSpec may be written directly when behaviour is already clear. When the behaviour was clarified in an ExampleDoc, reference that ExampleDoc and carry forward only the finalized behaviour decisions.

Do not copy unresolved questions from an ExampleDoc into a BehaviourSpec as if they were implementation tasks. A BehaviourSpec must describe a contract that can be implemented without guessing.

## How to use BehaviourSpecs and BEHAVIOURS.md

When authoring a BehaviourSpec, start from the skeleton at the bottom and flesh it out as you research the codebase. Define the boundary first, then write the behaviour and scenarios, then write validation and milestones.

When implementing from a BehaviourSpec, do not rely on implied meaning. If a phrase can be interpreted more than one way, tighten the spec before coding. Implement in small additive steps and keep the behaviour provable at all times.

When discussing or revising a BehaviourSpec, treat it as a shared execution contract. Record boundary decisions, response shape decisions, and validation decisions in the `Decision Log` so the spec remains restartable and self-contained.

When a feature contains significant technical unknowns, include a small prototype milestone that de-risks implementation. Prototypes must still be runnable and must state the promotion/discard criteria.

## Formatting

Format and envelope are simple and strict. Each BehaviourSpec must be inside a single fenced code block labeled as `md` that begins and ends with triple backticks. Do not nest additional triple-backtick fences inside. When you need to show commands, payloads, responses, logs, or code snippets, include them as indented blocks inside the single fence.

When writing a BehaviourSpec to a Markdown (`.md`) file where the file contains only the BehaviourSpec, omit the outer triple backticks.

Write in plain prose. Prefer sentences over lists. Avoid checklists except in `Progress` and `Validation and Acceptance`, where they are mandatory. Narrative sections must remain prose-first.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:

* Every BehaviourSpec must be fully self-contained. Self-contained means that, in its current form, it contains all knowledge and instructions needed for a novice to implement and verify the behaviour.
* Every BehaviourSpec is a living document. Contributors must revise it as progress is made, discoveries occur, and decisions are finalized. Each revision must remain fully self-contained.
* Every BehaviourSpec must define exactly what a user or external observer can do and observe at a specific boundary.
* Every BehaviourSpec must produce a demonstrably working behaviour, not merely code changes that compile.
* Every BehaviourSpec must define every term of art in plain language at first use, or avoid the term.
* Every BehaviourSpec must be test-shaped: the expected outcome must be expressible as deterministic assertions.
* Every BehaviourSpec must include validation instructions that a novice can run.

Purpose and intent come first. Begin by explaining what someone gains after the change and how they can see it working. Then specify the behaviour contract at the boundary and how to prove it.

## Guidelines

Anchor the spec on an observable boundary. In Elixir and Phoenix, the most stable boundaries are usually an HTTP endpoint, a LiveView event, a CLI command (`mix ...`), or an explicit message/event contract. Pick the boundary that a novice can exercise and that ExUnit can assert deterministically.

Write one behaviour at a time. One behaviour means one primary action leading to one primary observable outcome. If you have two different actions or two unrelated outcomes, split the work into multiple BehaviourSpecs or multiple scenarios only if they are the same capability at the same boundary.

Make ambiguity impossible. Do not say “returns an error” without naming the exact observable error outcome. Do not say “shows results” without naming what is rendered or returned. Tighten all vague words until a novice can implement the behaviour without interpretation.

Make starting conditions explicit. If data must exist, say exactly what data and how it exists for the scenario. If authentication is required, define exactly what “signed in” means at the boundary. If configuration matters, state what config is set and how to set it.

Keep the spec test-shaped. Phrase outcomes as conditions that can be asserted directly: status code, response body shape, rendered text, redirect path, emitted message shape, persisted row existence, or another measurable side effect observable at the boundary.

Be idempotent and safe. Write steps that can be repeated safely where possible. If a step is stateful or risky, include a safe retry or rollback path.

Validation is not optional. A BehaviourSpec must tell a novice what commands to run, what success looks like, and how to tell the difference between “working” and “not working.”

Capture evidence. When steps produce output, include short transcripts or snippets that prove success. Keep evidence concise and reproducible.

## Milestones

Milestones are narrative, not bureaucracy. If you break the implementation into milestones, introduce each milestone with a short paragraph that states the goal, the scope, and what new observable behaviour or proof will exist at the end.

Each milestone must state:

- the exact commands to run
- the expected acceptance outcome
- how the milestone advances the behaviour contract

Milestones must be independently verifiable. A milestone that cannot be verified at the boundary is not a milestone; it is a note.

Progress and milestones are distinct. Milestones tell the implementation story. `Progress` tracks the granular work actually performed.

## Living documents and design decisions

BehaviourSpecs are living documents. As you make key implementation-facing design decisions that affect the boundary contract or verification strategy, record them in the `Decision Log` with the reasoning.

BehaviourSpecs must contain and maintain a `Progress` section, a `Surprises & Discoveries` section, a `Decision Log` section, and an `Outcomes & Retrospective` section. These sections are not optional.

When you discover hidden constraints (framework behaviour, database constraints, boundary limitations, test harness limitations), record them in `Surprises & Discoveries` with short evidence a novice can reproduce.

If you change course, update the relevant scenario text, milestones, and validation instructions so the BehaviourSpec does not contain stale behaviour meanings.

## Prototyping milestones and parallel implementations

It is acceptable, and often encouraged, to include explicit prototyping milestones when they de-risk a larger implementation. Examples include validating a LiveView event contract in an isolated test, confirming a JSON error shape in a controller test, or proving that an Ecto constraint maps to a required boundary error outcome.

Keep prototypes additive and testable. Label the milestone as “prototyping.” State exactly how to run it, what to observe, and the criteria for promoting or discarding it.

Parallel implementations (for example, running an old response shape and a new response shape behind a feature flag during migration) are acceptable when they reduce risk. If you do this, specify how to validate both paths and how one path will be retired safely.

## Skeleton of a Good BehaviourSpec

    # <Short, action-oriented description>

    This BehaviourSpec is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

    If a `BEHAVIOURS.md` file is checked into the repository, reference the path to that file here from the repository root and state that this document follows `BEHAVIOURS.md`.

    ## Purpose / Big Picture

    Explain in a few sentences what someone gains after this change and how they can see it working. State the user-visible behaviour you will enable.

    ## Source of Behaviour Definition (optional but recommended)

    If this BehaviourSpec is based on an ExampleDoc, reference the ExampleDoc path and summarize the specific rules/examples that define this behaviour. Do not copy unresolved questions. If there are unresolved behaviour questions, stop and return to the ExampleDoc.

    ## Boundary

    Name the exact observation boundary for this spec. Examples: HTTP endpoint, LiveView event, CLI command, or message/event contract.

    Define the boundary in plain language so a novice can identify where to exercise and observe the behaviour.

    ## Behaviour

    Describe the behaviour in plain language at the chosen boundary. Keep it to one primary action and one primary observable outcome.

    If the capability includes a related failure outcome at the same boundary and same action shape, describe it as an “Otherwise” outcome. If it is a second behaviour, split it into a second spec or a separate scenario only when it is still the same capability.

    ## Scenarios

    Each scenario describes exactly one behaviour execution at the same boundary: one primary action and one primary observable outcome.

    Example:

            ### Scenario 1: <Short name>

            #### Starting Conditions

            State explicit preconditions with concrete example values. If setup is required, state exactly how the data/config/auth state exists for this scenario.

            #### Primary Action

            State exactly one action at the boundary with example inputs.

            If HTTP, include method, path, headers, and body shape.
            If LiveView, include event name and params.
            If CLI, include the exact command and arguments.
            If message contract, include the message shape and where it is sent.

            #### Expected Outcome

            State one deterministic, observable result at the same boundary. Prefer outcomes that can be asserted in ExUnit without interpretation.

            #### Otherwise (optional)

            If included, keep the same boundary and action shape. State the precise observable failure outcome.

    ## Implementation Notes (optional)

    Use this section only for implementation details that are necessary to keep the behaviour unambiguous or safely implementable. Do not turn this into a general design document. If a deeper architectural decision is required, create or reference an ADR.

    ## Milestones

    Describe the implementation as a small number of narrative milestones. For each milestone, explain the goal, the work, how to run it, and what proof is expected.

    Keep milestones boundary-oriented. Each milestone must produce a new proof that the behaviour is moving toward a fully working end-to-end result.

    ## Progress

    Use a checklist for granular implementation work. This section tracks actual work performed and must always reflect the current state.

    **Legend**

    [ ] - Not started
    [~] - In progress
    [x] - Completed

    - [ ] (YYYY-MM-DDTHH:MM:SS±HH:MM) Drafted the boundary and behaviour statement.
    - [ ] (YYYY-MM-DDTHH:MM:SS±HH:MM) Wrote or identified the failing test that proves the behaviour is missing.
    - [ ] (YYYY-MM-DDTHH:MM:SS±HH:MM) Implemented the change until the test passes.
    - [ ] (YYYY-MM-DDTHH:MM:SS±HH:MM) Verified the behaviour manually or at the external boundary (if applicable).
    - [ ] (YYYY-MM-DDTHH:MM:SS±HH:MM) Refactored safely while preserving the proof.

    Use timestamps to measure progress and to preserve the implementation history.

    ## Surprises & Discoveries

    Record anything learned during implementation that changes the boundary contract, the scenario wording, or the validation strategy. Include short evidence that a novice can reproduce.

    - Observation: …
      Evidence: …

    ## Decision Log

    Record every decision that affects the behaviour contract, boundary semantics, validation strategy, or implementation constraints.

    - Decision: …
      Rationale: …
      Date/Author: …

    ## Outcomes & Retrospective

    Summarize what was delivered, what remains, and what was learned. Compare the result against the original purpose and scenario outcomes.

    ## Validation and Acceptance

    Describe exactly how to exercise the behaviour and what to observe.

    Include:
    - setup commands (if required)
    - test commands
    - manual verification steps at the boundary (if applicable)
    - the expected outputs or observations

    If a new test is expected, state plainly that the test fails before the change and passes after.

    Use this checklist to verify the BehaviourSpec is implementable and unambiguous. The spec passes only if all statements are true.

    - [ ] Names one exact boundary.
    - [ ] Describes one primary action and one primary outcome per scenario.
    - [ ] States starting conditions explicitly with concrete values.
    - [ ] States deterministic expected outcomes at the boundary.
    - [ ] Includes runnable validation steps and commands.
    - [ ] States what success looks like.
    - [ ] States what failure looks like when relevant.
    - [ ] Contains no unresolved behaviour questions.

    ## Idempotence and Recovery

    State which steps are safe to repeat. If any step is risky or stateful, provide a safe retry or rollback path. Keep the environment clean after completion.

    ## Artifacts and Notes

    Include the most important transcripts, payloads, response examples, diffs, or logs that prove the behaviour works and help a novice reproduce results. Keep them concise and focused on proof.

    ## Interfaces and Dependencies

    Be prescriptive only when necessary to make the behaviour unambiguous or verifiable. Name libraries, modules, services, or contracts only when the boundary or validation depends on them. Prefer stable names and paths, and keep this section minimal.