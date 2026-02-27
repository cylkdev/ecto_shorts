# Example Mapping Documents (ExampleDocs)

This document describes the requirements for example mapping documents (“ExampleDoc”), a discovery document that a coding agent can follow to produce a precise, shared understanding of behaviour before writing a Behaviour Spec or implementation code.

Treat the reader as a complete beginner to this repository: they have only the current working tree and the single ExampleDoc file you provide. There is no memory of prior discussions and no external context.

An ExampleDoc is not a Behaviour Spec. An ExampleDoc exists to make ambiguity visible and resolvable. Its output is a clear story, a small set of rules, concrete examples under those rules, and explicit blocking questions. Its job ends when a Behaviour Spec can be written without inventing behaviour.

An ExampleDoc is also not an implementation plan. It does not tell a coding agent which modules to change, what functions to create, or how to sequence production code work. It defines what must be true at the behaviour level so implementation can be specified later without guessing.

## Boundary of Responsibility

Use an ExampleDoc when the problem is still ambiguous and the team needs to decide what the feature means before deciding how to implement it.

Do not use an ExampleDoc as a substitute for a Behaviour Spec. If you are naming exact HTTP routes, exact payload shapes, exact ExUnit test names, exact commands to run, or implementation milestones, you are already writing a Behaviour Spec and should move that work into the Behaviour Spec.

The output of an ExampleDoc should be sufficient to write the following parts of a Behaviour Spec without inventing behaviour: the purpose, the behaviour statement, and the scenarios’ starting conditions and expected outcomes.

## How to use ExampleDocs and EXAMPLES.md

When authoring an ExampleDoc, follow `EXAMPLES.md` exactly. If `EXAMPLES.md` is not already in your context, read the entire file before writing the ExampleDoc. An ExampleDoc is only useful if it is internally consistent and deterministic.

When writing an ExampleDoc, work in this order: story -> rules -> concrete examples -> questions -> readiness decision. Do not skip ahead to scenarios or tests. If a question is unresolved, record it immediately in the document and mark the affected rule or example as blocked.

When discussing an ExampleDoc, treat it as a shared discovery artifact. The purpose is not to “win” the right answer quickly. The purpose is to remove multiple plausible interpretations of the behaviour. Record decisions and why they were made.

When revising an ExampleDoc after new information is discovered, update the rules and examples first, then update the questions and readiness section so the document remains restartable and self-contained.

## Formatting

Format and envelope are simple and strict. Each ExampleDoc must be inside a single fenced code block labeled as `md` that begins and ends with triple backticks. Do not nest additional triple-backtick fences inside. When you need to show commands, transcripts, or snippets that support a discovery, include them as indented blocks inside the single fence.

When writing an ExampleDoc to a Markdown (`.md`) file where the file contains only the ExampleDoc, omit the outer triple backticks.

Write in plain prose. Prefer sentences over lists. Avoid checklists except in `Progress` and `Readiness Checklist`, where they are mandatory. Narrative sections must remain prose-first.

## Requirements

NON-NEGOTIABLE REQUIREMENTS

* Every ExampleDoc must be fully self-contained. Self-contained means that, in its current form, it contains all information a novice needs to understand the behaviour decisions captured in the document.
* Every ExampleDoc is a living document. Contributors must revise it as discoveries occur and decisions are made. Each revision must remain fully self-contained.
* Every ExampleDoc must remove ambiguity from behaviour, not move ambiguity into implementation.
* Every ExampleDoc must define every term of art in plain language at first use, or avoid the term.
* Every ExampleDoc must end with an explicit readiness decision: ready for Behaviour Spec, or not ready for Behaviour Spec.

Purpose and intent come first. Begin by explaining, in a few sentences, what user-visible capability is being discussed and why clarity is needed before implementation.

An ExampleDoc must not require a working implementation to be considered complete. Completion is based on clarity and determinism of the behaviour description, not on code existing.

## What belongs in an ExampleDoc

An ExampleDoc defines behaviour meaning using examples. It captures the business/domain semantics that must be agreed upon before implementation details are chosen.

This includes:

- one short story that names the actor, action, and observable outcome at a high level
- rules that are always true if the behaviour is considered correct
- concrete examples that demonstrate each rule
- blocking questions where the team does not yet know the behaviour
- assumptions and out-of-scope notes when they affect interpretation
- a readiness decision that states whether the document can be promoted into a Behaviour Spec

## What does not belong in an ExampleDoc

An ExampleDoc must not prescribe implementation details unless an implementation fact changes observable behaviour.

Do not include:

- module names, function names, schemas, ETS table names, algorithms, or internal architecture choices
- exact routes, exact event names, exact payload shapes, or exact response bodies unless those are already decided and the document is being finalized specifically to hand off to a Behaviour Spec
- implementation milestones, code-change sequencing, refactor steps, or rollback steps
- test file names or a requirement to already have executable ExUnit tests

The examples should be testable in principle, but the ExampleDoc is not the place to author the tests.

## Guidelines

Make ambiguity visible. If two reasonable readers could interpret a rule differently, the rule is not done. Fix the rule or add examples that remove the ambiguity.

Keep rules stable and examples concrete. Rules state what is always true. Examples show one specific case. Do not write rules as examples, and do not write examples as vague summaries.

Keep questions answerable. A good question requests one decision. A poor question starts a broad discussion without identifying what must be decided.

Prefer behaviour language over implementation language. Say what a user or external observer can observe. Do not say how the code will achieve it.

Separate discovery from execution. If you catch yourself writing commands to run, test names, or file paths to edit, stop and move that content to a Behaviour Spec.

## Optional discovery experiments

Sometimes a small experiment is necessary to answer a blocking question. This is allowed in an ExampleDoc, but it must stay in service of discovery.

A discovery experiment is acceptable only when it answers a specific behaviour question that cannot be resolved from existing information. The experiment must be minimal, and the result must be recorded as an observation that updates a rule, example, or question.

Do not turn discovery experiments into production implementation inside the ExampleDoc. If the experiment proves feasibility and the behaviour is now clear, record the decision and move to a Behaviour Spec.

## Living documents and design decisions

ExampleDocs are living documents. As the team resolves questions or discovers hidden domain rules, update the document so a new contributor can understand the current behaviour decisions without replaying prior conversations.

ExampleDocs must contain and maintain a `Progress` section, a `Surprises & Discoveries` section, a `Decision Log` section, and an `Outcomes & Retrospective` section. These sections are not optional.

When you change a rule or example, record why in the `Decision Log`. If the change was triggered by a discovery, also record the discovery and the evidence in `Surprises & Discoveries`.

## Skeleton of a Good ExampleDoc

    # <Short description of the behaviour area being clarified>

    This ExampleDoc is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

    If an `EXAMPLES.md` file is checked into the repository, reference the path to that file here from the repository root and state that this document follows `EXAMPLES.md`.

    ## Purpose / Big Picture

    Explain in a few sentences what behaviour is being clarified, why it matters to a user or operator, and why an ExampleDoc is needed before implementation.

    ## Story

    Write one sentence in this format:

    "As a <type of user>, when I <do one action>, I can <observe one outcome>."

    The story is a framing statement, not a full specification. The rules and examples define the behaviour.

    ## Rules

    Write each rule as one short “always true” statement. A rule is complete only when its terms are defined well enough that different readers will interpret it the same way.

    Separate each rule with a horizontal rule (`---`).

    Under each rule, include:

    - one or more examples that demonstrate the rule
    - any blocking questions specific to that rule
    - optional assumptions or out-of-scope notes if they affect interpretation

    Example shape:

        **Rule:** <Always true statement>

        **Examples**
        - <Concrete example 1>
        - <Concrete example 2>

        **Question (blocking):** <One specific decision that must be made> (optional)

        **Assumption:** <Temporary assumption used until a question is resolved> (optional)

        **Out of scope:** <What this rule explicitly does not cover> (optional)

        ---

    ## Concrete Examples

    Copy or summarize the examples from the rules section into one ordered list so a reader can review the complete behaviour surface in one place.

    Write 3–8 concrete examples as plain sentences, unless the behaviour is so small that fewer examples are sufficient. Add more examples when needed to remove ambiguity.

    A concrete example is one specific, testable case that states:

    - the exact starting state
    - the exact user action or input
    - the exact observable result
    - the observation boundary where the result is seen

    The example must be specific enough that a later Behaviour Spec can turn it into a scenario without inventing missing behaviour details.

    Example sentence shape:

        "When starting state S is true, and the actor does action A with input I, I observe result R at boundary B."

    Examples must describe observable behaviour. They must not describe internal implementation steps.

    ## Questions and Open Decisions

    List all unresolved questions in one place, even if they are also listed under individual rules.

    For each question, state:

    - what decision is needed
    - which rule(s) or example(s) it blocks
    - what assumption (if any) is currently being used temporarily

    If a question is unresolved and materially changes expected outcomes, the ExampleDoc is not ready for a Behaviour Spec.

    ## Readiness for Behaviour Spec

    State one of the following outcomes explicitly:

    - **Ready for Behaviour Spec**
    - **Not ready for Behaviour Spec**

    Then explain why in plain language.

    If ready, summarize which rules and examples define the behaviour to carry forward.

    If not ready, list the minimum unresolved questions that must be answered before promotion.

    ## Readiness Checklist

    Use this checklist to validate the ExampleDoc. All statements must be true before marking the document **Ready for Behaviour Spec**.

    - [ ] The story is one sentence with one user action and one observable outcome.
    - [ ] Every rule is written as a single “always true” statement.
    - [ ] Every rule has at least one concrete example.
    - [ ] Every example specifies starting state, action/input, observable result, and boundary.
    - [ ] Every example describes behaviour, not implementation.
    - [ ] Every blocking question identifies the rule(s) or example(s) it blocks.
    - [ ] The document contains no placeholder outcomes such as “etc.”, “something like”, or “some error”.
    - [ ] Terms of art are defined in plain language at first use.
    - [ ] The document ends with an explicit readiness decision.

    ## Progress

    Use a checklist for granular discovery work. This section tracks the status of clarification work, not implementation work.

    When the step has been created and not yet started, use `[ ]`. When the step is in progress, use `[~]`. When the step is complete, use `[x]`.

    - [ ] (YYYY-MM-DDTHH:MM:SS±HH:MM) Wrote the story sentence.
    - [ ] (YYYY-MM-DDTHH:MM:SS±HH:MM) Drafted initial rules.
    - [ ] (YYYY-MM-DDTHH:MM:SS±HH:MM) Added concrete examples under each rule.
    - [ ] (YYYY-MM-DDTHH:MM:SS±HH:MM) Collected and labeled blocking questions.
    - [ ] (YYYY-MM-DDTHH:MM:SS±HH:MM) Recorded readiness decision (ready / not ready).

    Use timestamps to measure progress and to preserve the history of discovery decisions.

    ## Surprises & Discoveries

    Record anything learned during discovery that changed the meaning of the behaviour or exposed hidden domain rules. Include concise evidence when available.

    - Observation: …
      Evidence: …

    ## Decision Log

    Record every behaviour-level decision made while working on this ExampleDoc.

    - Decision: …
      Rationale: …
      Date/Author: …

    ## Outcomes & Retrospective

    Summarize what was clarified, what remains unresolved, and what a future contributor should know before converting this ExampleDoc into a Behaviour Spec.

    ## Artifacts and Notes

    Include only the most relevant discovery artifacts (short transcripts, issue comments, domain notes, or tiny experiment results) that help explain a decision. Keep them concise and focused on behaviour clarification.