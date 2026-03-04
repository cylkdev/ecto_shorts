# Behaviour Specification Plans (SpecPlans)

This document describes the requirements for a behaviour specification plan ("SpecPlan"). A SpecPlan is a design document that a coding agent or human can follow to produce high-quality behaviour specifications, and the supporting automation hooks (step definitions, fixtures, and test runners) needed to prove the behaviour works.

Treat the reader as a complete beginner to this repository. They have only the current working tree and the single SpecPlan file you provide. There is no memory of prior plans and no external context.

## How to use SpecPlans and PLANS.md

When authoring a SpecPlan, follow this PLANS.md file to the letter. Read this entire file before writing or revising a SpecPlan. Start from the skeleton and flesh it out as you do your research.

When implementing a SpecPlan, do not stop to ask for "next steps". Move to the next milestone. Keep all living sections up to date. Resolve ambiguities in the plan itself and record the decision. Commit frequently.

When discussing or revising a SpecPlan, record decisions in the `Decision Log`. A SpecPlan must be restartable from only the SpecPlan and the working tree.

## Requirements

Every SpecPlan must be fully self-contained. Self-contained means that the plan contains all the instructions, definitions, and commands a novice needs to succeed.

Every SpecPlan is a living document. Contributors must revise it as progress is made, as discoveries occur, and as design decisions are finalized. Each revision must remain fully self-contained.

Every SpecPlan must enable a complete novice to produce the behaviour spec end-to-end without prior knowledge of this repo.

Every SpecPlan must produce demonstrably working behaviour, not merely "nice looking feature files". The spec must be backed by a runnable proof that fails before and passes after.

Every SpecPlan must define every term of art in plain language, or avoid using it.

Purpose and intent come first. Start by explaining, in a few sentences, why the behaviour matters from a user's perspective, what someone can do after this work, and how to observe it working. Then guide the reader through the exact steps to get that outcome, including what to edit, what to run, and what they should observe.

The agent executing your plan can list files, read files, search, run the project, and run tests. It does not know any prior context. Repeat any assumption you rely on.

Do not depend on external blogs or docs. If knowledge is required, embed it in the plan itself in your own words.

## Formatting

Each SpecPlan must be one single fenced code block labeled as `md` that begins and ends with triple backticks. Do not nest additional triple-backtick fences inside. When you need to show commands, transcripts, diffs, or code, present them as indented blocks within that single fence.

Use two newlines after every heading. Use `#`, `##`, and so on.

Write in plain prose. Prefer sentences over lists. Use lists only when they are clearly shorter and clearer than prose.

Checklists are permitted only in the `Progress` section, where they are mandatory.

When writing a SpecPlan to a Markdown file where the file content is only the single SpecPlan, omit the triple backticks.

## Guidelines

Self-containment and plain language are paramount. If you introduce a phrase that is not ordinary English, define it immediately and explain where it appears in this repository by naming files, commands, or outputs.

Anchor the plan with observable outcomes. Acceptance should be phrased as behaviour a human can verify using specific inputs and outputs. Do not phrase acceptance as internal attributes like "added a struct" or "created a module".

Specify repository context explicitly. Name files with full repository-relative paths, name functions and modules precisely, and describe where new files should be created. When running commands, show the working directory and the exact command line.

Validation is not optional. Include instructions to run tests, to start the system if applicable, and to observe it doing something useful. Include expected outputs so a novice can tell success from failure.

Capture evidence. When steps produce terminal output, short diffs, or logs, include them as indented examples. Keep them concise and focused on what proves success.

## What "behaviour specification" means in this repo

A behaviour specification is a set of readable, example-driven scenarios that define what the system must do from a user-observable point of view.

The canonical shape is a feature file with `Feature`, `Scenario`, and `Given/When/Then` steps, but the plan must follow whatever tooling exists in the repo.

A behaviour specification is not a UI script. Steps should describe intent and outcomes in domain language rather than low-level interactions like clicking buttons or waiting for timers.

A behaviour specification must have a proof. A proof is the smallest runnable thing in this repo that demonstrates the behaviour end-to-end.

If the repo does not have BDD tooling, the plan must still use the behaviour-spec style, but it must map each scenario to the project's existing test style in a way a novice can run and verify.

## Milestones

Milestones are narrative. Each milestone must describe what will exist at the end that did not exist before, how to run it, and what acceptance looks like.

Each milestone must be independently verifiable and incrementally implement the overall goal.

It is acceptable to include a prototyping milestone when there are unknowns. A prototype must be additive and testable, must say how to run it, and must state the criteria for promoting it to "real" or discarding it.

## Living plans and design decisions

SpecPlans are living documents. As you make key decisions, update the plan to record both the decision and the thinking behind it. Record all decisions in the `Decision Log`.

SpecPlans must contain and maintain `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective`. These are not optional.

At completion of a major task or the full plan, write an `Outcomes & Retrospective` entry that summarizes what was achieved, what remains, and lessons learned.

## Skeleton of a Good SpecPlan

    # <Short, action-oriented description>

    This SpecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

    If PLANS.md is checked into the repo, reference its path from the repository root and note that this document must be maintained in accordance with it.

    ## Purpose / Big Picture

    Explain in a few sentences what someone gains after this change and how they can see it working. State the user-visible behaviour the specification will cover.

    ## Progress

    Use a list with checkboxes to summarize granular steps. Every stopping point must be documented here. Use timestamps.

    - [x] (YYYY-MM-DD HH:MMZ) Example completed step.
    - [ ] Example incomplete step.
    - [ ] Example partially completed step (completed: X; remaining: Y).

    ## Surprises & Discoveries

    Document unexpected behaviours, tooling constraints, test runner quirks, or domain edge cases discovered while writing or automating the spec. Provide concise evidence.

    - Observation: …
      Evidence: …

    ## Decision Log

    Record every decision made while working on the plan in this format.

    - Decision: …
      Rationale: …
      Date/Author: …

    ## Outcomes & Retrospective

    Summarize outcomes, gaps, and lessons learned at major milestones or at completion. Compare the result against the original purpose.

    ## Context and Orientation

    Describe the current state relevant to this behaviour in plain language as if the reader knows nothing. Name the key files and modules by full path. Define any non-obvious term you will use.

    Include a short "map" of where behaviour specs live in this repo. If they do not exist yet, define the new folder and naming conventions you will introduce.

    ## Behaviour Vocabulary

    Define the domain terms that appear in the scenarios. Each term must have one clear meaning. If a term maps to data in the system, name the exact fields or tables or JSON keys that correspond to it.

    ## Behaviour Spec Draft

    Write the initial behaviour specification in the target format, as it should appear in the repo. Prefer a small number of scenarios that cover the main behaviour and the most important failure mode.

    Include:
    - The `Feature:` title as a short statement of capability.
    - A short description that explains what the feature is for.
    - Scenarios that each describe one outcome.
    - Steps that use domain language and avoid UI-level actions.
    - Concrete example values that a novice can reuse.

    Place the draft here as indented text, and state the intended final file path.

    ## Plan of Work

    Describe, in prose, the sequence of edits and additions needed to land the behaviour spec and its proof.

    For each edit, name:
    - the file path
    - the exact location (module, function, section)
    - what to add or change
    - why that change is necessary for the spec to be runnable and trustworthy

    ## Concrete Steps

    State the exact commands to run and where to run them. Show a short expected transcript for each important command so the reader can compare. Update this section as work proceeds.

        # from repo root
        <command to run the BDD runner or test suite>

        # expected output (example)
        <short success signal>

    ## Validation and Acceptance

    Describe how to prove the behaviour end-to-end.

    Acceptance must be phrased as behaviour with specific inputs and outputs. If tests are involved, state the exact test command and what "passing" looks like. Name the new test(s) or scenario(s) and state that they fail before and pass after.

    ## Idempotence and Recovery

    If steps can be repeated safely, say so. If a step is risky, provide a safe retry or rollback path.

    ## Artifacts and Notes

    Include the most important transcripts, diffs, or snippets as indented examples. Keep them focused on what proves success.

    ## Traceability Map

    Create a short mapping that lets a novice answer: "Where is this behaviour specified, and where is it proven?"

    Include:
    - the spec file path(s)
    - the automation entry point (test runner command)
    - the step definition file path(s) or the test file path(s) that implement the scenarios
    - any shared fixtures or factories used by the proof

    ## Milestones

    Write 2–6 milestones. Each milestone must be independently verifiable.

    For each milestone, include:
    - the goal (what new behaviour spec/proof exists after this)
    - the exact files you will add/change
    - the command(s) to run
    - the expected observable outcome (pass/fail signals, output snippets)

    ## Revision Note

    When you revise this SpecPlan, add a short note here describing what changed and why.