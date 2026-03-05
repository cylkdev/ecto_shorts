# Example Mapping Execution Plans (ExampleMapPlans)

This document defines the format and requirements for an execution plan ("ExampleMapPlan"). A coding agent can follow an ExampleMapPlan to produce a complete Example Mapping deliverable for one feature or story.

Treat the reader as a complete beginner to this repository. They have only the current working tree and the single ExampleMapPlan file you provide. There is no memory of prior plans and no external context.

## How to use ExampleMapPlans and this PLANS.md

When authoring an ExampleMapPlan, read this entire file first. Follow it exactly.

When executing an ExampleMapPlan, proceed through the milestones in order. Do not stop to ask what to do next. Keep every required section up to date. If you stop work for any reason, update the plan first so a newcomer can restart from only this document.

When discussing an ExampleMapPlan with others, record decisions in the Decision Log so it is unambiguous why the plan changed. ExampleMapPlans are living documents.

## Requirements

Every ExampleMapPlan must be fully self-contained. Self-contained means that in its current form it contains all knowledge and instructions needed for a novice to succeed.

Every ExampleMapPlan is a living document. Contributors must revise it as progress is made, as discoveries occur, and as decisions are finalized. Each revision must remain fully self-contained.

Every ExampleMapPlan must enable a complete novice to produce Example Mapping output that is usable for implementation and testing, not merely notes or a rough outline.

Every ExampleMapPlan must define every term of art in plain language or not use it.

Every ExampleMapPlan must anchor itself to observable outcomes. It must state what will exist at the end, what a human can read or run to confirm it, and what "done" looks like.

## What "Example Mapping" means in this repository

Example Mapping is a structured way to clarify a single feature or story by writing:

A Story that states who wants what and why.

Rules that constrain the story and describe what must always be true.

Examples that are concrete cases showing how each rule behaves.

Questions that capture unknowns that block correct implementation.

An ExampleMapPlan must produce those four outputs in a consistent format. It must also define how those examples will be turned into acceptance tests or scenarios in this repository.

## Formatting

Format is simple and strict.

Write in plain prose. Prefer sentences over lists. Avoid checklists, tables, and long enumerations unless brevity would obscure meaning. Checklists are permitted only in the Progress section, where they are mandatory. Narrative sections must remain prose-first.

Use headings with #, ##, ### as needed. Use two newlines after every heading.

When you need to show commands, transcripts, diffs, or code, present them as indented blocks. Do not nest additional fenced code blocks inside a plan.

If this plan will be committed as a standalone Markdown file whose content is only the plan, omit outer triple backticks in the file itself.

## Guidelines

Purpose and intent come first. Start by explaining why the story matters and how someone will see it working once implemented.

Do not outsource key decisions to the reader. When ambiguity exists, resolve it in the plan itself and explain why you chose that path.

Do not rely on external links as required reading. If knowledge is required, embed it in the plan itself in your own words. If you rely on existing code conventions in this repo, point to the exact files and patterns by full path.

Specify repository context explicitly. Name files with full repository-relative paths, name modules precisely, and state where new files should be created.

Validation is not optional. The plan must include a way to check that the Example Mapping output is complete and usable.

## Milestones

Milestones are narrative. Each milestone must describe the scope, what will exist at the end of the milestone, and how a human can verify it.

Each milestone must be independently verifiable and must incrementally produce a more implementable, testable understanding of the story.

## Living plans and design decisions

Every ExampleMapPlan must contain and maintain these sections: Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective.

When you discover unclear behavior, conflicting requirements, or surprising system constraints, capture them in Surprises & Discoveries with short evidence.

If you change course mid-way, document why in the Decision Log and reflect the implications in Progress.

## Skeleton of a Good ExampleMapPlan

    # <Short, action-oriented description>

    This ExampleMapPlan is a living document. The sections Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

    ## Purpose / Big Picture

    Explain in a few sentences what someone gains after this feature exists and how they can see it working once implemented.

    State the single observable boundary you will use to prove the behavior, such as an API response shape, a function result, a CLI output, or a UI flow that can be exercised manually.

    ## Progress

    Use a list with checkboxes to summarize granular steps. Every stopping point must be documented here, even if it requires splitting a partially completed task into two.

    Use timestamps to make progress measurable.

    - [ ] (YYYY-MM-DD HH:MMZ) Example completed step.
    - [ ] Example incomplete step.
    - [ ] Example partially completed step (completed: X; remaining: Y).

    ## Surprises & Discoveries

    Document unexpected behaviors, contradictions, missing requirements, or important constraints discovered while mapping.

    - Observation: …
      Evidence: …

    ## Decision Log

    Record every decision made while working on the plan in this format:

    - Decision: …
      Rationale: …
      Date/Author: …

    ## Outcomes & Retrospective

    Summarize outcomes, gaps, and lessons learned at major milestones or at completion. Compare the result against the original purpose.

    ## Context and Orientation

    Describe the current state relevant to this story as if the reader knows nothing.

    Name the key files and modules by full path.

    Define any non-obvious term you will use.

    State assumptions explicitly, including environment assumptions, product assumptions, and any relevant existing behavior.

    ## Story

    Write the story in one or two sentences in the form "As a <role>, I want <capability>, so that <benefit>".

    State scope boundaries in plain language, including what is explicitly out of scope for this story.

    ## Rules

    List the rules as short, testable statements.

    Each rule must be phrased so a human can decide whether an example satisfies it.

    Each rule must avoid hidden implementation details. It must describe behavior, not code structure.

    ## Examples

    For each rule, provide examples.

    Each example must include inputs and the expected observable outcome.

    Each example must be concrete. Avoid placeholders like "valid data" unless you also define what makes it valid.

    If timing matters, use explicit durations and clocks. If ordering matters, describe the exact sequence of steps.

    If there are important negative cases, include them explicitly.

    If there are important boundary cases, include them explicitly.

    ## Questions

    List questions that block correct implementation.

    Each question must include why it matters and what decision will change based on the answer.

    If you can resolve a question by reading existing code or tests in this repo, do it and convert the question into either a rule or an example, then record the decision.

    ## Mapping to acceptance tests

    Explain how the examples will be turned into acceptance tests in this repository.

    Name the exact test files to create or update.

    Name the exact test style to follow in this repo, and point to a reference test file if it exists.

    Define a consistent mapping:

    A Story becomes one Feature.

    A Rule becomes either a Rule section or a group of scenarios (depending on tool support).

    An Example becomes a Scenario with explicit Given/When/Then steps.

    State the naming convention for scenarios so they stay stable over time.

    ## Plan of Work

    Describe, in prose, the sequence of edits and additions.

    For each edit, name the file and location and what to insert or change.

    If the repository does not yet have a place to store Example Mapping documents, specify the folder path and file naming convention you will create.

    ## Concrete Steps

    State the exact commands to run and where to run them.

    If you will run tests, state the exact commands and expected outcomes.

    Show short example transcripts as indented blocks so a novice can compare results.

    ## Validation and Acceptance

    Describe how to prove the mapping is usable.

    State what artifacts must exist at the end, such as:

    A completed Story/Rules/Examples/Questions section.

    A set of acceptance tests or scenarios that cover every rule with at least one example.

    No unresolved questions that block implementation, or a clearly documented decision on how unresolved questions will be handled.

    Phrase acceptance as behavior with specific inputs and outputs.

    ## Idempotence and Recovery

    If steps can be repeated safely, say so. If a step is risky, provide a safe retry or rollback path.

    ## Artifacts and Notes

    Include the most important snippets as indented examples.

    Keep them focused on what proves the mapping is correct and complete.

    ## Change Log

    When you revise this plan, add a note describing what changed and why.

    - YYYY-MM-DD: …
      Rationale: …
