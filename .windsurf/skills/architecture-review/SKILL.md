---
name: architecture-review
description: Guides a structured architecture review of an Elixir/Erlang/OTP system, focusing on failure modes, scalability risks, and operational resilience. Use this skill when reviewing the architecture of a project.
---

## When to use

Use this skill when you need to review the architecture of an Elixir/Erlang/OTP system. Architecture reviews focus on failure modes that are easy to miss when reading modules in isolation, such as cascade failures, serialized bottlenecks, state durability gaps, and distributed coordination risks. Use it when you need to assess whether a system holds up under load, stress, and partial failure.

## When not to use

Do **not** use this skill for implementing features. Use the `write-exec-plan` skill for that. Do **not** use this skill for behaviour-preserving refactors. Use the `refactor` skill for that. Do **not** use this skill for code style reviews. Use the `code-style-review` workflow for that.

## What to do

Read `.agent/ARCHITECTURE_REVIEW.md` and load it into your context. If it already exists in context, read it again to refresh your memory. Follow it exactly.

### 1. Understand the system

Identify the system under review and its purpose. State the availability, durability, and latency requirements. Define scope boundaries including what is explicitly out of scope.

### 2. Research the codebase

Map the supervision tree, key modules, entry points, and external dependencies. Start from the deepest modules and work upward. Note OTP primitives in use (ETS, GenServers, registries, etc.).

### 3. Write the ArchitectureReview document

Create a new `.md` file at the path specified by the `document-artifacts` rule in `.windsurf/rules/document-artifacts.md`. Follow the skeleton and all requirements in `.agent/ARCHITECTURE_REVIEW.md`. The document must be self-contained.

At minimum the document must include:

- **Purpose / Big Picture** - what risk you are reducing and what "good" looks like.
- **Constraints and Requirements** - availability, durability, latency, throughput, operational constraints.
- **Context and Orientation** - system description, key modules, runtime shape, supervision trees.
- **Review Approach** - traversal order from deep modules upward.
- **Progress** - checklist updated as work proceeds.
- **Decision Log**, **Surprises & Discoveries**, **Outcomes & Retrospective** - living sections.

### 4. Scan for high-scale risk areas

For each subsystem, evaluate:

- **Serialized work and backpressure** - GenServer bottlenecks, mailbox growth, missing bounded concurrency.
- **Crash behaviour and cascade control** - restart storms, missing circuit breakers, blast radius.
- **State ownership and durability** - in-memory-only state, non-idempotent replay, at-least-once without dedup.
- **Distributed behaviour** - partition assumptions, global singletons, cross-node coordination.

### 5. Apply the standard questions

For every component and boundary, apply the 13 standard questions from `.agent/ARCHITECTURE_REVIEW.md`. Add questions when the code suggests new risks.

### 6. Record findings

List findings with impact, trigger conditions, and evidence. For each high-priority finding, propose the smallest architectural change that reduces risk.

### 7. Close the review

Update the **Outcomes & Retrospective** section with a summary of what was found, what was changed (if anything), what remains, and what was learned. Compare the result against the original purpose.