# Architecture Review Guide (Elixir/Erlang/OTP)

This guide is written so an agent or a human can do an architecture review that holds up under load, stress, and partial failure. It is designed to be used like a living plan. It should stay self-contained, so a complete beginner can follow it without extra context.

## Requirements

- Use a ArchitectureReview document from beginning to the end of the review.

- Write the ArchitectureReview document to `docs/architecture_reviews/NNNN-short-title.md`, where `NNNN` is the next zero-padded sequence number in that directory. The canonical output path and naming convention are governed by the `document-artifacts` rule in `.windsurf/rules/document-artifacts.md`. Write the file in chunks as the document will grow over time.

## What to do

You are reviewing the architecture of an Elixir/Erlang system that runs on OTP.

You are looking for failure modes that are easy to miss when reading modules in isolation. Many issues only appear when components interact, when dependencies are slow, or when retries and restarts amplify a small fault into a larger outage.

Your goal is not only "does it work". Your goal is "does it keep working when the system is under stress".

The application must have a 99.9999999% availability (31.56ms max downtime per year). Run a static analysis to ensure the system can meet this requirement.

## How to perform the review

Start from the deepest modules and work upward.

Deep modules define primitives and invariants. Higher-level modules compose them into workflows. If a deep module has a hidden constraint, every caller inherits it, and the visible failure may show up far from the cause.

As you move up, keep a running model of:

- What this component promises to callers.
- What this component assumes about its inputs and dependencies.
- What this component does when those assumptions are violated.
- What state it owns, and whether that state survives restart.

Each time you find a boundary, treat it as a place where reality can disagree with the code. Boundaries include network calls, disk I/O, ETS access, cross-process calls, message passing, and anything that can block.

## How to reason during the review

For every component and boundary, always ask:

- What can go wrong here.
- When it can go wrong.
- What happens next when it goes wrong.
- How far the failure can spread.
- What a user sees when it happens.
- What data can be lost, duplicated, or corrupted.
- What the system does during recovery.

Do not stop at "it crashes and restarts". Restart is only one mechanism. The real question is whether restart restores correct behavior and correct state.

Anchor your conclusions in observable outcomes. If you claim something is safe, state how to prove it with a command, a test, a trace, or an operational drill.

## Track requirements and constraints as you go

Keep a short "Constraints" section in the review document. Write down the requirements that shape design choices, like availability targets, durability targets, latency limits, throughput, multi-node requirements, and operational constraints.

Each time you find a design decision, write which constraint it supports and which constraint it risks. This makes tradeoffs explicit instead of implicit.

## Treat sharp OTP primitives as architectural risk points

When you see low-level facilities, treat them as possible bottlenecks or failure amplifiers, especially under contention.

Examples include `:disk_log`, `:persistent_term`, `:ets`, `:dets`, `:prim_file`, registries, global coordination tools, and long-lived GenServers.

For each one:

- State why it is used in this system.
- State what happens under contention or high write rates.
- State what happens on restart, upgrade, or crash loops.
- State how growth in data size changes performance and memory pressure.
- State how failures propagate to callers and supervisors.

Prefer avoiding bottlenecks by changing the structure of work distribution. Do not rely on "we will optimize later" as the primary mitigation.

## High-scale risk areas to scan for

### Serialized work and backpressure

Messages to a single process are serialized. A single GenServer can become a throughput ceiling if it does too much per message, blocks inside handlers, or becomes a coordination hub for unrelated work.

Look for:

- `GenServer.call/3` used on a hot path.
- handlers that do disk or network I/O.
- large synchronous work inside message handlers.
- mailbox growth risks when producers outpace consumers.
- missing backpressure and missing bounded concurrency.

### Crash behavior and cascade control

Crashes are normal in OTP. Cascades are not automatically safe.

Look for:

- links and dependencies that take down a whole subsystem.
- supervisors that restart too aggressively and create repeated outages.
- restart storms caused by a shared dependency being down.
- missing circuit breakers and missing load shedding.

### State ownership and durability boundaries

Identify where state lives and what guarantees exist.

Look for:

- critical state held only in memory.
- state that is rebuilt from an external source without validation.
- replay logic that is not idempotent.
- "at least once" semantics without deduplication.

Define "armageddon" for this system in concrete terms. State what is lost, what is recomputed, what must be replayed, and what requires manual intervention.

### Distributed behavior

If the system runs on multiple nodes, assume partitions and partial failure.

Look for:

- assumptions that all nodes can always see each other.
- reliance on global singletons without clear leader election semantics.
- inconsistency risks when two nodes act concurrently.
- operations that span nodes without idempotency and reconciliation.

Decide what the system chooses during partitions. Consistency and availability trade off under real partitions, so the design must be explicit about what it prioritizes and how it behaves.

## Questions to apply everywhere

These are defaults. Add questions when the code suggests new risks.

1. How resilient is it to crashes, and what is the blast radius of a crash.
2. If one process dies, how can the error cascade through links, monitors, or dependencies.
3. If a process repeatedly dies quickly, how does the supervisor react, and what does that do to availability.
4. Where can stampeding herd happen, and what protects the system from it.
5. What are the single points of failure, and what redundancy exists.
6. Where is work serialized behind one process, one queue, or one lock.
7. Which GenServers can become bottlenecks, and what is the plan to distribute work.
8. How does the design behave across nodes during partial failure and partitions.
9. If a workflow spans multiple components, how are partial completion and retries handled.
10. Where are race conditions possible, and what user-visible outcomes can they cause.
11. What assumptions are encoded in the design that will fail at higher scale.
12. What is missing that is required for scale, such as backpressure, rate limiting, load shedding, circuit breaking, or observability.
13. Is there a simpler design that meets the same requirements with fewer coordination points.

## Skeleton of a Good ArchitectureReview document

    # <Short, action-oriented description of the review>

    This review plan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

    ## Purpose / Big Picture

    Explain why this review matters. State what risk you are trying to reduce and what "good" looks like when the review is complete. Describe the user-visible or operator-visible outcomes of fixing the risks you expect to find.

    ## Constraints and Requirements

    List the non-negotiable requirements that shape architecture decisions. Include availability, durability, latency, throughput, and operational constraints. Define any term you use that is not ordinary English.

    ## Context and Orientation

    Describe the system as if the reader knows nothing about this repository. Name the main entry points and key modules by full path. Describe the runtime shape at a high level, including supervision trees, major processes, and external dependencies.

    Include a simple diagram if it helps:

        [Client] -> [API] -> [Domain] -> [Storage]
                      |          |
                      v          v
                  [Cache]    [Background Jobs]

    ## Review Approach

    State how you will traverse the system. Start from deep modules and work upward. List the major subsystems you will review in order.

    ## Progress

    Use checkboxes for granular, trackable steps. Every stopping point must be recorded here, even if you split a task into "done" and "remaining". Use timestamps to show progress rate.

    - [ ] (YYYY-MM-DD HH:MMZ) Create this review plan and identify the review scope.
    - [ ] (YYYY-MM-DD HH:MMZ) Map the supervision tree and restart strategy.
    - [ ] (YYYY-MM-DD HH:MMZ) Identify serialized chokepoints and mailbox growth risks.
    - [ ] (YYYY-MM-DD HH:MMZ) Review state ownership and durability boundaries.
    - [ ] (YYYY-MM-DD HH:MMZ) Review dependency failure behavior and cascade control.
    - [ ] (YYYY-MM-DD HH:MMZ) Review distributed behavior, partitions, and coordination.
    - [ ] (YYYY-MM-DD HH:MMZ) Produce prioritized risk list with mitigations and proofs.

    ## Surprises & Discoveries

    Record unexpected behaviors or constraints you learn during review. Include short evidence like logs, traces, or test output.

    - Observation: …
      Evidence:

          <paste a short excerpt>

    ## Decision Log

    Record decisions made during the review, especially when you choose one interpretation or mitigation over another.

    - Decision: …
      Rationale: …
      Date/Author: …

    ## Plan of Work

    Describe, in prose, the sequence of review actions and what each action will produce. Name the files, modules, or runtime artifacts you will inspect. Keep it concrete.

    ## Concrete Steps

    State the exact commands to run and where to run them. Include expected outputs when helpful. This section must be updated as work proceeds.

        # Example
        # from repo root
        mix test
        mix test --only <tag>
        iex -S mix
        :observer.start()

    ## Validation and Acceptance

    Describe how you will prove the review findings are real and that mitigations work. Phrase acceptance as behavior a human can verify, not as internal refactors.

    Examples:

    - Under a synthetic load test, request latency remains under <X> and error rate remains under <Y>.
    - When a dependency is killed or delayed, the system degrades gracefully and recovers without restart storms.
    - After node restart, critical state is recovered with no invariant violations.

    ## Idempotence and Recovery

    State how to re-run review steps safely. If any step is disruptive, include a safe fallback or rollback path.

    ## Findings

    List findings as short entries, each with impact, trigger conditions, and evidence.

    - Finding: …
      Impact: …
      Trigger: …
      Evidence:

          <short excerpt>

    ## Recommended Changes

    For each high-priority finding, propose the smallest architectural change that reduces risk. Prefer structure changes that remove coupling or remove serialized chokepoints.

    - Change: …
      Why it helps: …
      Scope: …
      Risks introduced: …
      Proof plan: …

    ## Outcomes & Retrospective

    Summarize what you found, what you changed (if anything), what remains, and what you learned. Compare the result against the original purpose.

    ## Revision Notes

    When you revise this plan, add an entry that states what changed and why.

    - (YYYY-MM-DD HH:MMZ) Change: … Reason: …
