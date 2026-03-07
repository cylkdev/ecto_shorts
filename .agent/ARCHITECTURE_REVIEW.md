# Architecture Reviews

This document defines the standard for an `ArchitectureReview`, a living working document used to review one Elixir or OTP system deeply enough that meaningful risk paths are either covered, mitigated, or explicitly deferred. Treat the reader as a complete beginner to this repository. They have only the current working tree and the single ArchitectureReview you provide. There is no memory of prior reviews and no external context.

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Purpose / Big Picture

Use an ArchitectureReview to turn a complex system into an explicit, evidence-backed architecture risk review that can survive restarts, handoffs, and repeated review passes. Record the system model, the constraints that shape the design, the review strategy, the findings that matter, the mitigation directions that follow from those findings, the validation path, and the next safe handoff.

An ArchitectureReview does not own diagnosis of one isolated bug, it does not define intended behaviour at one user-observable boundary, and it does not prescribe implementation sequence. It owns deep system review, architecture risk analysis, evidence, mitigation direction, and the decision about when work should move using `Handoffs`.

## Output

- Primary artifact: A self-contained architecture risk review for one system or subsystem, including the system model, constraints, findings, mitigation directions, and validation path.
- Primary consumer: The architect, reviewer, or implementer deciding what structural changes, records, or follow-up work are required next.
- Ready when: The reviewed boundaries, constraints, findings, mitigation directions, review coverage, and next safe handoff are explicit enough for a beginner to continue without inventing missing review logic.
- Hands off to: See `Handoffs` for the valid next destination, the unresolved question that moves there, and the evidence that makes the handoff safe.

## How to Use ArchitectureReviews and ARCHITECTURE_REVIEW.md

When you write an ArchitectureReview, follow `.agent/ARCHITECTURE_REVIEW.md` to the letter. If it is not in your context, read the entire file before you continue.

Use this guide when the task is to review architecture, resilience, scaling behaviour, supervision behaviour, state ownership, dependency risk, or failure spread across an Elixir or OTP system. Start the ArchitectureReview before deep review work begins. Keep it open while you work. Update it as boundaries become clearer, passes are completed, findings are refined, mitigation directions change, blockers appear, and handoff decisions change. Do not treat the document as a summary you write at the end.

As soon as you choose this guide, write `Trigger for Using This Document`. Record the exact observed trigger facts, the full explicit reasoning path that made `ArchitectureReview` the correct document, the nearest competing document types you rejected and why, and a short replication rule a later contributor can reuse. If the owning question changes but the ArchitectureReview still owns the work, update that section and record the revision in `Change Log`.

Use `Document Relationships` to understand how this guide differs from the other planning guides. Use `Handoffs` to decide whether this document still owns the next unresolved question or whether a listed destination owns it now.

Review one user-observable boundary at a time, then repeat the same review loop across supervision trees, dependencies, state boundaries, recovery paths, concurrency chokepoints, discovery paths, and failure propagation paths until no meaningful unreviewed path remains or the remaining gap is recorded as a blocker.

Use `.agent/OUTPUTS.md` as the source of truth for the ArchitectureReview output location and naming rules.

## Safe Parallel Document Maintenance

When you update this document itself, let one coordinator own the final document edit. The coordinator decides the active scope, the canonical wording, and the final structure that lands in the checked-in guide.

After the change scope is stable, worker passes may inspect independent sections, companion files, or stale references in parallel. Each worker pass should return bounded facts such as outdated wording, missing sync updates, stale paths, or terminology drift.

Collect those worker-pass results before you edit this document. Do not update the guide from half-collected scans.

## Document Relationships

Use this section to understand how the planning guides relate to each other before you choose or change documents. Focus on purpose first, then intent, then the point where each document becomes the right place to work. Use `Handoffs` for the valid transitions.

### InvestigationLog

Purpose: Diagnose one visible problem and gather evidence.

Intent: Turn unclear failure into proven facts, interpretation, and a safe next action.

Use it when: A visible problem exists, but the cause is not yet proven.

### ExampleMappingDoc

Purpose: Clarify intended behaviour at one user-observable boundary.

Intent: Turn ambiguous or disputed behaviour into explicit rules, examples, and acceptance-test targets.

Use it when: The boundary is known, but the intended behaviour is still unclear.

### BehaviourSpecDoc

Purpose: Record a proof-ready behaviour specification at one user-observable boundary.

Intent: Turn accepted behaviour into concrete specification and proof mapping that implementation can follow without inventing behaviour.

Use it when: Intended behaviour is accepted, but implementation planning should not begin until the proof path is explicit.

### ExecPlan

Purpose: Define a concrete implementation sequence for a behaviour-changing result.

Intent: Turn a clear change request into exact edits, commands, and validation that produce a working result.

Use it when: Diagnosis, behaviour, and proof expectations are already clear enough to implement.

### RefactorPlan

Purpose: Define a safe structural change that preserves observable behaviour.

Intent: Turn a known refactor need into a restartable, behaviour-preserving work sequence with proof.

Use it when: The goal is to improve structure without intentionally changing observable behaviour.

### ArchitectureReview

Purpose: Review system shape, risk, and failure behaviour.

Intent: Turn a complex system into an explicit, evidence-backed architecture risk review and mitigation direction.

Use it when: The question is about resilience, scaling, state ownership, dependency risk, or system-level failure spread.

### ADR

Purpose: Record one lasting architectural or design decision.

Intent: Turn an important choice into a durable record of drivers, options, outcome, consequences, and validation.

Use it when: A decision must stay explicit over time so future maintainers can understand and apply it.

## Handoffs

This document owns a question only while it is the place where the next missing decision, evidence, or instructions must be added. Use this section to decide whether this document still owns the next unresolved question, which listed destination owns it if not, and what evidence makes the handoff safe now.

### Incoming Handoffs

- From `ADR`: Use this guide when a lasting design choice is already recorded, and the main unresolved question is how that decision affects system shape, risk, failure spread, or follow-up review.

### Outgoing Handoffs

- To `ADR`: Hand off when the review findings and mitigation direction make the lasting design choice explicit, and the remaining unresolved question is how to record that decision so future work can rely on it.
- To `ExecPlan`: Hand off when the reviewed boundaries, findings, and mitigation direction are explicit enough to stop architectural review, and the remaining unresolved question is how to plan behaviour-changing implementation that reduces the reviewed risk.
- To `RefactorPlan`: Hand off when the reviewed boundaries, findings, and mitigation direction are explicit enough to stop architectural review, and the remaining unresolved question is how to plan behaviour-preserving structural change that reduces the reviewed risk.
- To `InvestigationLog`: Hand off when the broader system path is explicit enough to stop architecture review, and the remaining unresolved question is which failure or unexpected behaviour must be diagnosed at a nearer user-observable boundary.

### Recording the Handoff

Use the `Next Handoff` section to name one destination listed in this section.

State what question this document no longer owns, what question the next destination now owns, and what evidence or completed sections make the handoff safe now.

If no listed handoff applies yet, stay in the current document and state what is still missing before work can move.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:

* Every ArchitectureReview must be fully self-contained, meaning a complete beginner can continue from the document and the current project files alone.
* Every ArchitectureReview must be a living document, meaning you update it as review coverage expands, findings are refined, mitigation directions change, blockers appear, and handoff decisions change.
* Every ArchitectureReview must keep the shared skeleton order defined in this guide.
* Every ArchitectureReview must include an `Output` section that uses the exact four-line template from this guide.
* Every ArchitectureReview must include a `Task and Key Files` section that records the concrete task, the key files, and every companion document, catalog, or artifact update that must stay in sync.
* Every ArchitectureReview must include a `Trigger for Using This Document` section immediately after `Task and Key Files` and before `Output`.
* Every ArchitectureReview must record the exact observed trigger facts, the full explicit reasoning path that made this the correct document, the nearest competing document types that were rejected and why, and a short replication rule another contributor can reuse.
* Every ArchitectureReview must revise `Trigger for Using This Document` whenever the trigger facts or reasoning change while the ArchitectureReview remains the correct document, and record that revision in `Change Log`.
* Every ArchitectureReview must define one system, subsystem, or tightly related review boundary at a time.
* Every ArchitectureReview must restate the request in concrete language.
* Every ArchitectureReview must start from a user-observable boundary that can actually be inspected, exercised, or traced.
* Every ArchitectureReview must record what runs before the user-observable boundary when boot code, generated code, or environment setup materially shape the reviewed path.
* Every ArchitectureReview must distinguish compile-time behaviour from runtime behaviour when macros, generated functions, code loading, reflection, or generated APIs shape the architecture.
* Every ArchitectureReview must record the important runtime owners of state, including process state, ETS state, caches, timers, queues, mailboxes, process dictionary entries, and external state boundaries when they affect behaviour or failure handling.
* Every ArchitectureReview must record how work is named, discovered, registered, routed, or selected at runtime when that changes ownership or behaviour.
* Every ArchitectureReview must include concrete control-flow traces for the critical paths that explain how the system actually works, not only what modules exist.
* Every ArchitectureReview must record alternate runtime modes when tests, development reload, ownership modes, transport fallback, release tasks, or distributed deployment materially change behaviour.
* Every ArchitectureReview must distinguish evidence-backed findings from assumptions, guesses, or mitigation ideas.
* Every ArchitectureReview must make review-pass coverage explicit so a beginner can see what has already been reviewed, what remains, and why the review can or cannot stop.
* Every ArchitectureReview must use `.agent/DEFINITIONS.md` as the source of truth for shared definitions. If a reusable term is missing, add it there instead of defining it locally.
* Every ArchitectureReview must record the observability surface that proves the review claims, including telemetry, logs, traces, metrics, drills, or other runtime signals when they exist.
* Every ArchitectureReview must record the important constraints that govern the review, including availability, durability, latency, throughput, partition, deployment, operational, and recovery expectations when they matter.
* Every ArchitectureReview must include an exact validation path that shows how the findings were checked and how proposed mitigations would be proved.
* Every ArchitectureReview must treat creating or refreshing the active document and syncing required companion documents, catalogs, or artifact notes as tracked work in `Progress`.
* Every ArchitectureReview must end with a clear current status and an explicit next handoff.

Treat these rules as mandatory. If one is missing, the ArchitectureReview is incomplete and the review is not ready to guide architectural decisions or follow-up work safely.

## Workflow

1. Write `Request Restated`, `Scope Boundaries`, `User-Observable Boundary`, `Task and Key Files`, and `Trigger for Using This Document` so a complete beginner can see what part of the system is under review, where the review begins, why this document owns the task, and which files and companion documents must stay in sync.
2. Build `System Model` before you judge the design. Record the runtime shape, supervision boundaries, state owners, discovery mechanisms, external boundaries, and pre-boundary work when they materially shape the path.
3. Write `Constraints and Requirements` as explicit review inputs. Record the actual limits and recovery expectations that decide whether the current architecture is adequate. If an important target is unknown, record that as a blocker.
4. Define `Review Strategy` and `Risk Areas Under Review` so the review has a repeatable traversal order and explicit coverage state.
5. After the boundary and review strategy are stable, let one coordinator fan out bounded worker passes across independent risk paths, critical flows, failure paths, recovery paths, or runtime modes. Keep each worker pass narrow enough that it can return one evidence-backed path review without changing the architecture question.
6. Record each worker pass result in the mailbox sections that fit it, such as `System Model`, `Risk Areas Under Review`, `Findings`, `Progress`, or `Open Questions / Blockers`. Collect those results before you claim a finding, widen the review boundary, or hand off.
7. Trace the critical flows, failure paths, and recovery paths. Separate compile-time behaviour from runtime behaviour when that split matters. Record observations in `System Model`, `Risk Areas Under Review`, `Findings`, or the relevant review sections before you make claims.
8. Write `Findings` only when they are grounded in evidence. For each finding, state the trigger, impact, evidence, threatened constraint, and blast radius.
9. Write `Mitigation Directions` as the smallest credible architectural move that reduces the risk. If implementation sequence becomes the real question, set `Next Handoff` using `Handoffs`.
10. Keep `Progress`, `Concrete Steps`, `Surprises & Discoveries`, `Decision Log`, and `Open Questions / Blockers` up to date so the review is safe to restart at any stopping point. Include one `Progress` item for creating or refreshing the active document and one for keeping required companion documents, catalogs, or artifact notes in sync.
11. Finish with `Validation and Acceptance`, `Next Handoff`, `Outcomes & Retrospective`, and `Change Log`. When wider repository command selection matters, consult `.agent/PROJECT.md` and record the exact command you chose. Stop only when the review coverage is explicit, the findings are evidence-backed, and the next safe handoff is clear.

## Communication Rules

Do not review architecture in silence. Record findings, rejected interpretations, blockers, review-pass coverage, and changes in direction in the ArchitectureReview as they happen.

Do not hide why the ArchitectureReview owns the task. Record `Trigger for Using This Document` as soon as the owning question is clear, and revise it whenever the trigger facts or reasoning change while the ArchitectureReview remains the correct document.

Let one coordinator own sequencing. The coordinator decides when the boundary and review strategy are stable enough to fan out worker passes, when path reviews have been collected, and when the review should hand off instead of continuing to speculate.

Use worker passes only for bounded review work. A worker pass is one narrow inspection such as tracing one runtime path, checking one recovery path, reviewing one dependency boundary, or inspecting one runtime mode. Do not let separate worker passes invent different review scopes.

Use the shared living sections as the mailbox for worker results. `System Model`, `Risk Areas Under Review`, `Findings`, `Progress`, `Concrete Steps`, `Surprises & Discoveries`, `Decision Log`, and `Open Questions / Blockers` are where partial results wait until the coordinator collects them.

Collect worker results before you state findings, widen the subsystem boundary, or set `Next Handoff`. Do not make architecture claims from half-collected path traces.

Before you treat a pattern as safe or unsafe, look for repository evidence. Evidence may include module structure, supervision trees, state ownership, tests, traces, metrics, configuration, deployment assumptions, retry paths, incident clues, or explicit constraints in the codebase or request.

When more than one interpretation is still possible, name the competing interpretations instead of silently choosing one. State what evidence would distinguish them.

Keep the review concrete. State what subsystem you inspected, what boundary you followed, what could fail, how the failure spreads, what evidence you saw, what constraint it threatens, and what should happen next.

Do not hide open risks behind optimistic language. If something is unknown, brittle, untested, or operationally assumed, say so directly and record the consequence.

## Document-Specific Guidance

### Formatting

Each document written from this guide must be one single fenced code block labeled `md` when it is embedded inside another document or message. When writing the document to a Markdown file where the entire file is only that document, omit the outer triple backticks. Do not nest additional triple-backtick fences inside the document. When you need to show commands, transcripts, diffs, examples, scenarios, or code, present them as indented blocks inside the single `md` fence.

Write in plain prose. Prefer sentences over lists. Avoid checklists, tables, and long enumerations unless brevity would obscure meaning. Checklists are permitted only in the `Progress` section, where they are mandatory. Narrative sections must remain prose-first.

### Validation, Evidence, and Revision Discipline

Validation is not optional. Include the exact checks, runner commands, traces, or observable signals that prove the document is complete enough for its next handoff. Capture concise evidence such as short transcripts, outputs, diffs, or cited repository facts when they help a beginner restart safely.

When validation needs repository-specific command selection or wider check breadth, consult `.agent/PROJECT.md` and cite the command you chose here.

Specify repository context explicitly. Name files with repository-relative paths, name modules and functions precisely when they matter, and point to exact artifacts or output locations when the guide requires them.

When you revise the document, ensure the revision is reflected across all relevant sections, including `Trigger for Using This Document`, and record the change in `Change Log`. If the document builds on another checked-in artifact, incorporate the needed context directly or reference the exact file and restate the required facts.

### Start Before Deep Review

Use an ArchitectureReview when the real question is whether a design can hold up under load, failure, restart, partial outage, or long-term maintenance. Start from one user-observable boundary. Then ask what runs before that boundary, what generated or selected it, who owns state at the first handoff, and what signal proves the path is live.

If the work is really about one bug, one intended behaviour, or one implementation sequence, use `Document Relationships` and `Handoffs` to move to the document that owns that question. Architecture review owns system shape and risk, not single-boundary diagnosis or implementation detail.

### Universal Review Loop

Architecture review usually repeats the same thought process across different shapes of systems. Use this loop every time.

#### First pass

1. Anchor the user-observable boundary.
2. Trace what runs before that boundary or generates it.
3. Map the runtime shape and the names or selectors that route work.
4. Name the owners of state.
5. Trace one critical flow end to end.

#### Second pass

1. Separate compile-time behaviour from runtime behaviour.
2. Identify generated surfaces, callback contracts, and hidden owners.
3. Identify serialized work and concurrency chokepoints.
4. Trace failure spread and recovery.
5. Record observability, invariants, and alternate runtime modes.
6. Write evidence-backed findings, then repeat the loop for the next risk path.

### Fast Path Questions

When you are stuck, ask these questions before you go deeper:

1. What runs before the user-observable boundary?
2. Which behaviour was fixed at compile time and which remains dynamic at runtime?
3. Which public, generated, or delegated surface is only a facade over the real owner?
4. How is work discovered, named, registered, routed, or selected at runtime?
5. Who owns state before and after each handoff?
6. What serializes work: a mailbox, pool, dispatcher, queue, registry, or single process?
7. What signal proves the explanation?

If you cannot answer one of these questions, you probably need another review pass before you trust your understanding.

### Boot Path and Pre-Boundary Work

If the system uses scripts, generated entrypoints, code loading, or environment setup before the user-observable boundary, record that path explicitly. Many expensive review mistakes happen because the real architecture starts before the first module or function you thought to inspect.

Examples of pre-boundary work include shell scripts that launch the VM, application startup callbacks, generated module functions, runtime configuration, code reloaders, release tasks, and setup logic in tests.

### Compile-Time Versus Runtime

Treat compile-time and runtime as separate architecture layers whenever macros, generated functions, reflection, route generation, protocols, code loading, or compiler diagnostics shape behaviour.

Record what is generated or fixed at compile time, what remains selectable or dynamic at runtime, what evidence proves the split, and what can fail because those two layers disagree. If you skip this split, you will often blame the wrong owner.

### Generated Surfaces, Contracts, and Runtime Selection

Many Elixir systems present a simple public surface while the real architecture lives behind generated functions, delegated calls, callbacks, or protocol dispatch.

Record the public surface you started from, the generated or delegated owner behind it, the contract that actually controls behaviour, and the mechanism that selects the active owner at runtime, such as a process name, registry key, topic, runtime config, or alternate mode.

### Alternate Runtime Modes

Do not assume the system has one runtime shape. Tests, development reload, ownership modes, transport fallback, release tasks, and distributed deployment can all create different architecture boundaries or owners.

If one of those modes materially changes state ownership, serialization, failure spread, or recovery, record it in the main review. Do not bury it as an afterthought.

### Architecture Lenses

Use these lenses to avoid reviewing only the part of the system that is easiest to see.

#### Structure

- Entrypoints and visible boundaries. Where a caller, message, timer, or scheduler starts work.
- Boot path and pre-boundary work. What runs before the user-observable boundary behaves the way you expect.
- Supervision boundaries. Which supervisors start, isolate, and restart each part.
- Generated surfaces and contract boundaries. Which macros, callbacks, protocols, or delegated APIs hide the real owner.
- State ownership. Which process, ETS table, queue, file, or external system owns truth.
- Naming and discovery. Which names, registries, topics, selectors, or runtime config choose the active owner.
- External boundaries. Which files, services, nodes, or protocols can fail independently.

#### Behaviour

- Critical flows. Which end-to-end paths matter most to correctness, latency, or recovery.
- Compile-time versus runtime. Which parts of the behaviour are generated or fixed before the system starts running.
- Concurrency and serialization. What runs in parallel and what is forced through one owner.
- Queueing, pooling, demand, and buffering. Where work waits, drops, or back-pressures.
- Failure propagation. How crashes, timeouts, exits, or partitions reach callers and neighbors.
- Recovery behaviour. What restarts, what is lost, and what must be rebuilt or replayed.
- Observability and operational constraints. What signals prove the architecture and what limits shape it.

### Control-Flow Traces, State Ownership, and Observability

Use `Review Strategy` and `System Model` together to record how behaviour moves through the system.

For each critical flow, record the entry point, the pre-boundary work that materially shapes it, the public or generated API used, the callback or delegated owner behind that API when it matters, the naming or discovery boundary crossed, the process or supervisor boundary crossed, the state owner before and after each handoff, any queue or buffering boundary crossed, any external boundary crossed, and the observable signals that prove the flow is behaving as described.

If the flow depends on hidden state, name it directly. Hidden state includes buffered mailboxes, timers, retry counters, ETS tables, registries, process dictionary entries, file-backed queues, and external leases.

### Diagrams

Diagrams are working tools, not decoration. Every ArchitectureReview must include at least one plain-text diagram. Use the diagram types below when the system has multiple important boundaries or the main flow is not obvious at a glance.

#### 1. Boundary map

Use a boundary map when you need to know what is inside the reviewed system and what sits outside it.

Example:

    [Caller]
        |
        v
    +---------------------------+
    | MyApp runtime             |
    | - Public API              |
    | - Supervisors             |
    | - Workers                 |
    | - ETS cache               |
    +---------------------------+
        |                   |
        v                   v
    [File system]     [External service]

#### 2. Runtime topology

Use a runtime topology diagram when you need to see how supervisors, workers, registries, and state owners fit together.

Example:

    MyApp.Application
    `-- RootSupervisor
        |-- Registry
        |-- CacheOwner
        |   `-- ETS table :cache_table
        |-- QueueSupervisor
        |   |-- QueueOwner
        |   `-- WorkerSupervisor
        `-- ExternalClient

#### 3. Critical flow

Use a critical flow diagram when the main runtime path is harder to understand than the module list.

Example:

    Caller process
        |
        v
    Dispatch API
        |
        v
    QueueOwner GenServer
        |
        +--> ETS job metadata
        |
        v
    Worker Task
        |
        v
    External service
        |
        v
    Reply or persisted result

#### 4. Boot or generation map

Use a boot or generation map when the most important architecture happens before the user-observable boundary or when macros or generated functions hide the runtime path.

Example:

    [Shell]
       |
       v
    Boot script
       |
       v
    VM startup
       |
       v
    Application.start/2
       |
       v
    Generated public API
       |
       v
    Runtime owner

### Findings, Mitigation Directions, and Restartability

Use `Findings` for evidence-backed risk statements only. A good finding states what can go wrong, when it is triggered, what evidence supports it, what constraint it threatens, and how wide the blast radius is.

Example finding:

    Finding: `MyApp.QueueOwner` serializes both queue admission and external dispatch in one `GenServer`.
    Impact: Caller latency rises and timed-out callers can lose accepted work under load.
    Trigger: More than 100 concurrent callers or slow external responses.
    Evidence: All callers use `GenServer.call/3`, the mailbox grows during a load drill, and in-flight work exists only in process state.
    Threatened constraint: Accept work within 200 ms.
    Blast radius: All callers using `dispatch/1`.

Use `Mitigation Directions` for the smallest credible architectural move that reduces the risk. Do not turn this into an implementation plan.

Example mitigation direction:

    Mitigation direction: Move slow external work out of `MyApp.QueueOwner` and keep the owner responsible only for bounded coordination.
    Why it helps: It removes one hot serialized path, narrows the blast radius of worker failure, and makes in-flight work ownership easier to reason about.

Use the shared `Task and Key Files`, `Progress`, `Current State Snapshot`, `Concrete Steps`, `Surprises & Discoveries`, `Decision Log`, `Open Questions / Blockers`, `Next Handoff`, `Outcomes & Retrospective`, and `Change Log` sections to keep the review restartable across long-running or repeated passes.

## Skeleton of a Good ArchitectureReview

Use this skeleton when you create a new ArchitectureReview. Keep it complete enough that a complete beginner can continue the review from the document alone.

    # <Short, action-oriented description>

    This ArchitectureReview is a living document. Keep it up to date as review coverage expands, findings are refined, mitigation directions change, blockers appear, and the next handoff becomes clearer.

    If `.agent/ARCHITECTURE_REVIEW.md` is checked into the repository, maintain this ArchitectureReview in accordance with that file.

    ## Definitions

    Use `.agent/DEFINITIONS.md` as the source of truth for shared definitions. If a reusable term is missing, add it there instead of defining it locally.

    ## Status

    Write the current state in one short sentence.

    Examples:

        **Open.** The review is in progress and meaningful risk paths still remain.

        **Blocked.** Important constraints, evidence, or environment access are still missing, so the review cannot yet reach a safe conclusion.

        **Ready for Handoff.** The findings and mitigation directions are explicit enough to set `Next Handoff` using `Handoffs`.

        **Resolved.** The review is complete, the important risk paths have been covered or deferred explicitly, and the next action is recorded below.

    ## Current State Snapshot

    Write a short summary of where the review stands right now.

    State what boundaries have already been reviewed, what constraints are already known, what findings are already evidence-backed, what still remains, and what a complete beginner should do first if they restart here.

    ## Task and Key Files

    Record the concrete task this review currently owns.

    List the key repository files, commands, tests, catalogs, or companion documents that matter right now.

    List every document, catalog, or artifact that must be created or updated in the same change and keep this section current as the task or handoff changes.

    ## Trigger for Using This Document

    Record the exact trigger that made this ArchitectureReview the correct document.

    State the concrete observed conditions from the request, repository, prior artifact, or observed system state that triggered this document choice.

    Write the full explicit reasoning path from those facts to this document. Do not skip intermediate decision steps.

    Name the nearest competing document types you considered and explain why each one does not own the current unresolved question.

    End with a short replication rule another contributor can follow to reach the same document choice.

    ## Output

    - Primary artifact: A self-contained architecture risk review for one system or subsystem, including the system model, constraints, findings, mitigation directions, and validation path.
    - Primary consumer: The architect, reviewer, or implementer deciding what structural changes, records, or follow-up work are required next.
    - Ready when: The reviewed boundaries, constraints, findings, mitigation directions, review coverage, and next safe handoff are explicit enough for a beginner to continue without inventing missing review logic.
    - Hands off to: See `Handoffs` for the valid next destination, the unresolved question that moves there, and the evidence that makes the handoff safe.

    ## Progress

    Use a list with checkboxes to summarize the review work and every meaningful stopping point.

    Include one item for creating or refreshing this ArchitectureReview and one item for keeping required companion documents, catalogs, or artifact notes in sync.

    **Legend**

    [ ] - Not started
    [~] - In progress
    [x] - Completed

    - [x] (YYYY-MM-DD HH:MMZ) Created or refreshed this ArchitectureReview and updated `Task and Key Files` and `Trigger for Using This Document`.
    - [ ] Keep required companion documents, catalogs, or artifact notes in sync with this ArchitectureReview.
    - [ ] Example partially completed step (completed: X; remaining: Y).

    ## Purpose / Big Picture

    Explain why this review matters, what risk it is trying to reduce, and what someone should be able to conclude after reading it.

    ## Context and Orientation

    Describe the current system or subsystem as if the reader knows nothing about it yet.

    Name the key files, applications, modules, supervisors, commands, generated surfaces, alternate runtime modes, or operational boundaries that a complete beginner must understand before continuing.

    Use `.agent/DEFINITIONS.md` as the source of truth for shared definitions. If a reusable term is missing, add it there instead of defining it locally.

    ## Request Restated

    Restate the review request so a complete beginner can answer "yes, that is the system concern" or "no, that is not what I meant."

    If more than one interpretation is still possible, list the competing interpretations here.

    ## Scope Boundaries

    State what system, subsystem, or boundary is in scope and what nearby architecture work is explicitly out of scope.

    If you are intentionally deferring adjacent concerns, name them directly.

    ## User-Observable Boundary

    Name the user-observable boundary where the review begins.

    Record the exact command, request, trace, subsystem entry point, deployment boundary, or runtime signal that anchors the review.

    ## System Model

    Record the concrete runtime shape of the reviewed system.

    Name the pre-boundary boot path when it matters, the compile-time versus runtime split when it matters, the main runtime boundaries, supervision tree shape, important processes, naming or discovery mechanisms, state owners, external dependencies, queues, storage boundaries, distributed boundaries, and alternate runtime modes when they change behaviour.

    If macros, generated entry points, delegated surfaces, callback contracts, or hidden state matter, name them directly.

    Include at least one plain-text diagram.

    ## Constraints and Requirements

    Record the constraints that govern the architecture.

    Include the actual availability, durability, latency, throughput, partition, deployment, operational, and recovery expectations that matter for this review.

    If behaviour is version-sensitive, record the exact versions and dependency scope under review here.

    If a required target is unknown, record that directly as a blocker.

    ## Review Strategy

    Record the repeatable traversal order for this review.

    State how you will move through boot or generated surfaces when they matter, deep modules, callers, supervisors, dependencies, discovery mechanisms, state boundaries, alternate runtime modes, and recovery paths, and how you will know when a pass is complete enough to move on.

    ## Risk Areas Under Review

    Record the risk areas currently under review and the current coverage state for each one.

    Example:

        - [~] Serialized queue admission
        - [ ] Crash recovery of accepted work
        - [ ] Observability of retries and timeouts

    ## Findings

    Record evidence-backed architecture findings only.

    For each finding, state the impact, trigger, evidence, threatened constraint, and blast radius.

    ## Mitigation Directions

    Record the smallest credible architectural moves that reduce the reviewed risks.

    Do not turn this into an implementation plan. If sequence matters, set `Next Handoff` using `Handoffs`.

    ## Concrete Steps

    Record the exact commands, traces, inspections, drills, dashboards, and reruns needed to review the system and safely restart the work.

    Include the working directory, environment assumptions, and any recovery notes if a step is disruptive.

    ## Surprises & Discoveries

    Record anything unexpected you learned while reviewing the architecture.

    Include the evidence that revealed it and why it matters.

    - Observation: ...
      Evidence: ...

    ## Decision Log

    Record important review decisions in this format:

    - Decision: ...
      Rationale: ...
      Evidence: ...
      Date/Author: ...

    ## Validation and Acceptance

    Describe how to prove the review is complete enough to trust.

    State the checks that must pass, such as:

    - the user-observable boundary is explicit,
    - the pre-boundary boot or generation path is recorded when it matters,
    - the system model names the important state owners and runtime boundaries,
    - the compile-time versus runtime split is explicit when it changes behaviour,
    - the naming or discovery mechanism that selects the active owner is explicit,
    - at least one critical flow, one failure path, and one recovery path are traced,
    - the observability signals that support the explanation are explicit,
    - the mitigation directions follow from evidence-backed findings.

    ## Open Questions / Blockers

    List only unresolved questions or blockers that can still change the findings, mitigation directions, or next handoff.

    For each blocker, state why it matters, what decision changes based on the answer, and what evidence has already been checked.

    If there are no remaining blockers, say that explicitly.

    ## Next Handoff

    State the next safe handoff using `Handoffs`.

    Name one exact destination listed in `Handoffs`.

    State what question this document no longer owns, what question the next destination now owns, and what evidence or completed sections make the handoff safe now.

    If no listed handoff applies yet, stay in the current document and state what is still missing before work can move.

    ## Outcomes & Retrospective

    State the current outcome in one short paragraph.

    Explain what was reviewed, what was found, what remains, and what a complete beginner should know before continuing.

    ## Change Log

    Record every revision so a newcomer can see how the document changed over time.

    - YYYY-MM-DD: ...
      Rationale: ...

## Final Reminder

An ArchitectureReview is for making system shape, risk, and mitigation direction explicit. It is not for guessing, hiding uncertainty, or leaving structural decisions to the next person. Use `Document Relationships` to confirm what this guide owns. Use `Handoffs` to choose the next listed destination when the unresolved question changes.
