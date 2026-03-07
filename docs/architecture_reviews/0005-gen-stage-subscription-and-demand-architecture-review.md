# Review GenStage Subscription and Demand Architecture

This ArchitectureReview is a living document. Keep it up to date as review coverage expands, findings are refined, mitigation directions change, blockers appear, and the next handoff becomes clearer.

If `.agent/ARCHITECTURE_REVIEW.md` is checked into the repository, maintain this ArchitectureReview in accordance with that file.

## Status

**Resolved.** Phase 1, Phase 2, and Phase 3 are complete enough to support cross-app synthesis and guide revision.

After this section, you should be able to answer: is this review open, blocked, ready for handoff, or resolved?

## Current State Snapshot

You can now explain GenStage architecture from `sync_subscribe/2` through demand dispatch, buffering, and consumer or supervisor behaviour. The guide already helped with visible boundaries and state owners, but it did not force enough attention onto subscription contracts, backpressure vocabulary, dispatcher ownership, and recovery semantics when stages or subscriptions drop out of a pipeline.

The issue evidence uses official GitHub issue search results from the GenStage repo across pages 1 and 2. As in the other reviews, some fix summaries are architecture inference from issue text and the library's documented direction.

After this section, you should be able to answer: what is already known, what is still unknown, and what should you check next?

## At a Glance

- System under review: upstream GenStage subscription, demand, dispatcher, and consumer-supervisor architecture
- Visible boundary: `GenStage.sync_subscribe/2` plus the first demand-dispatch cycle
- Top risks: hidden demand semantics, dispatcher ownership, buffering assumptions, restart semantics across stage pipelines, confusing subscription contracts
- Current status: `Resolved`
- Next handoff: compare this review with the other four app reviews and revise `.agent/ARCHITECTURE_REVIEW.md`

After this section, you should be able to answer: what system is this, where does the review start, what matters most, and what happens next?

## Output

- Primary artifact: A self-contained architecture risk review for one system or subsystem, including the system model, constraints, findings, mitigation directions, and validation path.
- Primary consumer: The architect, reviewer, or implementer deciding what structural changes, records, or follow-up work are required next.
- Ready when: The reviewed boundaries, constraints, findings, mitigation directions, review coverage, and next safe handoff are explicit enough for you to continue without inventing missing review logic.
- Hands off to: An `ADR`, `ExecPlan`, or `RefactoringPlan` when the review identifies a concrete next move, or to an `InvestigationLog` when the remaining uncertainty is still diagnosis.

## Progress

**Legend**

[ ] - Not started
[~] - In progress
[x] - Completed

- [x] (2026-03-07 09:03Z) Anchored the review on `GenStage.sync_subscribe/2` and the first demand-dispatch cycle.
- [x] (2026-03-07 09:15Z) Read upstream GenStage source from `elixir-lang/gen_stage` `main` SHA `72198f86c8cd4ba149e8ee2ad80ab442d6d106be`.
- [x] (2026-03-07 09:19Z) Completed Phase 1 inventory of unanswered guide questions.
- [x] (2026-03-07 09:27Z) Completed Phase 2 tracing for subscribe, dispatch, buffering, failure, and restart.
- [x] (2026-03-07 09:36Z) Collected 25 architecture-useful issues from GitHub search results across pages 1 and 2 and distilled fast-path heuristics.

After this section, you should be able to answer: what has been done, what is active now, and what remains?

## Purpose / Big Picture

You use this review to benchmark the guide against a system where the central architecture is not request/response or pool checkout but demand propagation, buffering, and pipeline lifecycle. GenStage is useful here because it forces the guide to handle backpressure, dispatcher ownership, subscription contracts, and restart semantics without hiding behind framework-specific terminology.

After this section, you should be able to answer: why does this review exist and what useful understanding should it produce?

## Context and Orientation

The reviewed system is the upstream GenStage runtime shape. The key files for this scope are:

- `lib/gen_stage.ex` for start, subscribe, and stage behaviour
- `lib/gen_stage/dispatchers/demand_dispatcher.ex` for the default demand owner
- `lib/gen_stage/buffer.ex` and related dispatcher modules for buffering and dispatch strategy
- `lib/consumer_supervisor.ex` for supervision-shaped consumer behaviour

Terms used in this review:

- Demand: the count of events a consumer asks a producer to send.
- Dispatcher: the owner that decides which consumer gets which events.
- Subscription contract: the rules that define how a consumer and producer become connected and how cancellation or resubscription behaves.

After this section, you should be able to answer: which files and runtime entrypoints should you read first?

## Request Restated

Review whether the current ArchitectureReview guide gives you enough prompts to understand GenStage architecture from subscription setup through demand and dispatch behaviour.

After this section, you should be able to answer: what exact architecture question does this document own?

## Scope Boundaries

In scope:

- `GenStage.sync_subscribe/2`
- stage startup and `init/1` return contracts
- demand dispatch via `GenStage.DemandDispatcher`
- buffering and producer/consumer ownership
- consumer supervisor and pipeline restart implications

Out of scope:

- Flow and Broadway as separate higher-level systems
- every dispatcher implementation in full detail
- domain-specific stage design beyond the architecture lessons in issue history

After this section, you should be able to answer: what does this review cover and what does it intentionally leave out?

## Visible Boundary

The review begins when one stage subscribes to another with `GenStage.sync_subscribe/2` and the first demand request starts flowing upstream.

After this section, you should be able to answer: where does the review start in a way you can actually inspect or exercise?

## System Model

GenStage is a pipeline contract over processes and demand:

- `GenStage.start_link/3` starts a stage process under the GenServer wrapper.
- `sync_subscribe/2` sends a subscription request to a consumer stage, which then coordinates with the producer.
- Demand flows upstream from consumer to producer; events flow downstream from producer to consumer.
- `GenStage.DemandDispatcher` owns default distribution of events according to consumer demand.
- Buffers and internal subscription state hold pending events and demand between stage interactions.
- Consumer supervisors and dynamic consumers add restart and child-lifecycle semantics on top of the demand model.

Boundary map:

    [Producer stage] ---> [Dispatcher / buffer] ---> [Consumer stage]
            ^                                           |
            |-------------------------------------------|
                         demand upstream

Runtime topology:

    Stage process (GenServer wrapper)
      `-- GenStage internal state
          |-- subscription state
          |-- demand counters
          |-- optional buffer
          `-- dispatcher state

    ConsumerSupervisor
      `-- dynamically started consumer children

Critical flow:

    Consumer calls sync_subscribe/2
      |
      v
    subscription request stored and validated
      |
      v
    consumer asks producer for demand
      |
      v
    dispatcher assigns events to highest demand
      |
      v
    consumer handles events
      |
      v
    new demand may be sent upstream

Failure path traced from source:

- Invalid subscribe arguments or incompatible stage roles fail at subscription contract boundaries.
- Demand/dispatch confusion can stall pipelines even when processes stay alive.
- Consumer or producer death can cancel downstream stages depending on supervision and subscription topology.

Recovery path traced from source:

- Restarting a stage can recreate subscriptions if `subscribe_to` is in `init/1`.
- Recovery is not just supervisor restart; it also depends on whether subscriptions, buffers, and pending demand are rebuilt or lost.
- ConsumerSupervisor adds another lifecycle layer where child restart policy changes whether the pipeline remains healthy.

After this section, you should be able to answer: where does work enter, where does state live, and which supervisors or processes own recovery?

## Constraints and Requirements

- Backpressure must remain explicit and bounded.
- Demand vocabulary must match actual dispatch behaviour.
- Dispatcher ownership must be visible because it decides who receives events.
- Pipeline restart behaviour must be understandable for both stages and dynamic consumers.
- Version-sensitive scope reviewed here:
  - Upstream source: `elixir-lang/gen_stage` `main` SHA `72198f86c8cd4ba149e8ee2ad80ab442d6d106be`

After this section, you should be able to answer: which limits decide whether the current architecture is adequate?

## Review Strategy

Traversal order used in this review:

1. Start at `sync_subscribe/2`.
2. Trace stage initialization and subscription ownership.
3. Trace default demand dispatcher behaviour.
4. Trace buffering and cancellation paths.
5. Trace restart or reconnect behaviour through `subscribe_to` and consumer supervision.
6. Compare recurring issue history for the fastest recurring diagnosis patterns.

First critical flow: subscription followed by demand and first event dispatch.

First failure path: subscription or demand semantics stall or cancel the pipeline.

First recovery path: stage restarts and resubscribes via `init/1` or supervisor-driven consumer recreation.

After this section, you should be able to answer: what will you review first and why?

## Risk Areas Under Review

- [x] Entrypoints and boot path
- [x] Compile-time versus runtime
- [x] Public API versus generated API
- [x] Callback, behaviour, protocol, or macro boundaries
- [x] Supervision and lifecycle
- [x] State ownership and ownership transfer
- [x] Concurrency, queueing, backpressure, or demand
- [x] Failure spread and recovery
- [x] Observability and proof
- [x] Test-only or environment-specific runtime differences

After this section, you should be able to answer: which risks are already reviewed and which still need a pass?

## Phase 1 Inventory Notes

What the current guide already answered well:

- It already encouraged state owner and critical flow tracing.
- It made failure and recovery a required part of the review.
- It encouraged diagrams, which are especially useful for pipelines.

What it did not force you to answer:

- What exact contract a subscription creates and who owns it.
- Where demand is counted, buffered, and dispatched.
- Which restart path actually recreates a healthy pipeline and which one only restarts a process.
- Whether backpressure vocabulary matches the implemented behaviour.
- How dynamic consumer supervision changes pipeline semantics.

Where the review had to invent its own logic:

- A subscription-contract map
- A demand-counter and dispatcher-ownership map
- A restart-versus-resubscribe comparison

Diagrams the guide did not prompt strongly enough:

- Demand-upstream / events-downstream diagram
- Dispatcher and buffer ownership diagram

Architecture facts that felt fundamentally Elixir-shaped rather than library-shaped:

- Mailbox, buffer, and demand counters are architecture state.
- Restart without resubscribe is not real recovery.
- Child restart policy can create or destroy backpressure health.

## Phase 2 Source Review Notes

Source-backed answers:

- Where does work first enter the system: `GenStage.sync_subscribe/2` or `subscribe_to` in `init/1`.
- What code runs before the first user-visible boundary: stage init return contract determines stage type and optional subscriptions.
- Which contracts define the real architecture: stage callbacks, dispatcher behaviour, subscription options, and consumer supervisor restart policies.
- Which processes or structures own truth: stage process state, dispatcher demand list, internal buffers, and consumer supervisor children.
- Where serialized work happens: per-stage mailbox, dispatcher demand ordering, and buffer processing.
- What is rebuilt on restart and what is lost: stage-local state and buffered items may be lost unless externalized; `subscribe_to` can rebuild subscriptions on restart.
- What signals prove the explanation: demand counters, warning logs, stalled pipelines, cancellation messages, and issue clusters about demand semantics and reconnection.

## Findings

Finding: GenStage requires the guide to model the subscription contract explicitly.
Impact: Without it, reviews blur together producer, consumer, and dispatcher responsibilities.
Trigger: any pipeline setup or restart analysis.
Evidence: `sync_subscribe/2` and `subscribe_to` are core architecture surfaces, not just convenience APIs.
Threatened constraint: accurate architecture understanding of pipelines.
Blast radius: all GenStage systems.

Finding: Backpressure vocabulary must be tied to real counters and dispatcher behaviour.
Impact: A novice can read `max_demand` and `min_demand` yet still misunderstand what actually happens.
Trigger: throughput tuning, stalled consumers, uneven distribution, and buffering bugs.
Evidence: `DemandDispatcher` explicitly owns demand ordering and warns on mismatched demand expectations; issue history repeatedly questions demand semantics.
Threatened constraint: fast diagnosis of throughput and stall problems.
Blast radius: all producer-consumer pipelines.

Finding: Recovery in a stage pipeline is more than restarting one process.
Impact: Reviews that stop at supervisor restart miss subscription loss, buffer loss, and downstream cancellation.
Trigger: consumer crash, remote disconnect, subscription cancel, or supervisor policy mismatch.
Evidence: docs and issue history around `subscribe_to`, ConsumerSupervisor, and pipeline cancellation.
Threatened constraint: correct reasoning about resilience.
Blast radius: all nontrivial pipelines.

Finding: Dynamic consumer supervision changes the meaning of demand and concurrency.
Impact: A novice can misread `ConsumerSupervisor` issues as ordinary child bugs instead of pipeline topology bugs.
Trigger: dynamic workers, abnormal exit policies, `max_demand`, and child-spec behaviour.
Evidence: issue history repeatedly touches DynamicSupervisor and ConsumerSupervisor semantics.
Threatened constraint: fast and accurate review of dynamic stage systems.
Blast radius: supervised consumers and dynamically scaled pipelines.

After this section, you should be able to answer: which risks are proven and which ones are still guesses?

## Mitigation Directions

Mitigation direction: Add a subscription-contract prompt.
Why it helps: it forces you to name who subscribes to whom, who can cancel, and what resubscribe path exists.

Mitigation direction: Add a backpressure-state prompt.
Why it helps: it makes demand counters, buffers, and dispatchers visible in the system model.

Mitigation direction: Add a recovery-versus-resubscribe prompt.
Why it helps: it prevents reviewers from treating process restart as full pipeline recovery by default.

Mitigation direction: Add a topology prompt for dynamic consumers.
Why it helps: it surfaces how supervision policy changes backpressure and failure propagation.

After this section, you should be able to answer: what architectural move follows from each real finding?

## Guide Implications

Prompts missing from the guide for this app:

- "What exact subscription contract exists at this boundary?"
- "Where are demand, pending events, and buffered items counted and owned?"
- "Does restart recreate the pipeline or only the process?"
- "Which supervisor or child policy changes concurrency and failure semantics?"
- "What signal tells you the pipeline is stalled versus merely slow?"

## Issue Evidence

Method: official GitHub search queries `repo:elixir-lang/gen_stage is:issue is:closed sort:comments-desc` pages 1 and 2. Lower-value release-only items were replaced with later architecture-useful issues.

- `#10 Proposal for DynamicSupervisor` Symptom: dynamic child supervision needs a clearer abstraction. Root cause: pipeline concurrency and child lifecycle needed a first-class owner. Concept: supervision and lifecycle. Evidence: the issue frames DynamicSupervisor as a spawn-off of simple_one_for_one. Fix or mitigation: move dynamic child ownership into a dedicated supervisor abstraction. Why it worked: lifecycle responsibility became explicit. Faster path: ask who owns dynamic child lifecycle. Guide prompt: "Which supervisor owns dynamic concurrency?"
- `#214 producer_consumer does not invoke handle_demand` Symptom: users expect producer-consumer stages to intercept demand differently. Root cause: subscription and demand contract was misunderstood. Concept: queueing and demand. Evidence: issue is specifically about `handle_demand`. Fix or mitigation: clarify which stage types own demand handling. Why it worked: demand owner became explicit. Faster path: identify the stage type before tracing demand. Guide prompt: "Which stage type owns upstream demand?"
- `#131 Configure abnormal exit reasons for DynamicSupervisor` Symptom: temporary worker lifecycle does not match desired exit semantics. Root cause: supervisor policy and workload semantics were not aligned. Concept: supervision and restart semantics. Evidence: issue is about abnormal exit reasons. Fix or mitigation: expose or refine restart-policy control. Why it worked: failure meaning matched supervision behaviour. Faster path: ask which exit reasons should cause restart. Guide prompt: "What restart policy matches the meaning of failure here?"
- `#94 Spurious Protocol.UndefinedError` Symptom: intermittent protocol errors in Flow/GenStage use. Root cause: event shapes and enumerable expectations crossed abstraction boundaries unclearly. Concept: callback boundaries. Evidence: issue is an intermittent `Enumerable` protocol error. Fix or mitigation: tighten event-shape and pipeline expectations. Why it worked: the true contract became explicit. Faster path: inspect event and protocol expectations at each stage boundary. Guide prompt: "What exact data contract crosses this boundary?"
- `#228 Remote node disconnect with ConsumerSupervisor` Symptom: remote producer disconnect crashes consumer-side supervision unexpectedly. Root cause: distributed boundary failure propagated into local child lifecycle. Concept: distributed or node-boundary handling. Evidence: issue describes remote disconnect and ConsumerSupervisor crash. Fix or mitigation: treat remote subscription loss as a first-class failure mode. Why it worked: node-boundary failure was named explicitly. Faster path: ask what happens when the upstream node disappears. Guide prompt: "How does node-boundary failure reach local supervisors?"
- `#201 new language instead of max_demand and min_demand` Symptom: current demand vocabulary is misleading. Root cause: naming did not match actual dispatch semantics. Concept: queueing and backpressure. Evidence: issue says assumptions are made when reading max/min demand. Fix or mitigation: explain or rename based on real behaviour. Why it worked: readers reasoned from actual counters instead of misleading labels. Faster path: inspect actual demand algorithm before tuning. Guide prompt: "Does the vocabulary match the implemented counter semantics?"
- `#128 producer_consumer different behaviour in handle_subscribe` Symptom: producer and consumer subscriptions behave differently in ways users miss. Root cause: subscription callbacks have role-sensitive contracts. Concept: callback boundaries. Evidence: issue explicitly contrasts producer and consumer subscriptions. Fix or mitigation: make role-specific subscription behaviour explicit. Why it worked: the callback contract matched stage type. Faster path: ask which role the stage plays for this subscription. Guide prompt: "How does this callback contract change by role?"
- `#36 Consumer event list in handle_events return` Symptom: docs around consumer return values confuse readers. Root cause: stage output contract was not explicit enough. Concept: public API versus true contract. Evidence: issue quotes confusing example. Fix or mitigation: clarify that consumers do not emit items. Why it worked: one contract became unambiguous. Faster path: verify what each stage type may emit. Guide prompt: "What is this stage allowed to emit or return?"
- `#48 Consider automatically computing statistics` Symptom: pipeline health metrics are hard to derive. Root cause: observability around events, demand, and buffers was weak. Concept: observability gaps. Evidence: issue proposes tracking event counts and demand. Fix or mitigation: expose or compute pipeline statistics. Why it worked: stall and throughput diagnosis became measurable. Faster path: ask what metric proves backpressure health. Guide prompt: "What signal proves events and demand are moving?"
- `#150 GenStage and Ecto streams` Symptom: stream-driven work does not fit GenStage expectations cleanly. Root cause: push/pull and demand semantics differed across boundaries. Concept: external boundary handling. Evidence: issue calls it an impedance mismatch. Fix or mitigation: adapt or buffer the external stream boundary explicitly. Why it worked: demand semantics were not assumed to match. Faster path: inspect whether the upstream source already has its own pacing model. Guide prompt: "Does the neighboring system share the same backpressure model?"
- `#238 FunctionClauseError in GenStage.Streamer.handle_info/2` Symptom: streamer crashes on unexpected info messages. Root cause: mailbox contract was narrower than runtime reality. Concept: failure propagation. Evidence: issue shows `handle_info/2` mismatch. Fix or mitigation: harden or document accepted message shapes. Why it worked: mailbox ownership matched actual runtime traffic. Faster path: inspect unexpected mailbox messages first. Guide prompt: "What else can arrive in this mailbox besides the happy path?"
- `#44 Consumers stop requesting events if they process events too quickly` Symptom: fast pipelines stall. Root cause: demand/accounting behaviour was misunderstood or flawed under quick turnover. Concept: queueing and demand. Evidence: issue title directly states the stall. Fix or mitigation: inspect demand replenishment timing and buffer accounting. Why it worked: the real bottleneck was in counters, not business code. Faster path: trace demand replenishment after each batch. Guide prompt: "When exactly is new demand issued?"
- `#72 Default demand should be 1` Symptom: default demand can overload or confuse new users. Root cause: default concurrency/backpressure policy encoded too much assumption. Concept: queueing and backpressure. Evidence: issue gives premises around default demand. Fix or mitigation: reduce or clarify default demand expectations. Why it worked: the default matched safer mental models. Faster path: review defaults before blaming the pipeline. Guide prompt: "Which defaults materially shape concurrency?"
- `#224 BroadcastDispatcher subscription is not idempotent` Symptom: duplicate subscription creates subtle errors. Root cause: subscription identity and deduplication were not guarded. Concept: state ownership. Evidence: issue says one consumer can subscribe multiple times. Fix or mitigation: make subscription identity idempotent or clearly non-idempotent. Why it worked: ownership of one subscription became stable. Faster path: ask what uniquely identifies a subscription. Guide prompt: "What is the identity and cardinality of this relationship?"
- `#260 subscribe_to option too restrictive` Symptom: `subscribe_to` type and surface do not support real usage patterns cleanly. Root cause: subscription contract surface was too narrow. Concept: public API versus true contract. Evidence: issue explicitly questions `subscribe_to` restriction. Fix or mitigation: widen or clarify accepted subscription forms. Why it worked: the API matched real topology needs. Faster path: inspect the actual subscription topology required. Guide prompt: "Does the boundary surface fit the real topology?"
- `#105 Fixed windows waits for each stage event before emitting` Symptom: windowed flows wait unexpectedly. Root cause: buffer and event-flush semantics were not obvious. Concept: buffering and replay semantics. Evidence: issue describes fixed-window waiting behaviour. Fix or mitigation: clarify or adjust flush semantics. Why it worked: buffering rules matched user expectation. Faster path: inspect flush conditions and buffer ownership. Guide prompt: "What exact condition causes buffered work to flush?"
- `#227 Support handle_continue` Symptom: long startup work in `init` blocks stage startup. Root cause: stage lifecycle lacked a clearer separation between initialization and later work. Concept: supervision and lifecycle. Evidence: issue explicitly mentions long-running startup code. Fix or mitigation: support deferred startup work. Why it worked: stage startup no longer blocked lifecycle unnecessarily. Faster path: ask whether `init` is doing too much synchronous work. Guide prompt: "What work must happen before the stage is considered started?"
- `#321 pass from to ConsumerSupervisor child args` Symptom: child workers need subscription origin or ack context. Root cause: ownership transfer into supervised children was incomplete. Concept: state ownership and transfer. Evidence: issue is about passing `from` into child args. Fix or mitigation: carry origin information through child creation when lifecycle needs it. Why it worked: ack ownership stayed connected to the work. Faster path: ask what context the dynamic child needs to finish ownership transfer. Guide prompt: "What context must cross the supervision boundary?"
- `#68 Introduce Flow.departition` Symptom: partitions give fragmented data views. Root cause: concurrency topology changed data visibility. Concept: distributed or partition handling. Evidence: issue explains partitioned view fragmentation. Fix or mitigation: add a way to rejoin partitioned results. Why it worked: topology and result semantics aligned. Faster path: ask how partitioning changes the visible contract. Guide prompt: "How does concurrency topology change what one consumer can observe?"
- `#204 PartitionDispatch documentation` Symptom: hashing and partition docs are unclear. Root cause: dispatch ownership and routing rules were under-documented. Concept: queueing and distribution. Evidence: issue quotes hash documentation confusion. Fix or mitigation: document how partition dispatch decides routing. Why it worked: users could reason about event placement. Faster path: inspect the dispatcher rule before debugging skew. Guide prompt: "What exact rule decides routing to consumers or partitions?"
- `#222 GenStage.cancel kills associated consumer instead of just the subscription` Symptom: canceling a subscription kills too much. Root cause: cancellation contract and lifecycle coupling were not obvious. Concept: failure spread and recovery. Evidence: issue explicitly describes cancellation killing the consumer. Fix or mitigation: refine or document cancellation semantics. Why it worked: the blast radius of cancel became visible. Faster path: ask what a cancel message is allowed to terminate. Guide prompt: "What is the blast radius of cancellation?"
- `#195 Warn or fail when ConsumerProducer child spec has permanent restart` Symptom: restart policy can lead to DoS. Root cause: supervision defaults and pipeline semantics can interact dangerously. Concept: supervision and restart semantics. Evidence: issue explicitly says it led to DoS. Fix or mitigation: warn or prevent unsafe restart policy combinations. Why it worked: it aligned lifecycle policy with pipeline behaviour. Faster path: inspect restart policy against demand model. Guide prompt: "Could this restart policy amplify failure or load?"
- `#38 Async event delivery is undocumented or unsupported` Symptom: users are unsure whether producers must reply immediately. Root cause: delivery timing contract was unclear. Concept: callback boundaries. Evidence: issue asks whether delivery must be immediate. Fix or mitigation: document or support asynchronous delivery explicitly. Why it worked: timing expectations became clear. Faster path: ask whether callback contract is synchronous or eventually consistent. Guide prompt: "What timing contract does this callback promise?"
- `#71 DynamicSupervisor ignores min_demand` Symptom: configured demand threshold is not respected in one runtime path. Root cause: dynamic supervision path diverged from documented demand semantics. Concept: test/runtime divergence and backpressure. Evidence: issue says `min_demand` is stored and computed but ignored. Fix or mitigation: align implementation with documented demand contract. Why it worked: one runtime model matched the stated semantics again. Faster path: compare documented demand behaviour to actual dynamic-supervisor path. Guide prompt: "Does each runtime mode honor the same demand contract?"
- `#311 Events from internal buffer are not sent after consumer reconnect` Symptom: reconnect loses buffered events. Root cause: restart/reconnect path did not rebuild buffer-to-consumer delivery correctly. Concept: recovery and replay semantics. Evidence: issue explicitly mentions internal buffer and reconnect. Fix or mitigation: restore or clarify replay semantics after reconnect. Why it worked: recovery matched buffering guarantees. Faster path: ask what survives reconnect and who owns replay. Guide prompt: "After reconnect, what buffered work is replayed, dropped, or hidden?"

## Fast Path Heuristics

1. In GenStage, begin by drawing arrows for demand upstream and events downstream.
2. Identify the stage type before reasoning about callbacks.
3. Find the dispatcher and buffer owners before tuning throughput.
4. Treat restart and resubscribe as separate questions.
5. For dynamic consumers, inspect restart policy and child context alongside demand settings.
6. If a pipeline stalls, trace counters and replenishment timing before business logic.

## Concrete Steps

- `curl -sL https://api.github.com/repos/elixir-lang/gen_stage`
- `curl -sL https://api.github.com/repos/elixir-lang/gen_stage/branches/main`
- `curl -sL https://api.github.com/repos/elixir-lang/gen_stage/contents/lib/gen_stage.ex?ref=72198f86c8cd4ba149e8ee2ad80ab442d6d106be`
- `curl -sL https://api.github.com/repos/elixir-lang/gen_stage/contents/lib/gen_stage/dispatchers/demand_dispatcher.ex?ref=72198f86c8cd4ba149e8ee2ad80ab442d6d106be`
- `curl -sL "https://api.github.com/search/issues?q=repo:elixir-lang/gen_stage+is:issue+is:closed&sort=comments&order=desc&per_page=25&page=1"`
- `curl -sL "https://api.github.com/search/issues?q=repo:elixir-lang/gen_stage+is:issue+is:closed&sort=comments&order=desc&per_page=25&page=2"`

After this section, you should be able to answer: how do you reproduce the evidence and continue the review safely?

## Surprises & Discoveries

- GenStage makes it obvious that backpressure vocabulary alone is not enough; the guide has to force concrete ownership and counter tracing.
- Pipeline recovery is easy to overstate if you only look at supervisors and not subscriptions or buffers.
- Many "bugs" in issue history are really architecture misunderstanding around who owns demand, restart, or buffering.

After this section, you should be able to answer: what changed your understanding during the work?

## Decision Log

- Decision: keep the review on subscription and demand rather than expanding into all Flow semantics.
  Rationale: the milestone boundary was GenStage itself.
  Evidence: `sync_subscribe/2`, dispatcher behaviour, and consumer supervision already exposed the key guide gaps.
  Date/Author: 2026-03-07 / Codex

- Decision: replace lower-value release issues with later architecture-useful ones from page 2.
  Rationale: the plan requires issue evidence that accelerates architecture review.
  Evidence: page 2 contained better demand, restart, and cancellation issues.
  Date/Author: 2026-03-07 / Codex

After this section, you should be able to answer: why did the review take its current shape?

## Validation and Acceptance

- You can point to the subscription boundary and describe the next owner of demand.
- You can explain where demand counters, buffers, and dispatcher state live.
- You can trace one stall or cancellation failure path.
- You can explain why restart alone is not always recovery.
- You can show how dynamic supervision changes backpressure and lifecycle semantics.
- You can show which issue patterns would be faster to solve if the guide prompted subscription contracts and replay semantics explicitly.

After this section, you should be able to answer: how do you know this review is complete enough to hand off safely?

## Open Questions / Blockers

- No architecture blockers remain for the scoped review.
- Precision note: some Phase 3 fix summaries are architectural inference rather than full-thread quotation.

After this section, you should be able to answer: what is still unknown and why does it matter?

## Next Handoff

Hand off to the cross-app synthesis pass so the recurring gaps from this review can be converted into dependency-neutral additions to `.agent/ARCHITECTURE_REVIEW.md`.

After this section, you should be able to answer: what should happen after this review and why?

## Outcomes & Retrospective

This review showed that the revised guide must treat subscription contracts, demand counters, buffering, and replay semantics as first-class architecture concepts. Those are generic Elixir review skills for any message-driven or queued system, which makes them valuable additions to the main guide.

After this section, you should be able to answer: what did this review achieve overall?

## Change Log

- (2026-03-07 09:36Z) Change: Created the GenStage benchmark ArchitectureReview. Reason: collect Phase 1-3 evidence before revising `.agent/ARCHITECTURE_REVIEW.md`.

After this section, you should be able to answer: how did the document evolve over time?
