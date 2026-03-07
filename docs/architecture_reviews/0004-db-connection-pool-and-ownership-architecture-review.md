# Review DBConnection Pool and Ownership Architecture

This ArchitectureReview is a living document. Keep it up to date as review coverage expands, findings are refined, mitigation directions change, blockers appear, and the next handoff becomes clearer.

If `.agent/ARCHITECTURE_REVIEW.md` is checked into the repository, maintain this ArchitectureReview in accordance with that file.

## Status

**Resolved.** Phase 1, Phase 2, and Phase 3 are complete enough to support cross-app synthesis and guide revision.

After this section, you should be able to answer: is this review open, blocked, ready for handoff, or resolved?

## Current State Snapshot

You can now explain the DBConnection architecture from `DBConnection.execute/4` through pool checkout, holder ownership, queueing, and connection lifecycle. The guide already handled state ownership and failure spread well for this library, but it did not force enough precision around cross-process state transfer, queue deadlines, checkout semantics, ownership mode differences, or what "recovery" means when connection state is intentionally lost and rebuilt.

The issue evidence uses official GitHub issue search results from the DBConnection repo across pages 1 and 2. Some fix summaries are architecture inference from issue statements and the documented library behaviour.

After this section, you should be able to answer: what is already known, what is still unknown, and what should you check next?

## At a Glance

- System under review: vendored `db_connection` `2.9.0` pool, holder, execute, and ownership surfaces
- Visible boundary: `DBConnection.execute/4` and the first checkout path
- Top risks: queue/deadline semantics, copied state across processes, ownership mode confusion, connection-lifecycle recovery assumptions
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

- [x] (2026-03-07 09:03Z) Anchored the review on `DBConnection.execute/4` and the first checkout path.
- [x] (2026-03-07 09:13Z) Read local vendored source in `deps/db_connection` version `2.9.0`.
- [x] (2026-03-07 09:19Z) Completed Phase 1 inventory of unanswered guide questions.
- [x] (2026-03-07 09:26Z) Completed Phase 2 source-backed tracing of execute, run, checkout, pool queueing, holder transfer, and ownership manager flows.
- [x] (2026-03-07 09:35Z) Collected 25 architecture-useful issues from GitHub search results across pages 1 and 2 and distilled fast-path heuristics.

After this section, you should be able to answer: what has been done, what is active now, and what remains?

## Purpose / Big Picture

You use this review to benchmark the guide against a library where the key architectural questions are about process ownership, queueing, deadlines, and connection lifecycle rather than domain objects. DBConnection is a strong stress test for whether the guide can teach a novice to reason about cross-process state movement, pool semantics, and overload behaviour quickly.

After this section, you should be able to answer: why does this review exist and what useful understanding should it produce?

## Context and Orientation

The reviewed system is vendored locally in `deps/db_connection`. The key files for the scoped path are:

- `deps/db_connection/mix.exs` for version and application start information
- `deps/db_connection/lib/db_connection.ex` for the behaviour, execute path, and run/transaction contract
- `deps/db_connection/lib/db_connection/connection_pool.ex` for the default pool queue and CoDel behaviour
- `deps/db_connection/lib/db_connection/holder.ex` for connection state transfer, deadlines, and disconnect handling
- `deps/db_connection/lib/db_connection/ownership/manager.ex` for checkout, allow, mode, and ownership-specific test behaviour

Terms used in this review:

- Holder: the ETS-backed owner that moves connection state between client and connection process contexts.
- Queue target / interval: the overload boundary for dropping or timing out queued work.
- Ownership mode: the explicit mode where a connection can be checked out, shared, allowed, or returned by a caller relationship instead of the default pool.

After this section, you should be able to answer: which files and runtime entrypoints should you read first?

## Request Restated

Review whether the current ArchitectureReview guide gives you enough prompts to understand DBConnection architecture from execute/checkouts through queueing, ownership, failure, and reconnect behaviour.

After this section, you should be able to answer: what exact architecture question does this document own?

## Scope Boundaries

In scope:

- `DBConnection.execute/4`
- `run/3` and checkout path
- Default `ConnectionPool`
- `Holder` state transfer and deadlines
- Ownership manager behaviours that matter for test and client-bound flows

Out of scope:

- Adapter-specific socket protocol implementations
- All transaction subpaths in full detail
- Downstream Ecto integration beyond issue-history context

After this section, you should be able to answer: what does this review cover and what does it intentionally leave out?

## Visible Boundary

The review begins at `DBConnection.execute/4`, where a caller asks the pool for a connection-backed execution path.

After this section, you should be able to answer: where does the review start in a way you can actually inspect or exercise?

## System Model

DBConnection is a process and ownership library first, not only a query helper:

- `DBConnection.execute/4` may encode the query, then uses `run/3` to acquire a connection context.
- `run/3` checks out a connection from the pool, validates status on checkin, and disconnects on status mismatch or retry conditions.
- `ConnectionPool` is a GenServer-backed pool that owns an ETS queue and CoDel-based overload behaviour.
- `Holder` moves connection state between the connection process and caller context, tracks deadlines, and coordinates disconnect, checkin, and stop behaviour.
- `Ownership.Manager` adds an alternate architecture where caller relationships, explicit checkout, and shared/manual modes define access.

Boundary map:

    [Caller process]
          |
          v
    +------------------------------+
    | DBConnection-owned runtime   |
    | - execute/run                |
    | - pool queue                 |
    | - holder transfer            |
    | - ownership manager          |
    +------------------------------+
          |
          v
    [Adapter socket / datastore]

Runtime topology:

    DBConnection.App
      `-- pool or ownership child tree

    caller process
      `-- DBConnection.execute/4
          `-- run/3
              `-- checkout
                  `-- ConnectionPool GenServer
                      `-- ETS queue
                      `-- Holder
                          `-- connection process / adapter state

Critical flow:

    caller
      |
      v
    DBConnection.execute/4
      |
      v
    maybe_encode
      |
      v
    run/3 checkout
      |
      v
    ConnectionPool / Holder
      |
      v
    adapter callback in client process
      |
      v
    decode + log + checkin or disconnect

Failure path traced from source:

- Checkout can fail because no connection is available and queueing is disabled.
- Queued callers can time out, and `Holder`/pool logic disconnects the connection with contextual error output.
- Connection state mismatch on checkin becomes a disconnect and raised error.

Recovery path traced from source:

- Connection processes are reused, but all connection state is lost on disconnect and rebuilt on reconnect.
- Pool recovery is not "keep the state"; it is "re-establish the connection and rebuild volatile state cleanly."
- Ownership mode recovery depends on owner/client lifecycle and explicit manager rules.

After this section, you should be able to answer: where does work enter, where does state live, and which supervisors or processes own recovery?

## Constraints and Requirements

- Checkout and queue semantics must keep the pool responsive under overload.
- State transfer between client process and connection process must remain coherent.
- Deadlines and queue drops must produce useful errors and avoid unbounded buildup.
- Disconnect/reconnect must be safe even though connection state is lost.
- Test/runtime ownership modes materially change behaviour.
- Version-sensitive scope reviewed here:
  - Vendored source: `db_connection` `2.9.0`
  - `mix.lock`: `db_connection` `2.9.0`

After this section, you should be able to answer: which limits decide whether the current architecture is adequate?

## Review Strategy

Traversal order used in this review:

1. Start at `execute/4`.
2. Trace checkout and `run/3`.
3. Trace pool queue behaviour and deadline handling.
4. Trace holder state transfer and disconnect behaviour.
5. Trace ownership manager mode changes and test-like flows.
6. Compare recurring issue history to find the fastest recurring diagnosis paths.

First critical flow: `execute/4` through checkout and successful checkin.

First failure path: queued caller times out or checkout is dropped.

First recovery path: disconnect and reconnect with rebuilt state.

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

- It already encouraged state-owner mapping, which is the center of DBConnection.
- It supported queue/failure/recovery reasoning better than for some other systems.
- It encouraged diagrams that made the pool and holder split visible.

What it did not force you to answer:

- Which process owns the connection state at each step.
- Where deadlines and queue drops are decided.
- What recovery means when all connection state is intentionally discarded on disconnect.
- How ownership mode changes the runtime model relative to the default pool.
- Which telemetry or metrics prove overload, queue depth, or dropped requests.

Where the review had to invent its own logic:

- A state-transfer map between caller, holder, and connection process
- An overload and deadline decision map
- A test-runtime divergence pass for ownership mode

Diagrams the guide did not prompt strongly enough:

- State transfer diagram
- Queue/deadline decision diagram

Architecture facts that felt fundamentally Elixir-shaped rather than library-shaped:

- Mailboxes, ETS transfer, and caller lifecycle are architecture.
- Recovery can mean "rebuild volatile state after disconnect," not "resume in place."
- Test sandboxes and ownership managers are real runtime models.

## Phase 2 Source Review Notes

Source-backed answers:

- Where does work first enter the system: `DBConnection.execute/4`.
- What code runs before the first user-visible boundary: query encode path and meter setup before checkout.
- Which parts define the real contract: DBConnection behaviour callbacks, pool callbacks, holder state-transfer rules, and ownership manager rules.
- Which processes or tables own truth: connection process state, holder ETS tables, pool ETS queue, and ownership manager ETS mappings.
- Where serialized work happens: checkout queue, holder ownership, and per-connection execution.
- What is rebuilt on restart and what is lost: connection state is lost on disconnect; the process can be reused and reconnect with fresh state.
- What signals prove the explanation: queue timeout errors, connection metrics, pool telemetry issues, log entries, and overload/drop messages.

## Findings

Finding: DBConnection requires the guide to model ownership transfer explicitly.
Impact: A novice will misunderstand the system if they assume one process owns connection state throughout the request.
Trigger: Any execute, run, transaction, or ownership mode path.
Evidence: the module docs explicitly say callbacks may run in the client process with state copied to and from the connection process; `Holder` and ETS transfer are central.
Threatened constraint: Accurate understanding of state ownership and failure propagation.
Blast radius: All DBConnection consumers.

Finding: Queueing and deadlines are architecture, not implementation detail.
Impact: Without those prompts, overload failures look like random query errors.
Trigger: Load spikes, busy pools, disabled queueing, or long waits.
Evidence: `ConnectionPool` owns a CoDel queue and timeout/drop handling; issue history is dominated by checkout and timeout problems.
Threatened constraint: Fast root-cause discovery under load.
Blast radius: Any pool-backed runtime.

Finding: Recovery in DBConnection means safe disconnect and rebuild, not preserving in-memory state.
Impact: Reviewers can look for the wrong guarantees after failures.
Trigger: network closure, retry, disconnect, idle ping, or ownership loss.
Evidence: module docs state all state is lost on disconnect and re-established after reconnect.
Threatened constraint: Correct failure and recovery reasoning.
Blast radius: All connection lifecycle paths.

Finding: Ownership mode creates a materially different runtime model that the guide must prompt explicitly.
Impact: Tests and owner/client flows can look broken if reviewed as though they were default pooled behaviour.
Trigger: sandbox tests, shared mode, allow/checkin behaviour, owner exit handling.
Evidence: `Ownership.Manager` exposes mode changes, checkout rules, and owner/client lifecycle handling; issue history repeatedly hits these paths.
Threatened constraint: Accurate review of test and manual-ownership systems.
Blast radius: Test-heavy systems and explicit ownership consumers.

After this section, you should be able to answer: which risks are proven and which ones are still guesses?

## Mitigation Directions

Mitigation direction: Add a required state-transfer prompt.
Why it helps: it forces you to name each owner before and after every handoff.

Mitigation direction: Add an overload-boundary prompt.
Why it helps: it makes queue target, deadline, drop, and timeout behaviour part of the core review.

Mitigation direction: Add a recovery-contract prompt.
Why it helps: it clarifies whether recovery preserves, rebuilds, or abandons volatile state.

Mitigation direction: Add a test-runtime divergence prompt.
Why it helps: it surfaces ownership manager and sandbox-like behaviour as a different runtime model instead of a minor variant.

After this section, you should be able to answer: what architectural move follows from each real finding?

## Guide Implications

Prompts missing from the guide for this app:

- "Who owns the critical state before and after each process boundary?"
- "Where are overload decisions made, and what exact signal proves them?"
- "After disconnect or crash, what state is intentionally lost and what must be rebuilt?"
- "Does this system have alternate runtime modes, such as ownership or test modes, that change who may access the resource?"
- "Which errors are really queue or lifecycle errors rather than query or domain errors?"

## Issue Evidence

Method: official GitHub search queries `repo:elixir-ecto/db_connection is:issue is:closed sort:comments-desc` pages 1 and 2. Low-value release-only items were replaced with later architecture-useful issues.

- `#127 Timeouts on Poolboy.checkout` Symptom: callers time out waiting for a connection. Root cause: checkout queue overload, not query semantics. Concept: queueing and pool pressure. Evidence: issue title and body center on checkout timeout. Fix or mitigation: tune pool and queue behaviour around the actual serialized boundary. Why it worked: the bottleneck was checkout. Faster path: inspect queue owner first. Guide prompt: "What serializes access to the resource?"
- `#99 Add expiration timeout for max connection lifetime` Symptom: long-lived connections need recycling. Root cause: connection lifecycle policy was implicit. Concept: recovery behaviour. Evidence: issue asks for expiration timeout and randomization. Fix or mitigation: support controlled disconnect/reconnect for stale connections. Why it worked: recovery matched lifecycle needs. Faster path: ask when deliberate reconnect is required. Guide prompt: "When is teardown intentional rather than failure?"
- `#193 Pool inconsistency` Symptom: pool state becomes inconsistent. Root cause: queue, holder, and connection ownership fell out of sync. Concept: state ownership. Evidence: issue references earlier fix and inconsistency. Fix or mitigation: tighten ownership transitions and pool invariants. Why it worked: it restored one coherent owner model. Faster path: inspect ownership invariants before surface errors. Guide prompt: "Which invariant must hold across queue, holder, and worker state?"
- `#54 Ownership manager debug logs constantly in mix test` Symptom: tests emit noisy ownership logs. Root cause: test runtime uses a distinct ownership architecture. Concept: test-runtime mismatch. Evidence: issue explicitly references `Ownership.Manager` in tests. Fix or mitigation: tune logging and treat ownership mode as a separate runtime model. Why it worked: the real runtime in tests became explicit. Faster path: ask whether tests use ownership mode. Guide prompt: "What runtime exists only in tests?"
- `#96 Do not permanently give connections out when starting the sandbox` Symptom: sandbox startup pins connections too aggressively. Root cause: ownership transfer semantics were too sticky. Concept: state ownership and transfer. Evidence: issue links sandbox access discussion. Fix or mitigation: keep ownership temporary and explicit. Why it worked: resources returned to the pool model correctly. Faster path: inspect lifetime of checkout ownership. Guide prompt: "How long does one caller own the resource?"
- `#274 post_checkout and pre_checkin callbacks` Symptom: callers need hooks around checkout lifecycle. Root cause: lifecycle boundaries were not visible enough for instrumentation and setup. Concept: observability and proof. Evidence: issue asks for checkin/checkout hook points. Fix or mitigation: expose lifecycle hook surfaces. Why it worked: the critical ownership transitions became observable. Faster path: ask where you can observe ownership changes. Guide prompt: "What signal proves an ownership transfer occurred?"
- `#177 Connection issues after migrating to db_connection 2.0` Symptom: many connection issues after upgrade. Root cause: lifecycle/queue semantics changed under the public API. Concept: public API versus real contract. Evidence: issue reports many new connection failures after upgrade. Fix or mitigation: compare queue, timeout, and connection contract changes instead of query code. Why it worked: the changed architecture sat below the visible calls. Faster path: compare lifecycle semantics before and after upgrade. Guide prompt: "Which lower-layer contract changed across versions?"
- `#197 Connection closure error inconsistency` Symptom: closed-connection errors surface inconsistently. Root cause: different failure paths exposed different closure semantics. Concept: failure propagation. Evidence: issue title explicitly calls out inconsistency. Fix or mitigation: normalize closure error handling across lifecycle paths. Why it worked: callers could reason about one failure model. Faster path: ask whether multiple failure paths represent the same condition differently. Guide prompt: "Do equivalent failures surface through one contract or many?"
- `#144 ConnectionError tcp send: close` Symptom: connection closes during send. Root cause: network boundary failure reaches caller through lifecycle logic. Concept: external boundary handling. Evidence: issue is a network close during DB write path. Fix or mitigation: treat close as connection lifecycle failure, not domain error. Why it worked: recovery moved to reconnect logic. Faster path: inspect external connection state first. Guide prompt: "Is this failure owned by the external transport boundary?"
- `#9 Ecto incompatibilities` Symptom: integration assumptions diverged between Ecto and DBConnection. Root cause: contract boundaries between layers were not aligned. Concept: callback boundaries. Evidence: issue tracks integration changes like option naming. Fix or mitigation: align shared contract terms across the boundary. Why it worked: one abstraction boundary became consistent. Faster path: inspect boundary contract mismatch. Guide prompt: "Which neighboring abstraction expects a different contract than this layer provides?"
- `#45 DB connection may go down and not retry` Symptom: connection loss is not retried as expected. Root cause: reconnect and backoff behaviour were not aligned with failure mode. Concept: recovery behaviour. Evidence: issue explicitly says connection may go down and not retry. Fix or mitigation: harden reconnect/backoff path. Why it worked: the recovery contract matched runtime reality. Faster path: trace disconnect to reconnect sequence. Guide prompt: "After failure, who schedules recovery and when?"
- `#172 Error after upgrading to Ecto 3.0.0-rc.0` Symptom: tests fail after dependency upgrade. Root cause: ownership/lifecycle behaviour changed at the DBConnection boundary. Concept: test-runtime mismatch. Evidence: issue is upgrade-triggered test failure. Fix or mitigation: compare ownership and lifecycle contracts across versions. Why it worked: the changed boundary, not domain code, explained the break. Faster path: inspect test runtime contracts after upgrade. Guide prompt: "Which runtime mode changed under this upgrade?"
- `#323 Pool telemetry` Symptom: users need direct insight into pool health. Root cause: overload boundaries were under-observed. Concept: observability gaps. Evidence: issue asks for telemetry from the pool itself. Fix or mitigation: expose pool metrics and events. Why it worked: queue and ready-connection state became measurable. Faster path: ask what metric proves pool saturation. Guide prompt: "What runtime signal proves queue health and ready capacity?"
- `#48 Clarify error when DB adapter app is not running` Symptom: missing adapter app yields cryptic startup failure. Root cause: boot prerequisites were unclear. Concept: boot and code loading. Evidence: issue mentions missed `postgrex` start. Fix or mitigation: surface missing-adapter diagnostics explicitly. Why it worked: the real boot boundary became visible. Faster path: verify all dependent apps are started. Guide prompt: "What application must be running before this boundary exists?"
- `#202 Keep alive pings do not work with many idle connections` Symptom: idle ping strategy does not scale with many idle connections. Root cause: idle recovery work was serialized too narrowly. Concept: queueing and serialization. Evidence: issue suggests round-robin pinging misses deadline. Fix or mitigation: model idle ping scheduling against pool size and timeout requirements. Why it worked: liveness maintenance matched resource count. Faster path: ask whether maintenance work itself is serialized. Guide prompt: "What maintenance path serializes across many resources?"
- `#195 Improve context for timeout errors logged` Symptom: timeout logs lack context. Root cause: observability did not identify the exact overloaded boundary. Concept: observability and proof. Evidence: issue asks for more context in timeout errors. Fix or mitigation: include queue/checkout context in failures. Why it worked: diagnosis became shorter. Faster path: ask whether the error says which boundary timed out. Guide prompt: "Does the failure signal identify the exact lifecycle stage?"
- `#40 Do not log when owner/client exits if normal or shutdown` Symptom: normal exits look like noisy failures. Root cause: lifecycle semantics did not distinguish expected owner exit from fault. Concept: failure propagation. Evidence: issue asks not to log normal/shutdown exits. Fix or mitigation: respect exit reason semantics. Why it worked: observability aligned with true failure conditions. Faster path: inspect exit reason semantics before calling it an error. Guide prompt: "Which exits are expected lifecycle transitions?"
- `#315 Guidance on handling closed connections` Symptom: users need a clear recovery path for closed connections. Root cause: reconnect expectations were unclear at the public API. Concept: recovery behaviour. Evidence: issue explicitly asks for guidance. Fix or mitigation: document or expose proper reconnect handling. Why it worked: callers stopped treating closure as an application-level mystery. Faster path: trace reconnect contract before wrapper retries. Guide prompt: "What is the official recovery contract after closure?"
- `#183 2.0.4 causes test failures` Symptom: CI starts failing after transitive upgrade. Root cause: test runtime depends on ownership/lifecycle details that differ from normal runtime. Concept: test-runtime mismatch. Evidence: issue is test failure after dependency update. Fix or mitigation: inspect sandbox and ownership interactions. Why it worked: test-only runtime model explained the break. Faster path: isolate ownership mode before business assertions. Guide prompt: "Which test-only ownership rules are in play?"
- `#1 Add owner feature to connection` Symptom: connection should exit when owner dies. Root cause: connection lifecycle must track caller ownership explicitly. Concept: state ownership. Evidence: issue says if the owner dies, connection should exit. Fix or mitigation: tie connection lifecycle to owner lifecycle where ownership model requires it. Why it worked: ownership and cleanup aligned. Faster path: ask who is responsible for cleanup on owner death. Guide prompt: "What dies when the owner dies?"
- `#119 More helpful ConnectionError on query timeout` Symptom: timeout error lacks actionable guidance. Root cause: user-visible failures did not point to queue or timeout semantics strongly enough. Concept: observability gaps. Evidence: issue title is explicit. Fix or mitigation: improve message so the architecture boundary is obvious. Why it worked: users reached the real bottleneck faster. Faster path: inspect whether the error names queue or query timeout correctly. Guide prompt: "Does the error tell you whether you were waiting or executing?"
- `#61 Allow configuring pool_size with env vars` Symptom: pool sizing needs runtime configurability. Root cause: connection capacity is an environment-specific architecture parameter. Concept: environment-specific runtime differences. Evidence: issue asks for env-var tuning. Fix or mitigation: support runtime tuning of pool size. Why it worked: capacity moved to environment where it belonged. Faster path: ask which capacity knobs must be runtime-configurable. Guide prompt: "Which concurrency limits vary by environment?"
- `#272 transaction is already started` Symptom: callers do not understand nested transaction state. Root cause: connection status and transaction state ownership were unclear. Concept: state ownership. Evidence: issue is directly about transaction state status. Fix or mitigation: clarify status contract and nested behaviour. Why it worked: callers could reason about status transitions. Faster path: inspect status state machine first. Guide prompt: "What state machine governs this boundary?"
- `#284 add encode_time to LogEntry` Symptom: log timing misses one phase of work. Root cause: observability omitted part of the lifecycle. Concept: observability and proof. Evidence: issue asks why `decode_time` exists but not `encode_time`. Fix or mitigation: expose timing for all critical stages. Why it worked: the whole execution path became measurable. Faster path: map every stage that can consume latency. Guide prompt: "Which timing stages are missing from observability?"
- `#176 Document how queueing and deadlines work` Symptom: users do not understand overload behaviour. Root cause: the core architecture contract was not documented explicitly enough. Concept: queueing and backpressure. Evidence: the issue title states it directly. Fix or mitigation: document queue/deadline semantics as first-class architecture. Why it worked: users could reason about drop and timeout behaviour. Faster path: read deadline and queue rules before tuning pool size. Guide prompt: "What exact rule decides whether work waits, drops, or disconnects?"

## Fast Path Heuristics

1. In DBConnection, start with who owns connection state before you read adapter code.
2. If the symptom is a timeout, decide whether it happened while waiting, executing, or reconnecting.
3. Treat queue metrics, deadlines, and drops as the real overload contract.
4. Redefine recovery as "disconnect and rebuild safely" unless the source says state survives.
5. Always ask whether the current runtime is pooled mode or ownership mode.
6. In tests, inspect sandbox and owner/client lifecycle before query semantics.

## Concrete Steps

- `sed -n '1,140p' deps/db_connection/mix.exs`
- `sed -n '1,260p' deps/db_connection/lib/db_connection.ex`
- `sed -n '830,1040p' deps/db_connection/lib/db_connection.ex`
- `sed -n '1,260p' deps/db_connection/lib/db_connection/connection_pool.ex`
- `sed -n '1,260p' deps/db_connection/lib/db_connection/holder.ex`
- `sed -n '1,280p' deps/db_connection/lib/db_connection/ownership/manager.ex`
- `jq -r '.items[] | [.number,.title,.comments] | @tsv' /tmp/db_connection_issues.json`
- `jq -r '.items[] | [.number,.title,.comments] | @tsv' /tmp/db_connection_issues_page2.json`

After this section, you should be able to answer: how do you reproduce the evidence and continue the review safely?

## Surprises & Discoveries

- DBConnection is one of the clearest proofs that ownership transfer must be a first-class review concept.
- The docs already explain many behaviours, but a novice still needs the guide to force state-owner mapping and overload-boundary tracing.
- The issue history strongly confirms that pool and queue errors are often misread as domain or adapter bugs.

After this section, you should be able to answer: what changed your understanding during the work?

## Decision Log

- Decision: center the review on `execute/4` rather than `transaction/3`.
  Rationale: the plan specified execute plus first checkout path, and that path already exposes the main ownership and queue semantics.
  Evidence: `execute/4`, `run/3`, `ConnectionPool`, and `Holder`.
  Date/Author: 2026-03-07 / Codex

- Decision: replace the high-comment "New Release" issue with a later queue/deadline documentation issue.
  Rationale: release timing is not architecture-useful evidence.
  Evidence: page 2 contained more relevant issues such as `#176`.
  Date/Author: 2026-03-07 / Codex

After this section, you should be able to answer: why did the review take its current shape?

## Validation and Acceptance

- You can point to the visible boundary and the next real owner of connection state.
- You can trace one successful checkout/checkin path and one dropped or timed-out path.
- You can explain how holder, pool, and connection process split ownership.
- You can explain what state survives disconnect and what does not.
- You can distinguish pooled runtime from ownership runtime.
- You can show which issue patterns would be faster to solve if the guide prompted ownership transfer and overload contracts explicitly.

After this section, you should be able to answer: how do you know this review is complete enough to hand off safely?

## Open Questions / Blockers

- No architecture blockers remain for the scoped review.
- Precision note: some Phase 3 fix summaries are architectural inference rather than full-thread quotation.

After this section, you should be able to answer: what is still unknown and why does it matter?

## Next Handoff

Hand off to the cross-app synthesis pass so the recurring gaps from this review can be converted into dependency-neutral additions to `.agent/ARCHITECTURE_REVIEW.md`.

After this section, you should be able to answer: what should happen after this review and why?

## Outcomes & Retrospective

This review showed that the revised guide must make ownership transfer, overload contracts, and recovery semantics far more explicit. Those are not DB-specific concerns. They are core Elixir and OTP reasoning patterns that apply to any system with shared resources, queues, and lifecycle-sensitive state.

After this section, you should be able to answer: what did this review achieve overall?

## Change Log

- (2026-03-07 09:35Z) Change: Created the DBConnection benchmark ArchitectureReview. Reason: collect Phase 1-3 evidence before revising `.agent/ARCHITECTURE_REVIEW.md`.

After this section, you should be able to answer: how did the document evolve over time?
