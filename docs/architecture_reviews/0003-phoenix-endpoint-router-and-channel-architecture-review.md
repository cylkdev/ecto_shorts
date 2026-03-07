# Review Phoenix Endpoint, Router, and Channel Architecture

This ArchitectureReview is a living document. Keep it up to date as review coverage expands, findings are refined, mitigation directions change, blockers appear, and the next handoff becomes clearer.

If `.agent/ARCHITECTURE_REVIEW.md` is checked into the repository, maintain this ArchitectureReview in accordance with that file.

## Status

**Resolved.** Phase 1, Phase 2, and Phase 3 are complete enough to support cross-app synthesis and guide revision.

After this section, you should be able to answer: is this review open, blocked, ready for handoff, or resolved?

## Current State Snapshot

You can now explain the Phoenix request and realtime architecture from endpoint startup through router dispatch and channel join/broadcast flow. The guide already handled visible boundaries, supervision, and state owners well here, but the review exposed missing prompts around compile-time router generation, configuration split, PubSub and socket ownership, and environment-specific runtime shapes such as code reloader, watchers, and longpoll fallback.

The issue evidence uses official GitHub issue search results from the Phoenix repo. A few resolution summaries are architecture inference from the issue statement and the framework's later design direction rather than full thread quotation.

After this section, you should be able to answer: what is already known, what is still unknown, and what should you check next?

## At a Glance

- System under review: upstream Phoenix endpoint, router, and channel runtime
- Visible boundary: one HTTP request entering a Phoenix endpoint, plus one channel join
- Top risks: compile-time router generation hidden behind macros, configuration split, PubSub/realtime ownership, proxy and transport assumptions, dev-runtime divergence
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

- [x] (2026-03-07 09:03Z) Anchored the review on one HTTP request entering the endpoint and one channel join.
- [x] (2026-03-07 09:14Z) Read upstream Phoenix source from `phoenixframework/phoenix` `main` SHA `850162d618cf4e31be82fb53ad6e259095fa6126`.
- [x] (2026-03-07 09:19Z) Completed Phase 1 inventory of unanswered guide questions.
- [x] (2026-03-07 09:25Z) Completed Phase 2 tracing for endpoint startup, router generation, channel join, broadcast, failure, and recovery.
- [x] (2026-03-07 09:34Z) Collected 25 architecture-useful issues from GitHub search results across pages 1 and 2 and distilled fast-path heuristics.

After this section, you should be able to answer: what has been done, what is active now, and what remains?

## Purpose / Big Picture

You use this review to benchmark the guide against a system where macros generate a large visible surface but the runtime shape is still very much OTP and process-oriented. Phoenix is a useful test because a novice can find the endpoint and router quickly, but they can still miss the real architecture unless the guide forces them to map compile-time routes, endpoint supervision, transport fallback, PubSub ownership, and dev-versus-prod runtime differences.

After this section, you should be able to answer: why does this review exist and what useful understanding should it produce?

## Context and Orientation

The reviewed system is the upstream Phoenix runtime shape, not one generated application. The key files for this scope are:

- `lib/phoenix/endpoint.ex` for the endpoint boundary and config split
- `lib/phoenix/endpoint/supervisor.ex` for startup children, telemetry, watchers, sockets, and server startup
- `lib/phoenix/router.ex` for compile-time route generation and dispatch semantics
- `lib/phoenix/channel/server.ex` for channel join, process startup, and broadcast dispatch

Terms used in this review:

- Endpoint boundary: where HTTP requests and transport traffic first enter Phoenix.
- Compile-time router generation: routes are macros expanded into optimized dispatch code and metadata.
- Realtime ownership: the process, PubSub, and transport boundaries that own channels, broadcasts, and fallback transports.

After this section, you should be able to answer: which files and runtime entrypoints should you read first?

## Request Restated

Review whether the current ArchitectureReview guide gives you enough prompts to understand Phoenix architecture from endpoint entry through router and channel behaviour without relying on framework-specific prior knowledge.

After this section, you should be able to answer: what exact architecture question does this document own?

## Scope Boundaries

In scope:

- Endpoint startup and configuration shape
- One HTTP request path through the endpoint and router
- One channel join and broadcast path
- Socket, PubSub, and transport boundaries that shape realtime behaviour
- Environment-specific runtime differences that repeatedly affect Phoenix architecture reviews

Out of scope:

- Generated controller and view app structure in depth
- Full LiveView internals
- Asset tool implementation details beyond the architectural implications surfaced by issue history

After this section, you should be able to answer: what does this review cover and what does it intentionally leave out?

## Visible Boundary

The review begins at an HTTP request entering a Phoenix endpoint and, for realtime, one socket/channel join that becomes a channel process.

After this section, you should be able to answer: where does the review start in a way you can actually inspect or exercise?

## System Model

Phoenix splits its architecture across compile time and runtime:

- `use Phoenix.Endpoint` defines the boundary module and initial plug pipeline.
- `Phoenix.Endpoint.Supervisor.start_link/3` starts the endpoint supervision tree, emits `[:phoenix, :endpoint, :init]`, loads configuration, and starts config, warmup, PubSub, sockets, server adapter children, drainers, and watchers.
- `use Phoenix.Router` generates route dispatch and route metadata at compile time.
- Channel joins create per-channel processes through `Phoenix.Channel.Server.join/4`, which starts a child, sends the initial join message, and monitors for crash or reply.
- Broadcast dispatch is PubSub-driven and can use fastlane encoding shortcuts.

Boundary map:

    [Client / browser / proxy]
             |
             v
    +------------------------------+
    | Phoenix endpoint boundary    |
    | - plug pipeline              |
    | - router dispatch            |
    | - sockets and channels       |
    | - PubSub integration         |
    | - transport fallback         |
    +------------------------------+
         |                     |
         v                     v
    [App code]           [Web server / adapters]

Runtime topology:

    MyAppWeb.Endpoint
      `-- Phoenix.Endpoint.Supervisor
          |-- Phoenix.Config child
          |-- warmup child
          |-- PubSub child or pubsub server reference
          |-- socket children
          |-- adapter child specs
          |-- socket drainer children
          `-- watcher children

    Router macros
      `-- compiled dispatch + route metadata

    Channel join
      `-- PoolSupervisor.start_child
          `-- Channel process
              `-- PubSub subscription / broadcast path

Critical flow:

    HTTP request
      |
      v
    Endpoint plug pipeline
      |
      v
    Router compiled dispatch
      |
      v
    Controller / downstream app code

    Socket connect
      |
      v
    Channel.Server.join/4
      |
      v
    Channel child process starts
      |
      v
    PubSub broadcast / fastlane dispatch

Failure path traced from source:

- Endpoint startup can fail on invalid config or socket security checks.
- Channel join monitors the started child and returns an error if the process crashes before acknowledging join.
- Transport and proxy assumptions can surface as 400s, fallback failures, or longpoll state problems.

Recovery path traced from source:

- Endpoint runtime recovery is supervisor-driven around socket, adapter, and watcher children.
- Channel recovery is process-based: a crashed channel process is isolated and can be rejoined or restarted according to surrounding supervision.
- Realtime fallback behaviour depends on transport state and client reconnect logic, not only server supervisor restarts.

After this section, you should be able to answer: where does work enter, where does state live, and which supervisors or processes own recovery?

## Constraints and Requirements

- Endpoint config is split across compile-time and runtime concerns.
- Router macros generate performance-critical dispatch logic at compile time.
- Realtime behaviour depends on socket security checks, PubSub ownership, and transport fallback.
- Dev-time features such as watchers and code reloader change runtime topology and failure modes.
- Reverse proxy and browser behaviour can materially change the visible architecture.
- Version-sensitive scope reviewed here:
  - Upstream source: `phoenixframework/phoenix` `main` SHA `850162d618cf4e31be82fb53ad6e259095fa6126`

After this section, you should be able to answer: which limits decide whether the current architecture is adequate?

## Review Strategy

Traversal order used in this review:

1. Start at the endpoint boundary and startup supervisor.
2. Trace one HTTP request through compile-time router dispatch.
3. Trace one channel join into a dedicated process and PubSub path.
4. Map configuration split, transport fallback, and environment-specific topology.
5. Compare recurring issue history to see which questions repeatedly matter in real systems.

First critical flow: request enters endpoint and reaches compiled router dispatch.

First failure path: channel join crashes or transport/proxy assumptions produce bad requests.

First recovery path: endpoint supervisor restarts children; clients reconnect to channels and transports.

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

- It made endpoint and channel visible boundaries straightforward to anchor.
- It pushed the review toward supervision, state owners, and failure propagation.
- It naturally supported boundary maps and runtime diagrams for the endpoint tree.

What it did not force you to answer:

- Compile-time versus runtime: what the router macros generated versus what runs per request.
- Public API versus generated API: which endpoint, router, or socket macros hide the real runtime behaviour.
- Naming and discovery: how topics, socket routes, PubSub server names, and watcher/server config route work.
- Environment-specific runtime differences: what code reloader, watchers, and proxy assumptions change in development and deployment.
- Transport fallback and reconnect behaviour: where the real recovery logic lives for longpoll or websocket issues.

Where the review had to invent its own logic:

- A compile-time route generation inventory
- A config split map for compile-time versus runtime endpoint options
- A realtime ownership map across socket, channel process, PubSub, and transport fallback

Diagrams the guide did not prompt strongly enough:

- Compile-time route generation to runtime dispatch diagram
- Realtime ownership diagram showing channel process, PubSub, and transport boundaries

Architecture facts that felt fundamentally Elixir-shaped rather than library-shaped:

- Macros are architecture surfaces.
- Process-per-channel is state ownership, not an implementation detail.
- Environment changes such as dev reloader or proxy shape are real architecture divergence.

## Phase 2 Source Review Notes

Source-backed answers:

- Where does work first enter the system: an endpoint module using `Phoenix.Endpoint`, then the endpoint supervisor and its plug or socket entrypoints.
- What runs before the first visible request handling: config merge, pubsub and socket child startup, security checks, optional reloader/watchers, and adapter child startup.
- Which parts are generated or compile-time expanded: router dispatch and route metadata, endpoint and socket definition surfaces.
- Which contracts define the real architecture: endpoint supervisor, adapter child specs, channel join process contract, and PubSub dispatch contract.
- Which processes own truth: endpoint supervisor, channel process, PubSub subscribers, transport processes, and adapter/server children.
- Where serialized work happens: per-request plug path, per-channel process mailbox, and PubSub broadcast fan-out.
- What is rebuilt on restart and what is lost: endpoint children restart under supervision; channel-local state is lost on process crash and must be rejoined or reconstructed by clients.
- What signals prove the explanation: endpoint init telemetry, logs, transport errors, join crash logs, and issue clusters around proxy, transport, and watcher behaviour.

## Findings

Finding: Phoenix requires the guide to treat compile-time route generation as architecture, not syntax sugar.
Impact: Without that prompt, a novice sees a router file but not the generated dispatch and metadata that actually shape runtime behaviour.
Trigger: Any review that starts from endpoint or router code.
Evidence: `Phoenix.Router` explicitly documents macro-generated dispatch and metadata.
Threatened constraint: Fast and accurate architecture understanding.
Blast radius: Every Phoenix request path.

Finding: Phoenix runtime shape changes materially across environments.
Impact: Reviews that ignore code reloader, watchers, adapter config, and proxy assumptions miss real failure paths.
Trigger: Development, deployment behind reverse proxy, longpoll fallback, and asset pipeline behaviour.
Evidence: endpoint config split, supervisor children, and issue history around watchers, build tools, proxy limits, and fallback state.
Threatened constraint: Correct reasoning about startup, recovery, and runtime ownership.
Blast radius: Most Phoenix applications.

Finding: Realtime architecture has multiple state owners.
Impact: Without naming the channel process, PubSub, transport, and client reconnect behaviour separately, failures look random.
Trigger: Join crashes, presence races, fallback issues, and fastlane/broadcast bugs.
Evidence: `Phoenix.Channel.Server.join/4` creates monitored child processes and routes broadcast through PubSub dispatch.
Threatened constraint: Fast diagnosis of realtime issues.
Blast radius: Any socket/channel system.

Finding: Proxy and adapter boundaries are first-class architecture boundaries.
Impact: Reviewers can waste time in router or controller code when the real problem is header limits, transport behaviour, or adapter startup.
Trigger: 400 bad requests, longpoll issues, child spec mismatches, or startup failures.
Evidence: issue history repeatedly clusters around header size, transport fallback, and server adapter behaviour.
Threatened constraint: Fast root-cause discovery.
Blast radius: Phoenix applications under real deployment conditions.

After this section, you should be able to answer: which risks are proven and which ones are still guesses?

## Mitigation Directions

Mitigation direction: Add an explicit compile-time generation prompt to the guide.
Why it helps: it makes router and macro-generated surfaces visible early.

Mitigation direction: Add a required config-split prompt.
Why it helps: it separates compile-time config, runtime config, and environment-specific topology changes.

Mitigation direction: Add a realtime ownership prompt.
Why it helps: it forces you to name socket, channel process, PubSub, transport, and client roles separately.

Mitigation direction: Add an external deployment-boundary prompt.
Why it helps: it moves proxies, adapters, and browser transport behaviour into the core architecture review.

After this section, you should be able to answer: what architectural move follows from each real finding?

## Guide Implications

Prompts missing from the guide for this app:

- "What did the macros generate here, and how do you inspect the generated runtime shape?"
- "Which configuration is compile-time only, which is runtime, and which changes the supervision tree by environment?"
- "For realtime paths, which process owns state, which subsystem fans out, and which side reconnects or resubscribes?"
- "Which external proxy, adapter, or browser assumptions can fail before controller or channel code is wrong?"
- "What recovery depends on client behaviour rather than server supervision?"

## Issue Evidence

Method: official GitHub search queries `repo:phoenixframework/phoenix is:issue is:closed sort:comments-desc` pages 1 and 2. Low-value non-architecture issues such as branding were skipped in favor of later architecture-useful issues.

- `#1410 app.js failed due to missing es2015` Symptom: new project boot fails in asset compilation. Root cause: dev/runtime startup depended on external asset toolchain assumptions. Concept: boot and code loading. Evidence: issue body shows server boots but JS build fails. Fix or mitigation: align generated asset config with actual build tool dependencies. Why it worked: startup ownership included external toolchain. Faster path: inspect watchers and asset boundary before endpoint code. Guide prompt: "Which external build steps are required before this boundary is truly healthy?"
- `#2464 Error: EPERM unlink` Symptom: asset install/build fails on Windows. Root cause: host filesystem semantics affected Phoenix's dev startup path. Concept: environment-specific runtime differences. Evidence: issue is about `npm install` in `assets`. Fix or mitigation: harden asset setup against host file locking semantics. Why it worked: the failure was in host environment, not Phoenix request code. Faster path: check host FS semantics first. Guide prompt: "Which host filesystem behaviours are part of startup?"
- `#459 Way to read request body as string` Symptom: request body access is unclear. Root cause: the real request-body owner is lower in the plug stack than Phoenix's top-level surface suggests. Concept: state ownership. Evidence: issue references Plug support. Fix or mitigation: expose or document the lower boundary clearly. Why it worked: body ownership became explicit. Faster path: ask who owns request state at each pipeline stage. Guide prompt: "At this boundary, who owns the mutable request state?"
- `#2437 400 bad request issues` Symptom: random 400s due to large headers. Root cause: deployment and web-server limits existed below controller code. Concept: external boundary handling. Evidence: issue body mentions Cowboy's default header limit. Fix or mitigation: tune or surface server/proxy limits explicitly. Why it worked: the real failure was adapter/proxy-side. Faster path: inspect adapter and header limits before app code. Guide prompt: "Which lower-layer limits can reject work before your code runs?"
- `#11 Add view layer discussion` Symptom: output safety and rendering ownership were still being defined. Root cause: rendering architecture needed a clear safety boundary. Concept: callback or macro boundaries. Evidence: issue mentions template/XSS protection concerns. Fix or mitigation: define rendering as a clear boundary with safe escaping rules. Why it worked: output safety belonged to one owner. Faster path: ask who owns output escaping and rendering safety. Guide prompt: "Which boundary owns safe external representation?"
- `#1575 CompilationError at GET /` Symptom: intermittent compile error on request. Root cause: dev runtime with code reloading creates a compile/runtime crossover boundary. Concept: compile-time versus runtime. Evidence: issue occurs at request time with compilation error. Fix or mitigation: inspect code reloader and compile graph, not only request handler. Why it worked: the failure happened in compile-on-request behaviour. Faster path: ask whether a request can trigger compilation. Guide prompt: "Can this runtime path invoke compilation or code reload?"
- `#1031 Phoenix install error` Symptom: setup/install fails before app use. Root cause: project bootstrap depends on toolchain and archive setup boundaries. Concept: boot and code loading. Evidence: issue is install failure. Fix or mitigation: align generator/toolchain prerequisites. Why it worked: startup prerequisites were the real contract. Faster path: inspect boot toolchain first. Guide prompt: "What must exist before this system can even generate or start?"
- `#402 NewRelic integration` Symptom: production instrumentation path is unclear. Root cause: observability ownership was not first-class in the framework surface. Concept: observability gaps. Evidence: issue asks for integration experience in production. Fix or mitigation: clarify instrumentation hooks and boundary signals. Why it worked: operators could anchor on authoritative signals. Faster path: ask where tracing and metrics hook in. Guide prompt: "What observability surface proves each critical flow?"
- `#5102 We can't find the internet flash` Symptom: transient offline UI appears during navigation. Root cause: client transport and server handoff timing created a visible false-failure path. Concept: recovery and replay semantics. Evidence: issue is specific to page transitions on Phoenix 1.7 runtime. Fix or mitigation: tighten client/server transition logic so fallback messaging reflects true state. Why it worked: it aligned recovery signaling with actual connectivity. Faster path: ask which failure states are client-side artifacts. Guide prompt: "Which failure signals are authoritative versus cosmetic?"
- `#2998 Extensible schemes` Symptom: custom scheme support hits hard-coded assumptions. Root cause: URL and adapter scheme handling was less extensible than the visible API implied. Concept: external boundary handling. Evidence: issue notes hard-coded scheme assumptions. Fix or mitigation: make scheme handling extensible at the right boundary. Why it worked: the external boundary became configurable. Faster path: inspect how URI assumptions are encoded. Guide prompt: "Which external protocol assumptions are hard-coded?"
- `#1786 Provide send_attachment/3` Symptom: no first-class attachment boundary for responses. Root cause: Phoenix response ownership did not expose a common output pattern cleanly. Concept: public API versus generated API. Evidence: issue asks for download/inline mechanism. Fix or mitigation: expose response ownership explicitly with a higher-level helper. Why it worked: it reduced ad hoc response handling. Faster path: ask whether the output path lacks an explicit boundary helper. Guide prompt: "Which common output modes lack a first-class boundary?"
- `#1349 Can't see homepage at localhost:4000` Symptom: new app appears to start but homepage is unavailable. Root cause: startup success and endpoint availability are not the same boundary. Concept: boot and code loading. Evidence: issue reports no homepage after following guide. Fix or mitigation: tighten startup diagnostics around endpoint availability and runtime prerequisites. Why it worked: users could distinguish generated code from running server state. Faster path: verify endpoint server child and adapter startup first. Guide prompt: "Which child or adapter actually makes the boundary live?"
- `#2255 Presence race condition after state retrieval` Symptom: presence leave happens right after state retrieval, causing inconsistency. Root cause: realtime state is distributed and time-sensitive across PubSub/process boundaries. Concept: concurrency and serialization. Evidence: issue is explicitly a race around presence state. Fix or mitigation: make state reconciliation idempotent and time-aware. Why it worked: it acknowledged independent state owners. Faster path: inspect message ordering and reconciliation, not just data shape. Guide prompt: "Which state owners can change independently between snapshot and event?"
- `#835 Provide a phoenix-static buildpack` Symptom: deployment needs asset compilation outside the app node. Root cause: runtime health depends on build pipeline shape. Concept: external boundary handling. Evidence: issue asks for Heroku buildpack split. Fix or mitigation: separate build-time asset work from runtime server boundary. Why it worked: deploy architecture matched actual responsibilities. Faster path: ask whether build and serve happen in one place or two. Guide prompt: "Which artifacts are built here and which are only served here?"
- `#10 Routing DSL changes` Symptom: route DSL design shapes how developers understand dispatch. Root cause: router macros are a major architecture surface. Concept: compile-time versus runtime. Evidence: issue directly discusses DSL shape. Fix or mitigation: keep routing DSL aligned with compiled dispatch model. Why it worked: the visible syntax matched the real generated structure. Faster path: inspect what the DSL expands into. Guide prompt: "What runtime dispatch does this DSL generate?"
- `#1493 Error with new version of brunch` Symptom: generated app breaks after asset dependency release. Root cause: framework startup was coupled to external asset tool versions. Concept: environment-specific runtime differences. Evidence: issue says default project fails after new brunch release. Fix or mitigation: pin or regenerate compatible tool config. Why it worked: the external dependency contract became explicit. Faster path: compare generated tool versions before app code. Guide prompt: "Which external dependency versions materially shape this runtime?"
- `#2914 Fallback to LongPoll not working on iOS` Symptom: websocket fallback fails on iOS/proxy path. Root cause: browser and proxy transport behaviour is part of the realtime architecture. Concept: distributed or external boundary handling. Evidence: issue references known iOS websocket/proxy behaviour. Fix or mitigation: harden and test longpoll fallback behaviour. Why it worked: recovery moved to the right transport layer. Faster path: inspect transport compatibility matrix early. Guide prompt: "Which client and proxy transport behaviours shape the realtime path?"
- `#1192 Provide --no-html / --api on phoenix.new` Symptom: generated project shape includes unneeded runtime surfaces. Root cause: scaffolding influences architecture understanding and accidental complexity. Concept: supervision and lifecycle. Evidence: issue asks for API-only generation. Fix or mitigation: generate only the needed runtime boundary set. Why it worked: it reduced accidental architecture. Faster path: ask whether generated components are essential. Guide prompt: "Which generated surfaces are required versus incidental?"
- `#1395 mix test hangs on inet_gethost` Symptom: test runtime hangs in host lookup. Root cause: DNS and host services alter runtime behaviour in tests. Concept: test-runtime mismatch. Evidence: issue is a test hang in name resolution. Fix or mitigation: isolate or correct host lookup behaviour in test setup. Why it worked: the failure was environmental. Faster path: inspect host/network assumptions in tests first. Guide prompt: "Which host services are only required in this runtime mode?"
- `#3121 Cowboy2Handler.child_spec undefined` Symptom: app crashes because expected handler child spec is missing. Root cause: server adapter contract drifted from Phoenix expectations. Concept: callback or behaviour boundaries. Evidence: issue is an undefined child spec on server start. Fix or mitigation: align adapter boundary and version compatibility. Why it worked: startup depended on the contract, not endpoint code. Faster path: inspect adapter/version boundary first. Guide prompt: "Which callback or child-spec contract does this runtime expect?"
- `#1544 REST API performance problem` Symptom: Phoenix API underperforms expectation. Root cause: throughput and serialization questions need concrete runtime tracing instead of framework assumptions. Concept: observability and proof. Evidence: issue compares basic API behaviour to expectations. Fix or mitigation: trace request path, DB work, serialization, and adapter limits instead of blaming the framework surface. Why it worked: the bottleneck moved to a measurable owner. Faster path: inspect the hottest serialized boundary. Guide prompt: "What serializes this request path under load?"
- `#1720 mix phoenix.server does not start` Symptom: generated project cannot boot. Root cause: startup chain depends on Hex/tooling availability before endpoint code exists. Concept: boot and code loading. Evidence: issue says Hex could not start. Fix or mitigation: fix toolchain bootstrap and dependency state. Why it worked: the app code was never the real boundary. Faster path: check generator and dependency boot path first. Guide prompt: "Which package or tool boot path precedes the reviewed system?"
- `#5741 Longpoll fallback preserved after server restart` Symptom: client remains stuck on fallback after restart/reload. Root cause: client reconnect state and server transport recovery were misaligned. Concept: recovery and replay semantics. Evidence: issue describes fallback persistence after restart. Fix or mitigation: reset or renegotiate transport state correctly. Why it worked: recovery matched the actual owner of connection state. Faster path: inspect who owns reconnect and transport-selection memory. Guide prompt: "After restart, what state is rebuilt and what stale client state survives?"
- `#248 RFC incoming/outgoing channel events` Symptom: channel event structure lacked a clear contract. Root cause: inbound and outbound realtime messages needed explicit boundary semantics. Concept: callback boundaries. Evidence: issue is an RFC on channel events. Fix or mitigation: formalize event shape and lifecycle. Why it worked: the realtime contract became inspectible. Faster path: ask what message contract exists at the channel boundary. Guide prompt: "What is the exact contract of inbound and outbound messages?"
- `#70 RFC feedback on websocket/channels/pubsub implementation` Symptom: websocket/pubsub architecture was still being shaped. Root cause: channel and pubsub architecture needed explicit boundaries around distribution and fan-out. Concept: distributed or node-boundary handling. Evidence: issue is a design RFC for websocket/pubsub. Fix or mitigation: define boundaries between transport, pubsub, and channel process responsibilities. Why it worked: ownership became clearer. Faster path: separate transport, fan-out, and per-channel state early. Guide prompt: "Which subsystem accepts the connection, which fans out messages, and which owns per-client state?"

## Fast Path Heuristics

1. In Phoenix, inspect what macros generated before you trust the file layout.
2. Split config into compile-time, runtime, and environment-specific topology changes immediately.
3. For realtime bugs, name the channel process, PubSub, transport, proxy, and client as separate owners.
4. For boot or request failures, check adapter and proxy boundaries before controller code.
5. For dev-only failures, inspect code reloader and watchers as part of the runtime, not as tooling outside it.
6. If recovery feels inconsistent, ask whether the client, not the server supervisor, owns part of the recovery path.

## Concrete Steps

- `curl -sL https://api.github.com/repos/phoenixframework/phoenix`
- `curl -sL https://api.github.com/repos/phoenixframework/phoenix/branches/main`
- `curl -sL https://api.github.com/repos/phoenixframework/phoenix/contents/lib/phoenix/endpoint.ex?ref=850162d618cf4e31be82fb53ad6e259095fa6126`
- `curl -sL https://api.github.com/repos/phoenixframework/phoenix/contents/lib/phoenix/endpoint/supervisor.ex?ref=850162d618cf4e31be82fb53ad6e259095fa6126`
- `curl -sL https://api.github.com/repos/phoenixframework/phoenix/contents/lib/phoenix/router.ex?ref=850162d618cf4e31be82fb53ad6e259095fa6126`
- `curl -sL https://api.github.com/repos/phoenixframework/phoenix/contents/lib/phoenix/channel/server.ex?ref=850162d618cf4e31be82fb53ad6e259095fa6126`
- `curl -sL "https://api.github.com/search/issues?q=repo:phoenixframework/phoenix+is:issue+is:closed&sort=comments&order=desc&per_page=25&page=1"`
- `curl -sL "https://api.github.com/search/issues?q=repo:phoenixframework/phoenix+is:issue+is:closed&sort=comments&order=desc&per_page=25&page=2"`

After this section, you should be able to answer: how do you reproduce the evidence and continue the review safely?

## Surprises & Discoveries

- Phoenix is easier to orient visually than Elixir core, but the compile-time/runtime split is still critical.
- The most repeated real-world Phoenix failures sit below controller code: assets, adapters, proxies, transports, and environment-specific runtime topology.
- Realtime recovery often depends on client and transport state, not only on supervisor restart semantics.

After this section, you should be able to answer: what changed your understanding during the work?

## Decision Log

- Decision: keep the scoped review on endpoint, router, and channel architecture instead of branching into LiveView internals.
  Rationale: the milestone only required one request flow and one realtime flow.
  Evidence: endpoint supervisor, router macros, and channel server already exposed the key missing guide prompts.
  Date/Author: 2026-03-07 / Codex

- Decision: replace low-value high-comment issues with later architecture-useful issues from page 2.
  Rationale: the plan requires real-world architecture evidence, not branding or documentation-only items.
  Evidence: page 1 contained several high-comment issues with low architectural value.
  Date/Author: 2026-03-07 / Codex

After this section, you should be able to answer: why did the review take its current shape?

## Validation and Acceptance

- You can point to the endpoint startup path and name the major children it starts.
- You can distinguish compile-time router generation from runtime request handling.
- You can trace one channel join into its own process and one PubSub/broadcast path.
- You can explain which deployment or browser boundaries can fail before Phoenix app code is wrong.
- You can name at least one recovery path owned by supervision and one owned by reconnect or transport behaviour.
- You can show which issue patterns would be faster to solve if the guide prompted config split, transport boundaries, and generated surfaces explicitly.

After this section, you should be able to answer: how do you know this review is complete enough to hand off safely?

## Open Questions / Blockers

- No architecture blockers remain for the scoped review.
- Precision note: some Phase 3 resolution summaries are framework-direction inference rather than full-thread quotation.

After this section, you should be able to answer: what is still unknown and why does it matter?

## Next Handoff

Hand off to the cross-app synthesis pass so the recurring gaps from this review can be converted into dependency-neutral additions to `.agent/ARCHITECTURE_REVIEW.md`.

After this section, you should be able to answer: what should happen after this review and why?

## Outcomes & Retrospective

This review showed that Phoenix architecture becomes much easier to understand when you explicitly model compile-time route generation, config split, and realtime ownership boundaries. Those are generic Elixir review techniques, not Phoenix-specific tricks, which makes them strong candidates for the revised architecture-review guide.

After this section, you should be able to answer: what did this review achieve overall?

## Change Log

- (2026-03-07 09:34Z) Change: Created the Phoenix benchmark ArchitectureReview. Reason: collect Phase 1-3 evidence before revising `.agent/ARCHITECTURE_REVIEW.md`.

After this section, you should be able to answer: how did the document evolve over time?
