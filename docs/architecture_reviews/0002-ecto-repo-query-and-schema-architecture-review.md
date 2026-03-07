# Review Ecto Repo, Query, and Schema Architecture

This ArchitectureReview is a living document. Keep it up to date as review coverage expands, findings are refined, mitigation directions change, blockers appear, and the next handoff becomes clearer.

If `.agent/ARCHITECTURE_REVIEW.md` is checked into the repository, maintain this ArchitectureReview in accordance with that file.

## Status

**Resolved.** Phase 1, Phase 2, and Phase 3 are complete enough to support cross-app synthesis and guide revision.

After this section, you should be able to answer: is this review open, blocked, ready for handoff, or resolved?

## Current State Snapshot

You can now explain the main read path from a generated `Repo.all/2` call to query normalization, planning, adapter dispatch, and repo supervision setup. The current guide worked well for state and flow tracing here, but it did not force enough attention onto compile-time schema generation, generated repo APIs, dynamic repo routing, or the adapter boundary as the real architecture contract.

The issue evidence is grounded in the official Ecto repository issue list and local vendored source at version `3.13.5`. As with the Elixir review, some issue fix summaries are high-confidence architecture inference rather than direct quotation from every issue thread.

After this section, you should be able to answer: what is already known, what is still unknown, and what should you check next?

## At a Glance

- System under review: vendored `ecto` `3.13.5` repo, query, schema, and repo supervision surfaces
- Visible boundary: generated `Repo.all/2` on a module that `use`s `Ecto.Repo`
- Top risks: generated surfaces hiding the real owner, compile-time schema contracts, dynamic repo and adapter boundaries, query-planning assumptions
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

- [x] (2026-03-07 09:03Z) Anchored the review on generated `Repo.all/2`.
- [x] (2026-03-07 09:12Z) Read local vendored source in `deps/ecto` version `3.13.5`.
- [x] (2026-03-07 09:18Z) Completed Phase 1 inventory of unanswered guide questions.
- [x] (2026-03-07 09:24Z) Completed Phase 2 source-backed path tracing through repo, planner, schema, and supervisor code.
- [x] (2026-03-07 09:30Z) Collected the top 25 closed GitHub issues by comments and distilled fast-path heuristics.

After this section, you should be able to answer: what has been done, what is active now, and what remains?

## Purpose / Big Picture

You use this review to test the guide against a library whose most important architecture is split across generated APIs, compile-time schema metadata, a planner layer, and an adapter contract. Ecto is an ideal benchmark because a novice can recognize the high-level purpose quickly while still missing the real architecture if the guide does not ask about macros, query planning, or dynamic runtime routing.

After this section, you should be able to answer: why does this review exist and what useful understanding should it produce?

## Context and Orientation

The reviewed system is vendored locally in `deps/ecto`. The important files for the scoped path are:

- `deps/ecto/mix.exs` for version and application start information
- `deps/ecto/lib/ecto/application.ex` for top-level runtime state
- `deps/ecto/lib/ecto/repo.ex` for the generated repo API and dynamic repo lookup
- `deps/ecto/lib/ecto/repo/queryable.ex` for `all/2`, `get/3`, `stream/2`, and execution flow
- `deps/ecto/lib/ecto/repo/supervisor.ex` for repo startup, runtime config merge, and adapter child startup
- `deps/ecto/lib/ecto/schema.ex` for compile-time schema generation and `__schema__/1`
- `deps/ecto/lib/ecto/query/planner.ex` for query normalization and preparation

Terms used in this review:

- Adapter boundary: the point where Ecto stops owning the query path and the adapter becomes the real runtime owner.
- Generated repo API: functions created by `use Ecto.Repo`.
- Compile-time schema contract: metadata and reflection generated by `use Ecto.Schema`.

After this section, you should be able to answer: which files and runtime entrypoints should you read first?

## Request Restated

Review whether the current ArchitectureReview guide gives you enough prompts to understand Ecto architecture from the generated `Repo.all/2` boundary through schema/query planning and adapter dispatch.

After this section, you should be able to answer: what exact architecture question does this document own?

## Scope Boundaries

In scope:

- Generated repo read path (`Repo.all/2`)
- Repo startup and tuplet generation
- Query normalization, select enforcement, and planner handoff
- Compile-time schema generation and reflection relevant to query behaviour
- Adapter and dynamic repo boundaries

Out of scope:

- Full changeset and migration architecture
- SQL adapter implementation details outside the Ecto-owned boundary
- All write and transaction flows beyond the light issue-history context below

After this section, you should be able to answer: what does this review cover and what does it intentionally leave out?

## Visible Boundary

The review begins at a generated `Repo.all/2` function on a module that `use`s `Ecto.Repo`.

After this section, you should be able to answer: where does the review start in a way you can actually inspect or exercise?

## System Model

The Ecto read path is shaped by both compile-time generation and runtime dispatch:

- `use Ecto.Repo` generates functions such as `all/2` and `get/3`.
- Those generated functions call `get_dynamic_repo/0`, prepare options, and build an adapter tuplet via `Ecto.Repo.Supervisor.tuplet/2`.
- `Ecto.Repo.Queryable.all/3` converts inputs through `Ecto.Queryable.to_query/1`, ensures a select, and routes into planner and adapter execution.
- `Ecto.Query.Planner` owns normalization, preparation, cache usage, and adapter-facing query metadata.
- `Ecto.Repo.Supervisor` owns runtime startup, config merge, telemetry for repo init, query cache creation, and adapter child startup.
- `use Ecto.Schema` generates schema reflection such as `__schema__/1`, which influences how repo functions and query builders behave.

Boundary map:

    [Caller module]
          |
          v
    +------------------------------+
    | Ecto-owned architecture      |
    | - generated Repo API         |
    | - dynamic repo routing       |
    | - query normalization        |
    | - planner and cache          |
    | - schema reflection          |
    +------------------------------+
          |
          v
    [Adapter / datastore boundary]

Runtime topology:

    Ecto.Application
      `-- Ecto.Repo.Registry

    MyRepo.start_link/1
      `-- Ecto.Repo.Supervisor
          |-- adapter child(ren)
          `-- query cache / repo config state

Critical flow:

    Caller
      |
      v
    generated Repo.all/2
      |
      v
    get_dynamic_repo/0 + prepare_opts
      |
      v
    Ecto.Repo.Queryable.all/3
      |
      v
    Ecto.Queryable.to_query/1
      |
      v
    Ecto.Query.Planner.ensure_select/query
      |
      v
    adapter.execute / adapter.stream

Failure path traced from source:

- Invalid query shape or unsupported preload/stream combination fails in Ecto-owned code before the adapter path completes.
- Repo startup failures propagate from runtime config merge or adapter init in `Ecto.Repo.Supervisor`.
- Query execution errors can also be adapter-specific after Ecto hands off the prepared query.

Recovery path traced from source:

- Repo runtime recovery is supervisor-driven around the repo process and adapter child spec.
- Query-level recovery is mostly not automatic in Ecto core. The architectural question is whether state is reconstructed correctly on repo restart and whether the adapter boundary is observable enough.

After this section, you should be able to answer: where does work enter, where does state live, and which supervisors or processes own recovery?

## Constraints and Requirements

- Ecto must support generated APIs without hiding the real runtime owner too deeply.
- Query inputs can come from schemas, query DSL, or composable queryables, so compile-time and runtime contracts must align.
- Repo configuration can be static or dynamic at runtime.
- The adapter boundary must preserve enough metadata for logging, telemetry, planning, and caching.
- Version-sensitive scope reviewed here:
  - Vendored source: `ecto` `3.13.5`
  - `mix.lock`: `ecto` `3.13.5`

After this section, you should be able to answer: which limits decide whether the current architecture is adequate?

## Review Strategy

Traversal order used in this review:

1. Start from generated `Repo.all/2`.
2. Trace how generated code chooses the repo and builds the execution tuple.
3. Trace query conversion and planner ownership.
4. Trace compile-time schema metadata that affects runtime behaviour.
5. Trace repo startup and adapter initialization.
6. Compare recurring issue history to see where reviewers actually lose time.

First critical flow: generated `Repo.all/2` to adapter query execution.

First failure path: invalid or unsupported query shape before or during planner execution.

First recovery path: repo restart and re-init through `Ecto.Repo.Supervisor`.

After this section, you should be able to answer: what will you review first and why?

## Risk Areas Under Review

- [x] Entrypoints and boot path
- [x] Compile-time versus runtime
- [x] Public API versus generated API
- [x] Callback, behaviour, protocol, or macro boundaries
- [x] Supervision and lifecycle
- [x] State ownership and ownership transfer
- [x] Concurrency, queueing, or serialization
- [x] Failure spread and recovery
- [x] Observability and proof
- [x] Test-only or environment-specific runtime differences

After this section, you should be able to answer: which risks are already reviewed and which still need a pass?

## Phase 1 Inventory Notes

What the current guide already answered well:

- It made the visible boundary and critical flow explicit.
- It pushed the review toward state owners, failure paths, and restart behaviour.
- It already fit Ecto better than Elixir because the repo path looks more like a classic OTP subsystem.

What it did not force you to answer:

- Compile-time versus runtime: which behaviour comes from generated repo functions and schema macros.
- Public API versus generated API: whether `Repo.all/2` is the real owner or only a generated convenience wrapper.
- Callback and behaviour boundaries: where adapter contracts define the true architecture.
- State ownership: which parts of query state live in planner metadata, repo cache, process state, or adapter-owned runtime.
- Environment-specific runtime differences: dynamic repo configuration and compile warnings that only appear in certain setups.

Where the review had to invent its own logic:

- A generated-surface inventory for `use Ecto.Repo` and `use Ecto.Schema`
- A compile-time reflection pass for schema metadata
- An adapter contract map distinct from the public repo API

Diagrams the guide did not prompt strongly enough:

- Generated API to planner/adapter diagram
- Compile-time schema metadata to runtime query flow diagram

Architecture facts that felt fundamentally Elixir-shaped rather than library-shaped:

- Macros create architecture surfaces.
- Reflection functions such as `__schema__/1` are real runtime boundaries.
- Dynamic process-local routing such as `get_dynamic_repo/0` is architecture, not just configuration.

## Phase 2 Source Review Notes

Source-backed answers:

- Where does work first enter the system: generated `Repo.all/2` defined by `Ecto.Repo.__using__/1`.
- What code runs before the first user-visible boundary: repo module generation at compile time and repo startup through `Ecto.Repo.Supervisor`.
- Which parts are generated or expanded at compile time: repo functions and schema reflection.
- Which contracts define the real architecture: adapter callbacks, planner expectations, schema reflection, and repo supervisor config.
- Which processes or tables own truth: repo supervisor state, adapter child processes, query cache, and process-local dynamic repo routing.
- Where serialized work happens: repo selection and query planning are synchronous per call; later serialization belongs mainly to the adapter and datastore.
- What is rebuilt on restart and what is lost: repo startup replays config merge and cache creation; volatile in-process state is rebuilt.
- What signals prove the explanation: telemetry `[:ecto, :repo, :init]`, repo query events, stacktrace-enabled logs, and planner/runtime errors.

## Findings

Finding: Generated repo functions hide the real architecture owner unless you explicitly trace past them.
Impact: A novice can stop at `Repo.all/2` and miss dynamic repo routing, planner behaviour, and the adapter contract.
Trigger: Any `use Ecto.Repo` entrypoint.
Evidence: `Ecto.Repo.__using__/1` generates `all/2`, which immediately delegates into dynamic repo selection and `Ecto.Repo.Queryable`.
Threatened constraint: Fast and accurate architecture understanding from the visible boundary.
Blast radius: Every Ecto application and every library that wraps repo calls.

Finding: Compile-time schema metadata is architecture-critical.
Impact: Reviewers who only trace runtime calls miss how `__schema__/1` and generated reflection shape behaviour.
Trigger: Associations, queryable schemas, reload logic, or schema-driven queries.
Evidence: `Ecto.Schema` generates reflection used by query, reload, and association behaviour.
Threatened constraint: Correctly identifying the true owner of behaviour.
Blast radius: All schema-backed query paths.

Finding: The adapter boundary is the real contract boundary, not the public repo API.
Impact: Without naming the adapter contract, reviews blur Ecto-owned behaviour and datastore/adapter-owned behaviour.
Trigger: Query execution, startup, streaming, transaction, and dynamic repo config questions.
Evidence: `Ecto.Repo.Queryable` and `Ecto.Repo.Supervisor` both route into adapter-specific behaviour after planning and startup preparation.
Threatened constraint: Accurate blame and mitigation direction.
Blast radius: All adapter-backed Ecto systems.

Finding: Dynamic repo routing and runtime config belong in the main system model.
Impact: If you treat them as incidental configuration, you miss real state and ownership boundaries.
Trigger: Multiple repos, runtime config changes, sandbox/test setups, and multi-tenant patterns.
Evidence: issue history includes dynamic repo configuration and compile-time/runtime warnings around associations and queries.
Threatened constraint: Correct understanding of state ownership and environment divergence.
Blast radius: Multi-repo and test-heavy Ecto systems.

After this section, you should be able to answer: which risks are proven and which ones are still guesses?

## Mitigation Directions

Mitigation direction: Add a required "generated surface" prompt to the guide.
Why it helps: it forces you to map what `use` generated and where the runtime path actually goes next.

Mitigation direction: Add a compile-time reflection prompt.
Why it helps: it makes schema metadata, macros, and generated reflection part of the core review.

Mitigation direction: Add an explicit contract-boundary prompt.
Why it helps: it separates framework-owned behaviour from adapter-owned behaviour.

Mitigation direction: Add a runtime-routing prompt.
Why it helps: it surfaces dynamic repo selection, process-local state, and environment-specific runtime divergence.

After this section, you should be able to answer: what architectural move follows from each real finding?

## Guide Implications

Prompts missing from the guide for this app:

- "What did `use` generate here, and which generated function is your true starting point?"
- "Which compile-time reflection functions or metadata shape runtime behaviour?"
- "Where does framework ownership end and contract ownership by an adapter, callback, or external boundary begin?"
- "Which process-local or runtime configuration changes can alter the active owner without changing the call site?"
- "Which warnings or failures appear only because compile-time assumptions and runtime data disagree?"

## Issue Evidence

Method: official GitHub search query `repo:elixir-ecto/ecto is:issue is:closed sort:comments-desc`. These notes use issue body text, local source, and known Ecto architectural direction.

- `#2395 Adding default where clauses to schema` Symptom: users want schema-level default filters. Root cause: compile-time schema metadata and runtime query construction were not clearly separated. Concept: compile-time versus runtime. Evidence: issue proposes changing `__schema__(:query)`. Fix or mitigation: route default-query behaviour through explicit query metadata instead of hidden global scope. Why it worked: it made the schema/query contract explicit. Faster path: inspect `__schema__` surfaces before adding query behaviour. Guide prompt: "Which compile-time reflection API already owns this behaviour?"
- `#1114 Introducing Ecto.Multi` Symptom: transaction flows become hard to reason about when composed ad hoc. Root cause: transactional sequencing lacked a first-class data structure. Concept: state ownership and ownership transfer. Evidence: issue introduces a structure for multi-step transactions. Fix or mitigation: represent transactional intent explicitly. Why it worked: it made ownership and rollback ordering inspectible. Faster path: ask whether orchestration is hidden in call chains. Guide prompt: "Is there an explicit data structure for multi-step ownership changes?"
- `#978 Support non-public PostgreSQL schemas in model definition` Symptom: model definitions cannot cleanly target alternate DB schemas. Root cause: schema reflection and external datastore namespaces were too tightly coupled to defaults. Concept: external boundary handling. Evidence: issue asks for non-public schema support. Fix or mitigation: surface prefix/namespace explicitly in schema and migration contracts. Why it worked: the external boundary became named. Faster path: ask which external namespace assumptions are hard-coded. Guide prompt: "Which external namespace or prefix assumptions are embedded here?"
- `#2389 Support named joins` Symptom: complex query composition is hard to reason about without stable join identity. Root cause: query composition lacked explicit naming for generated query pieces. Concept: public API versus generated API. Evidence: issue checklist adds `:as` support and introspection. Fix or mitigation: expose names for generated join boundaries. Why it worked: composition became inspectible and less positional. Faster path: ask where identity is positional versus named. Guide prompt: "Which generated query components need stable names?"
- `#4293 Intermittent invalid association warnings at compile time` Symptom: compile-time warnings appear intermittently. Root cause: compile-time association resolution did not align cleanly with code loading order. Concept: compile-time versus runtime. Evidence: issue title and body are explicitly compile-time. Fix or mitigation: harden compile ordering and reflection lookup. Why it worked: the warning was about compile context, not runtime querying. Faster path: inspect compile order and reflection loading. Guide prompt: "Could this warning be an artifact of compile order rather than runtime state?"
- `#969 Add Repo.all_by as a mirror to Repo.get_by` Symptom: missing API symmetry causes repetitive call patterns. Root cause: generated repo surface did not expose a common query composition boundary clearly enough. Concept: public API versus generated API. Evidence: issue asks for a mirrored generated function. Fix or mitigation: expose first-class helpers when the architecture already supports them. Why it worked: it reduced repeated wrapper logic around a stable core path. Faster path: ask whether repeated wrapper code hides an existing primitive. Guide prompt: "What stable primitive is being rewrapped at call sites?"
- `#1966 Upsert does not read existing binary id` Symptom: upsert path does not preserve expected identity. Root cause: runtime write semantics and returned state were not aligned with data identity handling. Concept: state ownership. Evidence: issue is about reading existing binary id on upsert. Fix or mitigation: make datastore return-path semantics explicit in the adapter and schema contract. Why it worked: the owner of identity became explicit. Faster path: ask who owns identity after conflict resolution. Guide prompt: "After a conflict or merge, who owns the authoritative identity value?"
- `#635 Support multiple drivers for Mysql, Postgres, etc.` Symptom: users want alternate drivers. Root cause: adapter architecture and driver indirection were the real extension boundary. Concept: callback or behaviour boundaries. Evidence: issue asks for multiple drivers behind shared behaviour. Fix or mitigation: keep adapter contracts clean and driver-agnostic where possible. Why it worked: extensibility moved to the proper boundary. Faster path: ask whether the real abstraction is driver-specific or behaviour-specific. Guide prompt: "Which callback boundary is the true extension seam?"
- `#1964 Dynamic Repo configuration` Symptom: repo cannot always be configured ahead of compile time. Root cause: runtime routing and config ownership are architectural, not peripheral. Concept: test-runtime mismatch and environment-specific runtime differences. Evidence: issue asks for runtime-created repo behaviour. Fix or mitigation: support dynamic runtime repo configuration explicitly. Why it worked: ownership of config moved to runtime where it belonged. Faster path: ask whether repo identity is static, process-local, or runtime-created. Guide prompt: "How is the active owner selected at runtime?"
- `#1005 Support prefixes in migration runner` Symptom: migrations need namespace awareness. Root cause: operational tooling had to match schema prefix architecture. Concept: external boundary handling. Evidence: issue explicitly references prefixes. Fix or mitigation: carry namespace through operational paths, not just query paths. Why it worked: operational and runtime contracts matched. Faster path: compare operational tooling to runtime namespace behaviour. Guide prompt: "Do operational paths honor the same external namespace boundaries as runtime paths?"
- `#659 Add :order_by opt to has_many` Symptom: association loading order cannot be declared cleanly. Root cause: association reflection did not carry enough query metadata. Concept: compile-time reflection. Evidence: issue asks to declare order at association definition. Fix or mitigation: push stable association query metadata into reflection. Why it worked: the association contract became explicit. Faster path: inspect reflection metadata before writing per-call ordering workarounds. Guide prompt: "Which query defaults belong in reflection metadata instead of caller code?"
- `#2480 Queries in associations must be known at compile time` Symptom: association queries hit a compile-time limitation. Root cause: association definitions are generated at compile time and cannot safely depend on arbitrary runtime queries. Concept: compile-time versus runtime. Evidence: the issue title states the limitation directly. Fix or mitigation: keep association query parts compile-time-safe or move runtime variation elsewhere. Why it worked: it respected the generation boundary. Faster path: ask whether the query fragment is required during macro expansion. Guide prompt: "Does this boundary require a compile-time-safe value?"
- `#3017 embeds_one requires id field on cast_embed` Symptom: embed casting surprises callers with identity requirements. Root cause: ownership and lifecycle of embedded state were not obvious. Concept: state ownership. Evidence: issue is about embed identity during cast. Fix or mitigation: clarify or relax identity rules based on embed lifecycle semantics. Why it worked: it aligned the API with the true owner of embedded identity. Faster path: ask who owns identity for nested state. Guide prompt: "Who owns identity and replacement semantics for nested state?"
- `#569 Support reserving a worker` Symptom: a GenServer needs stable access to one Ecto worker. Root cause: connection/resource ownership transfer was not explicit enough. Concept: state ownership and ownership transfer. Evidence: issue describes one process needing a reserved worker. Fix or mitigation: make resource reservation and ownership explicit. Why it worked: serialization and handoff became visible. Faster path: ask whether one caller needs sticky ownership instead of pooled access. Guide prompt: "Is access pooled, reserved, or transferred?"
- `#2888 Latency spikes after updating to Ecto 3` Symptom: latency regressed after upgrade. Root cause: performance-critical behaviour changed at a lower layer than the visible query call. Concept: observability gaps. Evidence: issue reports latency spikes after version change. Fix or mitigation: inspect telemetry, planner changes, and adapter interaction rather than only query call sites. Why it worked: the bottleneck sat under the public API. Faster path: compare telemetry before and after upgrade. Guide prompt: "Which lower-layer timing signals prove where latency is really added?"
- `#3599 Mix test versus partitions performance difference` Symptom: test runtime differs sharply when partitioned. Root cause: test environment and runtime topology materially change behaviour. Concept: test-runtime mismatch. Evidence: issue compares partitioned and non-partitioned tests. Fix or mitigation: model test topology explicitly in the architecture review. Why it worked: the runtime under test was acknowledged as different. Faster path: ask how test concurrency/topology differs from normal runtime. Guide prompt: "What runtime model exists only in tests?"
- `#1272 Compose queries with OR` Symptom: users struggle to compose OR queries cleanly. Root cause: the DSL needed a clearer way to represent query composition semantics. Concept: generated API or delegation confusion. Evidence: issue calls out syntax design as blocker. Fix or mitigation: represent compositional intent explicitly in the query AST surface. Why it worked: the generated query matched mental model. Faster path: inspect AST composition rules before wrapper helpers. Guide prompt: "What AST shape is this surface trying to generate?"
- `#840 Problem with encoding Ecto model after upgrading to 0.14` Symptom: model encoding changed after metadata source change. Root cause: schema metadata leaked into serialization expectations. Concept: public API versus generated API. Evidence: issue mentions metadata source string-to-atom change. Fix or mitigation: isolate or normalize metadata before external encoding. Why it worked: serialization stopped depending on internal reflection format. Faster path: ask which internal metadata fields escape the boundary. Guide prompt: "Which internal reflection values leak into external representations?"
- `#2473 Decimal type always changes` Symptom: decimal fields look changed when they should not. Root cause: normalization and comparison semantics for custom types were unclear. Concept: state ownership. Evidence: issue title states persistent dirty tracking. Fix or mitigation: normalize comparison at the correct type boundary. Why it worked: the owner of equality moved to the type contract. Faster path: inspect type dump/load/equality semantics before blameing changesets. Guide prompt: "Who owns equality and normalization for this field type?"
- `#932 tcp connect: nxdomain when running mix ecto.migrate` Symptom: migration fails with host resolution error. Root cause: external network boundary and runtime config did not match the environment. Concept: external boundary handling. Evidence: issue is a DNS/connect failure. Fix or mitigation: correct host/runtime config and surface better diagnostics. Why it worked: the failure sat at the external boundary, not in migration logic. Faster path: verify network config before schema logic. Guide prompt: "Which external boundary can fail before any domain work begins?"
- `#2633 updated_at greater than inserted_at on creation` Symptom: timestamp ordering on create is surprising. Root cause: timestamp ownership and generation timing were not clear enough. Concept: state ownership. Evidence: issue compares two timestamps produced on one write. Fix or mitigation: define timestamp generation semantics precisely. Why it worked: callers stopped assuming a stronger invariant than the system provided. Faster path: ask who generates the timestamps and when. Guide prompt: "Who owns generated timestamps and what ordering is actually guaranteed?"
- `#1284 Issue when composing ecto queries` Symptom: composed query behaviour is unexpected. Root cause: query AST composition and merge semantics were not explicit enough. Concept: public API versus generated API. Evidence: issue title is about composition failure. Fix or mitigation: clarify and harden merge semantics in the DSL. Why it worked: query composition matched the actual planner contract. Faster path: inspect planner and AST merge rules, not only surface syntax. Guide prompt: "How are repeated query clauses merged or overridden?"
- `#1207 mix ecto.create without password but migrate works` Symptom: one operational command works while another fails with the same config. Root cause: operational paths exercised different config or connection assumptions. Concept: environment-specific runtime differences. Evidence: issue compares create and migrate behaviour. Fix or mitigation: align operational command config handling. Why it worked: both paths now used the same real boundary assumptions. Faster path: compare command-specific config loading paths. Guide prompt: "Do neighboring operational paths load configuration the same way?"
- `#135 Composite primary keys` Symptom: users want composite PK support. Root cause: identity assumptions are built deep into query, changeset, and schema contracts. Concept: state ownership. Evidence: issue asks whether technical reasons prevent support. Fix or mitigation: keep identity assumptions explicit, because changing them affects many boundaries. Why it worked: it revealed that identity is an architecture choice, not just a schema option. Faster path: ask which invariants assume one primary key. Guide prompt: "Which contracts assume one stable identity field?"
- `#535 Timestamps with time zone broken` Symptom: timezone timestamp behaviour is wrong or unclear. Root cause: external datastore semantics and type normalization were misaligned. Concept: external boundary handling. Evidence: issue reports broken timezone behaviour. Fix or mitigation: align type handling with datastore timezone semantics. Why it worked: the type contract matched the external boundary again. Faster path: inspect datastore type semantics before app-layer code. Guide prompt: "Which external type semantics does this abstraction rely on?"

## Fast Path Heuristics

1. When a library uses `use`, inspect what code was generated before you trust the visible call site.
2. If a query path is confusing, trace the AST and planner before the adapter.
3. If behaviour depends on schemas or associations, inspect compile-time reflection metadata.
4. If multi-repo or runtime config exists, treat active-repo selection as a state owner.
5. If a failure involves performance or semantics after upgrade, compare planner and telemetry layers before rewriting call sites.
6. If a bug is only in tests or migrations, compare runtime topology and config loading between those operational modes.

## Concrete Steps

- `sed -n '1,140p' deps/ecto/mix.exs`
- `sed -n '1,200p' deps/ecto/lib/ecto/application.ex`
- `sed -n '250,620p' deps/ecto/lib/ecto/repo.ex`
- `sed -n '1,220p' deps/ecto/lib/ecto/repo/queryable.ex`
- `sed -n '1,260p' deps/ecto/lib/ecto/repo/supervisor.ex`
- `sed -n '1,220p' deps/ecto/lib/ecto/query/planner.ex`
- `sed -n '1,260p' deps/ecto/lib/ecto/schema.ex`
- `jq -r '.items[] | [.number,.title,.comments] | @tsv' /tmp/ecto_issues.json`

After this section, you should be able to answer: how do you reproduce the evidence and continue the review safely?

## Surprises & Discoveries

- The guide feels naturally closer to Ecto than to Elixir CLI, but it still under-prompts compile-time generation and adapter contracts.
- `get_dynamic_repo/0` is easy to dismiss as "configuration" even though it changes the real owner at runtime.
- The issue history repeatedly turns seemingly library-specific questions back into core Elixir concerns: macros, reflection, runtime routing, and environment divergence.

After this section, you should be able to answer: what changed your understanding during the work?

## Decision Log

- Decision: review Ecto from the generated repo read path instead of a transaction path.
  Rationale: the plan specified `Repo.all/2` as the starting boundary.
  Evidence: generated `all/2` leads quickly into the main architecture surfaces.
  Date/Author: 2026-03-07 / Codex

- Decision: emphasize schema reflection even though the entrypoint is repo-focused.
  Rationale: the source and issue history both showed that generated schema metadata is one of the real owners of query behaviour.
  Evidence: `Ecto.Schema` and issues around associations, prefixes, and query metadata.
  Date/Author: 2026-03-07 / Codex

After this section, you should be able to answer: why did the review take its current shape?

## Validation and Acceptance

- You can point to the generated `Repo.all/2` boundary and name the next real owner.
- You can distinguish compile-time generation from runtime query execution.
- You can explain where adapter ownership begins.
- You can name the main state owners: repo supervisor, query cache, schema reflection, and dynamic repo routing.
- You can trace one failure path and one restart path from source.
- You can show which issue clusters would have been faster to review if the guide prompted generated surfaces and compile-time reflection explicitly.

After this section, you should be able to answer: how do you know this review is complete enough to hand off safely?

## Open Questions / Blockers

- No architecture blockers remain for the scoped review.
- Precision note: not every issue thread was read in full; some fix summaries are architectural inference from issue statement, local source, and later Ecto design direction.

After this section, you should be able to answer: what is still unknown and why does it matter?

## Next Handoff

Hand off to the cross-app synthesis pass so the recurring gaps from this review can be converted into dependency-neutral additions to `.agent/ARCHITECTURE_REVIEW.md`.

After this section, you should be able to answer: what should happen after this review and why?

## Outcomes & Retrospective

This review showed that even a library that already looks structurally "Elixir-like" still needs stronger guide prompts for generated APIs, compile-time reflection, adapter contracts, and runtime routing. Ecto confirms that the revised guide must stay dependency-neutral while still teaching readers how to uncover hidden macro and contract boundaries quickly.

After this section, you should be able to answer: what did this review achieve overall?

## Change Log

- (2026-03-07 09:30Z) Change: Created the Ecto benchmark ArchitectureReview. Reason: collect Phase 1-3 evidence before revising `.agent/ARCHITECTURE_REVIEW.md`.

After this section, you should be able to answer: how did the document evolve over time?
