# Architecture Decision Records (ADR):

This document describes the requirements for an Architecture Decision Record (“ADR”), a design document that captures a specific technical decision, why it was made, and what must be true in the repository as a result. Treat the reader as a complete beginner to this repository: they have only the current working tree and the single ADR file you provide. There is no memory of prior ADRs and no external context.

## How to use ADRs and ADRS.md

When authoring an executable Architecture Decision Record (“ADR”), follow ADRS.md to the _letter_. If ADRS.md is not in your current context, refresh your memory by reading the entire file before writing or revising the ADR. Read (and re-read) all source material you rely on so the ADR is accurate. Start from the ADR skeleton, then flesh it out as you do your research.

When working on an ADR, do not ask the user for “next steps.” Move directly to the next milestone. Keep every section current. At every stopping point, update the milestone list by adding, splitting, or rewriting entries so it always states: (1) what is already done, and (2) what is next. Resolve ambiguities proactively inside the ADR and explain the chosen interpretation. Commit frequently.

When discussing an ADR, record decisions in a decision log inside the ADR. It must be unambiguous why any change to the ADR was made. Treat ADRs as living documents: it must always be possible to restart work using only the current ADR and the repository working tree.

When a proposed design includes hard constraints, unfamiliar domains, or unanswered technical questions, plan a sequence of explicit validation milestones. Each milestone must produce a small, disposable proof of concept that tests one assumption in isolation. Examples include toy implementations, targeted benchmarks, or minimal integrations. The goal is not to ship production code. The goal is to produce observable evidence that the direction is viable.

Do not make decisions without evidence. Prefer primary sources. Acquire and read the source code (or official documentation) of any libraries and frameworks under consideration instead of relying on secondary descriptions. Record the exact modules, functions, and extension points that confirm or contradict the intended approach. Summarize those findings and connect them directly to the decision they support.

Include prototype results in the ADR as the primary justification for the chosen option. State what was attempted, how it was run, and what behaviour was observed. Make clear which unknowns were resolved, what remains unknown, and how the outcome changes the final architecture.

Mark all proof-of-concept artifacts as non-production. Define explicit criteria for either discarding them or promoting the learned approach into the real implementation.

# Requirements

NON-NEGOTIABLE REQUIREMENTS:

* Every ADR must be fully self-contained. Self-contained means that in its current form it contains all knowledge and instructions needed for a novice to succeed.
* Revise it when new information appears, when an assumption is proven wrong, when an alternative is rejected, or when the decision is superseded. Each revision must remain complete on its own and must clearly state what is now true and what is no longer true. Each revision must remain fully self-contained.
* Every ADR must explain the decision, not just the outcome. A novice with no context must be able to read the document and answer: what problem existed, what options were considered, what evidence was gathered, why one option was chosen, and what trade-offs were accepted.
* Every ADR must describe a real, observable effect. Record what becomes possible, safer, faster, cheaper, or easier as a result of the decision. Do not stop at “the code was reorganized” or “a new abstraction was introduced.” State how a human can see the impact by running the system, executing a command, or following an operational workflow.
* Every ADR must define every term of art in plain language or do not use it.

### Purpose and intent come first.

Begin by explaining, in a few sentences, why this decision matters. State the current limitation, who is affected, and what they can do after this decision that they could not do before. Show how to verify the outcome: the command to run, the endpoint to call, the test that now passes, or the operational step that is no longer required.

Then guide the reader through the reasoning that leads to the decision: the constraints, the options, the evaluation criteria, the evidence, and the selected approach.

### Assume zero prior context.

The reader can list files, read code, search the repository, run the application, and run tests. The reader cannot infer intent from earlier ADRs, planning documents, or conversations.

### Repeat every assumption you rely on.

Do not point to external blogs or documentation for required knowledge. If a concept is necessary to understand the decision, explain it in this record in your own words.

If this ADR builds on a previous ADR that is checked in, reference it by repository-relative path and restate the specific constraints or conclusions you depend on. If it is not present in the repository, include all relevant context here.

### Ground the decision in evidence.

When research, prototypes, or source-code inspection inform the outcome, record:

- what was examined
- how it was exercised
- what was observed
- which uncertainty it resolved

The ADR must allow a future reader to understand why this option was chosen without repeating the entire investigation.

### Prefer verifiable behaviour over structural description.

Write acceptance in a way that a human can verify it:

  - run a command and check for one specific output
  - start the app, call one specific endpoint, and check for one specific response
  - run the test suite and see it fail before the change, then pass after the change

If the decision is internal, explain how a human can still see its impact from outside the system (for example, through an endpoint response, logs, or a test that exercises the behaviour).

### Make supersession explicit.

When a decision changes, mark this ADR as superseded, link to the replacing record, and state what is no longer true. The history of decisions must remain understandable without external explanation.

## Formatting

Format and envelope are simple and strict. Each ADR must be one single fenced code block labeled as `md` that begins and ends with triple backticks. Do not nest additional triple-backtick code fences inside; when you need to show commands, transcripts, diffs, or code, present them as indented blocks within that single fence. Use indentation for clarity rather than code fences inside an ADR to avoid prematurely closing the ADR's code fence. Use two newlines after every heading, use # and ## and so on, and correct syntax for ordered and unordered lists.

When writing an ADR to a Markdown (.md) file where the content of the file *is only* the single ADR, you should omit the triple backticks.

Write in plain prose. Prefer sentences over lists. Avoid checklists, tables, and long enumerations unless brevity would obscure meaning. Checklists are permitted only in the `Progress` section, where they are mandatory. Narrative sections must remain prose-first.

## Guidelines

An ADR records a decision. It is not a brainstorming document.

Self-containment and plain language are paramount. If you introduce a phrase that is not ordinary English ("GenServer", "middleware", "RPC"), define it immediately and remind the reader how it manifests in this repository (for example, by naming the files or commands where it appears). Do not say "as defined previously" or "according to the architecture doc." Include the needed explanation here, even if you repeat yourself.

Do not rely on undefined jargon. Do not outsource key decisions to the reader. When multiple paths exist, select one, state why it was chosen, and note the rejected alternatives briefly. Prefer over-explaining user-visible effects and under-specifying incidental implementation detail.

State what a human can do after the change, the exact commands to run, and what they will see. Phrase acceptance as externally verifiable outcomes rather than internal structures. For internal changes, show how to prove the impact through failing-before / passing-after tests or a minimal end-to-end scenario.

Name files using full repository-relative paths. Name modules and functions precisely. State where new files are created. When multiple areas are touched, include a short orientation that explains how they relate so a novice can navigate confidently. Show the working directory for every command. State environment assumptions and reasonable alternatives.

Write steps that can be run multiple times without causing drift. For operations that can fail midway, include retry instructions. For destructive or migratory changes, require backups or safe fallbacks. Prefer additive, testable changes that can be validated incrementally.

Running tests and exercising the system is mandatory. Provide the exact commands for the project’s toolchain and how to interpret the results. Include expected outputs and common failure signals so a novice can distinguish success from error. Demonstrate effectiveness beyond compilation using a concrete scenario, CLI invocation, or HTTP transcript.

Include concise terminal output, logs, or small diffs that prove the outcome. Prefer minimal, file-scoped excerpts that can be recreated by following the steps. Evidence must show that the system behaves differently in a meaningful way.

An ADR that is written well allows a novice to understand what was decided, why it was decided, what was not chosen, and what will happen because of this decision.

An ADR following this decision is complete only if:
- A novice can execute it end-to-end.
- Success and failure are externally distinguishable.
- All paths that require choice are already decided or explicitly constrained.
- Re-running the steps produces the same result without cleanup.

## Milestones

Milestones should read like a story, not like paperwork. If you split the work into milestones, introduce each one with a short paragraph that explains what it covers, what will exist at the end that did not exist before, which commands to run, and what specific acceptance a human should observe. Keep the flow readable as: the goal, the work you will do, the result you get, and the proof you can check.

Milestones and progress are not the same thing. Milestones describe the narrative steps of the work. Progress tracks the smaller, day-to-day tasks inside those steps. Both must be present and kept up to date.

Do not shorten a milestone just to make it smaller. If a detail would matter to someone implementing this later, include it.

Each milestone must stand on its own. A person should be able to run the checks for that milestone and confirm it is complete. Each milestone should also move the overall ADR decision forward in a small, clear step.

## Living plans and design decisions

ADRs are living documents. When you make a key design decision, update the ADR to record both the decision and the reason for it. Record every decision in the Decision Log section.

ADRs must include and keep current a `Progress` section, a `Surprises & Discoveries` section, a `Decision Log` section, and an `Outcomes & Retrospective` section. These sections are required.

When you discover optimizer behaviour, performance tradeoffs, unexpected bugs, or tricky “undo” semantics that affect the design, write them down in `Surprises & Discoveries` and include a small piece of evidence (test output is ideal).

If you change direction during implementation, record why in `Decision Log` and update `Progress` to match. The ADR is a guide for the next contributor, not just a checklist for the current one.

When you finish a major milestone (or the whole ADR), add an `Outcomes & Retrospective` entry that summarizes what you shipped, what is still missing, and what you learned.

## Prototyping milestones and parallel implementations

It is fine-and often a good idea-to add prototyping milestones when they reduce risk for a larger change. For example, you might add a small low-level operator to a dependency to prove it can be done, or you might try two different composition orders and measure how the optimizer behaves. Keep prototypes as additive, small, and testable as possible. Clearly label the work as “prototyping.” Explain how to run it, what to look for, and what result would count as success. State the rules for what happens next: what would cause you to keep the idea and move it into the real design, and what would cause you to throw the prototype away.

Prefer adding code first and only deleting later, while keeping tests passing the whole time. Parallel implementations are acceptable when they reduce risk, such as keeping a new adapter alongside an older path during a migration. If you do this, explain how to validate both paths and how you will safely remove the old one without breaking behaviour, using tests as the guardrail. When you are working with multiple new libraries or new feature areas at once, consider creating separate spikes that test each one on its own. Each spike should prove, in isolation, that the external library behaves the way you need and supports the features the final design depends on.

### Skeleton of a Good Architecture Decision Record (ADR)

    # ADR-XXXX: <Short decision>

    This ADR is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

    If `ADRS.md` is checked into the repo, reference the path to that file here from the repository root and note that this document must be maintained in accordance with `ADRS.md`.

    - **Status:** Proposed | Accepted | Superseded | Rejected
    - **Date:** YYYY-MM-DD
    - **Owners:** <Team or role>
    - **Decision Type:** Architecture | Operational | Security | Data | Other

    ## Purpose / Big Picture

    Explain in a few sentences what someone gains after this decision and how they can see it working. State the user-visible behaviour this decision enables.

    After this decision is implemented, a developer or operator can use the system in the new way during normal operation and observe the result at the boundary it affects. The system performs the newly supported action as part of the standard workflow, without requiring special steps or internal knowledge.

    A reader can verify that the decision is in effect by exercising the relevant path and observing the expected outcome using the usual tools for this system (for example, running the documented command, triggering the flow, or checking the metric, log, or UI state that reflects the change). The presence of this observable result, and the absence of the previous limitation or failure mode, confirms that the decision is working in practice.

    ## Context and Orientation

    Describe the current state relevant to this task as if the reader knows nothing. Name the key files and modules by full path. Define any non-obvious term you will use. Do not refer to prior plans.

    Example:

        We need a reliable way to run background work for the API:

        - Send transactional emails (password resets, receipts)
        - Generate exports that can take up to 60 seconds
        - Run periodic maintenance tasks
        - Ensure jobs survive deploys and node restarts
        - Provide visibility into failures and retries

        Current approach:

        - A Task.Supervisor starts async work from controllers.
        - In-flight work is lost on restart.
        - There is no retry policy and no durable queue.
        - Concurrency is uncontrolled during traffic spikes.

        Constraints:

        - Postgres already exists in all environments.
        - The system runs on multiple BEAM nodes.
        - We need operational visibility without building custom tooling.

    ## Decision

    State the decision in one clear sentence, using a strong verb (“Adopt…”, “Standardize…”, “Replace…”, “Deprecate…”).

    Then define the decision boundary:

    - What will exist after this ADR that does not exist today.
    - What parts of the system are in-scope for this decision (modules, services, runtime paths).
    - What parts are explicitly out-of-scope (to prevent “scope creep by implication”).

    Use two explicit lists:

    - **We will:** concrete commitments (libraries to adopt, defaults to enforce, data flows to support, behaviours to guarantee).
    - **We will not:** explicit non-goals (features, products, infra, or refactors that are intentionally deferred).

    Requirements for this section:

    - Avoid aspirational language (“should”, “ideally”). Prefer commitments (“must”, “will”).
    - Name the concrete mechanism: the library, storage, protocol, runtime component, or pattern being chosen.
    - Include any critical configuration decisions (e.g., queue concurrency, retention windows, partition keys, encryption mode).
    - If the decision has phases (migration), state the phase boundaries and what is true at the end of each phase.
    - If the decision introduces a compatibility constraint, state it (e.g., “must remain backward compatible with v1 clients”).

    Example:

        Adopt Oban as the background job system backed by Postgres.

        We will:

        - Use Oban for all durable async work
        - Store jobs in Postgres
        - Configure queues with explicit concurrency limits
        - Use job uniqueness for idempotent workflows

        We will not:

        - Introduce a new external queue in this phase
        - Use Oban Pro features in this ADR

    ## Alternatives Considered
  
    List the realistic alternatives that a reasonable engineer would expect you to evaluate.
    
    Include the following:

    - The “do nothing / keep current approach” option.
    - One option that is “build it ourselves”.
    - One option that is “adopt existing infra / managed service” (when applicable).

    For each alternative, create a subsection with a consistent structure:

        ### <Alternative name>

        - **Description:** One or two sentences describing what it is and how it would work in *this* system.
        - **Pros:** 2–5 bullets. Prefer operational and developer-experience benefits.
        - **Cons:** 2–5 bullets. Prefer concrete costs, risks, and failure modes.
        - **Rejected because:** One sentence that ties rejection directly to the Purpose, Context, and Constraints.

    Requirements for this section:

    - Compare alternatives against the same set of constraints (durability, operability, cost, complexity, latency, safety, etc.).
    - Do not strawman alternatives. Use the strongest version of each option.
    - If two alternatives are both viable, say what would cause you to revisit the choice later (a trigger condition).
    - If an alternative is only partially viable, say what missing capability blocks it today.

    Example:

        ### Task.Supervisor only

        - **Pros:** No new dependency
        - **Cons:** Not durable, no retries, no cross-node coordination
        - **Rejected because:** durability and retry are required

        ### Custom GenStage queue

        - **Pros:** Flexible and powerful
        - **Cons:** High implementation and operational cost
        - **Rejected because:** solves infrastructure we can adopt

        ### External queue (SQS or RabbitMQ)

        - **Pros:** Proven and decoupled
        - **Cons:** New infrastructure and operational overhead
        - **Rejected because:** Postgres-backed jobs meet current needs

    ## Plan of Work

    Describe, in prose, the sequence of edits and additions. For each edit, name the file and location (function, module) and what to insert or change. Keep it concrete and minimal.

    ## Concrete Steps

    State the exact commands to run and where to run them (working directory). When a command generates output, show a short expected transcript so the reader can compare. This section must be updated as work proceeds.

    ## Progress

    Use a list with checkboxes to summarize granular steps. Every stopping point must be documented here, even if it requires splitting a partially completed task into two (“done” vs. “remaining”). This section must always reflect the actual current state of the work.

    **Legend**

    This legend indicates the status of each step in the progress list.

    [ ] - Not started
    [~] - In progress
    [x] - Completed

    - [x] (2026-02-16 18:00Z) Defined requirements for durable background work (durability, retries, visibility, controlled concurrency).
    - [x] (2026-02-16 18:20Z) Chosen job system: Oban backed by Postgres; documented rationale and alternatives.
    - [~] (2026-02-17 00:00Z) Add Oban dependency, migrations, and runtime config; confirm Oban starts under supervision.
    - [ ] (2026-02-17 00:00Z) Create `MyApp.Workers.*` namespace and implement one minimal “smoke” worker.
    - [ ] (2026-02-17 00:00Z) Replace one existing `Task.Supervisor` call-site with an Oban enqueue; keep behaviour the same.
    - [ ] (2026-02-17 00:00Z) Add basic telemetry hooks and document how to inspect queues/jobs locally.
    - [ ] (2026-02-17 00:00Z) Write `Outcomes & Retrospective` after first production deploy.

    Use timestamps to measure rates of progress.

    ## Surprises & Discoveries

    Document unexpected behaviours, bugs, optimizations, or insights discovered during implementation. Provide concise evidence.

    - Observation: …
      Evidence: …

    ## Decision Log

    Record every decision made while working on the plan in the format:

    - Decision: …
      Rationale: …
      Date/Author: …

    ## Outcomes & Retrospective

    Summarize outcomes, gaps, and lessons learned at major milestones or at completion. Compare the result against the original purpose.

    ## Validation and Acceptance

    Describe how to start or exercise the system and what to observe. Phrase acceptance as behaviour, with specific inputs and outputs. If tests are involved, say "run <project’s test command> and expect <N> passed; the new test <name> fails before the change and passes after>".

    ## Idempotence and Recovery

    If steps can be repeated safely, say so. If a step is risky, provide a safe retry or rollback path. Keep the environment clean after completion.

    ## Artifacts and Notes

    Include the most important transcripts, diffs, or snippets as indented examples. Keep them concise and focused on what proves success.

    ## Implementation Plan

    Reference the ADR for detailed implementation steps.

    ## Follow-ups

    - ADR-XXXX: Standardize idempotency patterns
    - ADR-XXXX: Evaluate dedicated database resources for jobs

    ## Interfaces and Dependencies

    Be specific. Say exactly which libraries, modules, and services we will use and why. Name the Elixir modules and functions that must exist when this ADR is done. Prefer stable, predictable paths like `lib/my_app/module.ex:Module.function/arity` and stable module names like `MyApp.Module`.

    This section should answer:

    - What new dependency are we adopting (and why)?
    - What module(s) are the “front door” that the rest of the codebase must call?
    - What contracts (behaviours) must exist so the code is testable and replaceable?
    - What config keys must exist and where?
    - What logs/telemetry must exist so we can tell it works?

    ### External libraries and services

    For each external dependency, write:

    - **Name:** `<library/service name>`
    - **Why we need it:** `<one clear sentence>`
    - **Where it’s configured:** `<repo-relative file paths>`
    - **Who runs it:** `<which app/node/environment>`
    - **What to watch:** `<the 1–2 signals that tell us it’s healthy>`

    Example:

        - **Name:** `:oban`
        - **Why we need it:** We need durable background jobs with retries that survive restarts, using Postgres we already have.
        - **Where it’s configured:** `config/runtime.exs`, `lib/my_app/application.ex`, `priv/repo/migrations/*`
        - **Who runs it:** `MyApp` on every BEAM node in each environment.
        - **What to watch:** job failures and queue backlog/latency (via logs/telemetry).

    ### “Front door” modules (what other code is allowed to call)

    Do not let random parts of the codebase call third-party libraries directly. Put one small wrapper module in `MyApp.*` and make that the only supported entry point.

    State the rule:

    - Callers **must** use `MyApp.<Area>` (or `MyApp.<Area>.*`).
    - Callers **must not** call `<third-party module>` directly outside this boundary.
    - This module owns defaults, validation, and idempotence rules.

    Example:

    In `lib/my_app/jobs.ex`, define a single place to enqueue jobs:

        defmodule MyApp.Jobs do
          @type args :: map()
          @type opts :: keyword()

          @spec enqueue(module(), args(), opts()) :: {:ok, Oban.Job.t()} | {:error, term()}
          def enqueue(worker_module, args, opts \\ []) do
            ...
          end
        end

    What must be true when done:

    - `MyApp.Jobs.enqueue/3` exists.
    - All new enqueue call-sites use `MyApp.Jobs.enqueue/3`, not `Oban.insert/2` directly.

    ### Contracts (behaviours) we depend on

    If we want clean seams for testing or future changes, define a behaviour. A behaviour is just a clear contract: “a module that implements these functions.”

    State:

    - The behaviour module name.
    - The callback functions that must exist.
    - The concrete implementation module we will use right now.
    - How the app picks the implementation (config).

    Example:

    In `lib/my_app/<area>/adapter.ex`, define:

        defmodule MyApp.<Area>.Adapter do
          @callback perform(term()) :: {:ok, term()} | {:error, term()}
        end

    And choose an implementation module:

    - `MyApp.<Area>.Adapter.Oban` implements `MyApp.<Area>.Adapter`.

    What must be true when done:

    - The behaviour module exists.
    - At least one implementation exists.
    - The app calls through the behaviour (not the concrete module from everywhere).

    ### Worker and process modules (if this ADR adds them)

    If the decision adds workers (Oban workers, GenServers, supervisors), say what must exist and where.

    Example (Oban worker):

    In `lib/my_app/workers/<name>.ex`, define:

        defmodule MyApp.Workers.<Name> do
          use Oban.Worker, queue: :default

          @impl Oban.Worker
          @spec perform(Oban.Job.t()) :: :ok | {:error, term()}
          def perform(%Oban.Job{} = job) do
            ...
          end
        end

    What must be true when done:

    - The module exists under `MyApp.Workers.*`.
    - The queue name is documented (and configured).
    - We clearly define what counts as:
      - success (`:ok`)
      - retry (`{:error, reason}`)
      - discard (if we use it)

    ### Configuration we require

    Treat configuration as part of the interface. Name the keys and where they live.

    State:

    - Which config file(s) must include the settings.
    - The minimum required settings.
    - Any environment variables (names only).
    - What differs between dev/test/prod.

    Example:

    In `config/runtime.exs`, define:

        config :my_app, Oban,
          repo: MyApp.Repo,
          queues: [default: 10]

    What must be true when done:

    - The config exists in the correct file(s).
    - The app starts the dependency under supervision (see `lib/my_app/application.ex`).

    ### Data and persistence (if this ADR stores anything)

    If we create tables, migrations, ETS tables, or other persisted state, list the concrete names.

    State:

    - **Repo:** `MyApp.Repo` (if Postgres)
    - **Migrations:** `priv/repo/migrations/*`
    - **Tables/indexes added or changed:** `<names>`
    - **Cleanup/retention:** `<how old data is removed>`

    If there is sensitive data:

    - Say which fields are sensitive and how we protect them.

    ### What we log and measure (how we prove it works)

    Name the minimum signals that must exist so we can see the system working.

    At minimum, define:

    - What gets logged on success and failure.
    - One telemetry event or metric we can watch for:
      - success count
      - failure count
      - latency or backlog (if relevant)

    State where the code lives:

    - Either a dedicated module like `lib/my_app/<area>/telemetry.ex`
    - Or inside the boundary module (the “front door”) if it’s small.

    What must be true when done:

    - A developer can run the normal workflow and see the expected log/metric change.
    - An operator can tell “it’s healthy” vs “it’s stuck” from these signals.

    ### Compatibility and rollout rules

    State what must remain compatible while deploying:

    - If we deploy gradually, old and new nodes must both work at the same time (or we must state why not).
    - If a migration is required, say the safe order (example: “run migrations before enabling the new path”).
    - If we deprecate something, name what replaces it and when it can be removed.

    What must be true when done:

    - A rollout plan exists that avoids breaking running traffic.
    - We have a clear “safe to remove” condition for any old code we keep temporarily.

If you follow the guidance above, a single, stateless agent -- or a human novice -- can read your ADR from top to bottom and produce a working, observable result. That is the bar: SELF-CONTAINED, SELF-SUFFICIENT, NOVICE-GUIDING, OUTCOME-FOCUSED.

When you revise a plan, you must ensure your changes are comprehensively reflected across all sections, including the living document sections, and you must write a note at the bottom of the plan describing the change and the reason why. ADRs must describe not just the what but the why for almost everything.

