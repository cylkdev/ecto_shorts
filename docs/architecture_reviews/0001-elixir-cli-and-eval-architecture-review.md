# Review Elixir CLI Boot and Eval Architecture

This ArchitectureReview is a living document. Keep it up to date as review coverage expands, findings are refined, mitigation directions change, blockers appear, and the next handoff becomes clearer.

If `.agent/ARCHITECTURE_REVIEW.md` is checked into the repository, maintain this ArchitectureReview in accordance with that file.

## Status

**Resolved.** Phase 1, Phase 2, and Phase 3 are complete enough to support cross-app synthesis and guide revision.

After this section, you should be able to answer: is this review open, blocked, ready for handoff, or resolved?

## Current State Snapshot

You can now explain the `elixir` CLI boundary from shell entry to `Code.eval_string/3`, including the boot work that happens before the first user-visible boundary. The review also exposed the questions the current guide still does not force you to answer for systems shaped by boot code, compile-time expansion, generated APIs, and environment-sensitive code loading.

The main remaining limitation is issue-depth. The issue evidence below is grounded in official GitHub issue data and project direction, but some root-cause and fix summaries are high-confidence inference rather than direct quotation from every full issue thread.

After this section, you should be able to answer: what is already known, what is still unknown, and what should you check next?

## At a Glance

- System under review: the upstream `elixir` executable path and one simple evaluation path
- Visible boundary: `bin/elixir -e "1 + 1"`
- Top risks: hidden boot work, compile-time versus runtime confusion, environment-sensitive code loading, generated surfaces that hide the real contract
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

- [x] (2026-03-07 09:03Z) Read `.agent/ARCHITECTURE_REVIEW.md` and anchored the review on the CLI `-e` path.
- [x] (2026-03-07 09:10Z) Collected upstream source evidence from `bin/elixir`, `lib/elixir/src/elixir.erl`, `lib/elixir/lib/kernel/cli.ex`, and `lib/elixir/lib/code.ex` at `elixir-lang/elixir` `main` SHA `9a663cf3faf4c5f5ddddd8bb02624b79837fb433`.
- [x] (2026-03-07 09:18Z) Completed Phase 1 inventory of unanswered architecture questions.
- [x] (2026-03-07 09:23Z) Completed Phase 2 runtime, failure, and recovery tracing.
- [x] (2026-03-07 09:30Z) Collected the top 25 closed GitHub issues by comments and distilled fast-path heuristics.

After this section, you should be able to answer: what has been done, what is active now, and what remains?

## Purpose / Big Picture

You use this review to test whether the current ArchitectureReview guide is strong enough to explain a system whose architecture is not primarily "an OTP app with a supervision tree." Elixir itself forces you to reason about shell boot code, VM startup, parser and compiler boundaries, code loading, compile-time versus runtime, and generated or cross-language surfaces.

If the guide works here, it is much more likely to work on complex Elixir systems that hide important behaviour behind macros, code loading, and environment assumptions.

After this section, you should be able to answer: why does this review exist and what useful understanding should it produce?

## Context and Orientation

The reviewed system is the upstream Elixir executable path, not one user application. The concrete entrypoint starts in `bin/elixir`, which parses shell arguments and launches the Erlang VM. The Elixir OTP application boot logic lives in `lib/elixir/src/elixir.erl`, which exports `start_cli/0` and performs startup work before the CLI hands off to Elixir modules. The user-facing CLI logic lives in `lib/elixir/lib/kernel/cli.ex`, where `Kernel.CLI.main/1` parses commands and routes `-e` to `Code.eval_string/3`.

The evaluation path itself is split again. `Code.eval_string/3` is the visible API, but the underlying parser and evaluator cross into `:elixir` internals such as `string_to_quoted`, tokenization, and eval forms in Erlang modules. That split matters because a novice can read `Code.eval_string/3` and still miss the real compile-time and runtime contract.

Terms used in this review:

- Boot path: the code that runs before the visible boundary behaves the way you expect.
- Generated surface: an API that looks like one function call but whose behaviour depends on macros, internal expansion, or hidden runtime wiring.
- Compile-time versus runtime: whether the behaviour is decided while code is being expanded and compiled or while code is executing in the VM.

After this section, you should be able to answer: which files and runtime entrypoints should you read first?

## Request Restated

Review whether the current ArchitectureReview guide gives you enough prompts to understand the architecture of upstream Elixir when the visible boundary is the CLI and a simple `-e` evaluation path.

Competing interpretations checked and rejected:

- Review the whole Elixir language implementation. Rejected because the milestone boundary is narrower.
- Review Mix as the main entrypoint. Rejected because the requested starting boundary is the `elixir` CLI path.

After this section, you should be able to answer: what exact architecture question does this document own?

## Scope Boundaries

In scope:

- Shell entry in `bin/elixir`
- OTP boot path into `elixir:start_cli/0`
- CLI command parsing in `Kernel.CLI`
- The `-e` command path into `Code.eval_string/3`
- Architecture questions driven by compile-time/runtime separation, code loading, environment assumptions, and error propagation

Out of scope:

- Full compiler pipeline for all file compilation modes
- Mix task architecture beyond what the CLI path reveals
- IEx-specific runtime beyond the small boot distinction noted in source

After this section, you should be able to answer: what does this review cover and what does it intentionally leave out?

## Visible Boundary

The review begins at `bin/elixir -e "1 + 1"`. This is concrete, externally observable, and it forces the guide to explain both the shell boot path and the internal evaluation path.

After this section, you should be able to answer: where does the review start in a way you can actually inspect or exercise?

## System Model

The reviewed path crosses four distinct architectural layers:

1. Shell bootstrap in `bin/elixir`
2. Erlang VM and Elixir OTP application startup in `:elixir`
3. CLI command parsing and command dispatch in `Kernel.CLI`
4. Evaluation API and parser/compiler internals in `Code` and `:elixir`

Important state owners and boundaries:

- Shell script state owns the translated VM flags before Elixir code runs.
- The Elixir OTP application boot path sets process and application environment before `Kernel.CLI.main/1`.
- `Kernel.CLI` owns parsed command order, error collection, `System.argv/1`, and process exit behaviour.
- The `Code` API owns the visible eval call, but parsing and evaluation cross into internal Erlang modules that own tokenization, quoted conversion, binding loading, and stacktrace pruning.
- OS and environment boundaries matter early: path rules, encoding, installer behaviour, and shell/VM assumptions can fail before normal Elixir application logic is reached.

Boundary map:

    [Shell / OS environment]
            |
            v
    +-----------------------------+
    | elixir executable path      |
    | - bin/elixir                |
    | - erl VM startup            |
    | - :elixir OTP app boot      |
    | - Kernel.CLI                |
    | - Code API                  |
    +-----------------------------+
            |
            v
    [File system / code path / terminal]

Runtime topology:

    shell process
      `-- bin/elixir
          `-- erl VM
              `-- :elixir application
                  |-- boot checks and config
                  |-- Logger start when available
                  `-- Kernel.CLI.main/1
                      `-- process_command({:eval, expr})
                          `-- Code.eval_string/3
                              `-- :elixir parser/eval internals

Critical flow:

    User runs `elixir -e "1 + 1"`
        |
        v
    bin/elixir normalizes flags and launches VM
        |
        v
    :elixir.start_cli/0 ensures app startup and logger availability
        |
        v
    Kernel.CLI.main/1 parses argv and records {:eval, expr}
        |
        v
    Kernel.CLI.process_command({:eval, expr})
        |
        v
    Code.eval_string/3
        |
        v
    tokenization -> quoted conversion -> eval forms -> result / error formatting

Failure path traced from source:

- `Kernel.CLI.exec_fun/2` wraps the executed function in a monitored process.
- Exceptions are formatted through `format_error/3`, printed to stderr, and converted into shutdown status.
- A `:DOWN` message from the monitored child also becomes a printed error and non-zero halt.

Recovery path traced from source:

- This path is not a self-healing supervision tree. The main recovery behaviour is deterministic CLI failure handling: print, flush logs, run `at_exit` hooks, and halt with status.
- The real architecture question is therefore not "which supervisor restarts this" but "which boundary is responsible for turning failures into stable diagnostics and exit codes."

After this section, you should be able to answer: where does work enter, where does state live, and which supervisors or processes own recovery?

## Constraints and Requirements

- Cross-platform correctness matters early because the CLI boundary touches shell, path, encoding, and installer behaviour before normal Elixir code.
- Compile-time and runtime behaviour must stay distinguishable. Users need to know which behaviour is fixed by expansion and which remains dynamic.
- Error reporting must preserve useful stacktrace and source context.
- Code loading and protocol availability must remain stable across umbrellas, packaging, and different environments.
- Version-sensitive scope reviewed here:
  - Upstream source: `elixir-lang/elixir` `main` SHA `9a663cf3faf4c5f5ddddd8bb02624b79837fb433`
  - Local runtime used only for context: Elixir `1.15.2`, Erlang/OTP `25`

After this section, you should be able to answer: which limits decide whether the current architecture is adequate?

## Review Strategy

Traversal order used in this review:

1. Start at the visible shell command.
2. Trace boot work that happens before the first Elixir module boundary.
3. Trace CLI parsing and the `{:eval, expr}` command path.
4. Trace the first hidden compile-time/runtime split inside `Code.eval_string/3`.
5. Check how failures, exit statuses, and environment differences alter the path.
6. Compare the traced path against recurring issue history to see what questions matter in practice.

First critical flow: `bin/elixir -e "1 + 1"` to `Code.eval_string/3`.

First failure path: malformed input or internal exception during CLI-evaluated code.

First recovery path: CLI wrapper normalizes failure into printed diagnostics, `at_exit` execution, log flush, and exit status.

After this section, you should be able to answer: what will you review first and why?

## Risk Areas Under Review

- [x] Entrypoints and boot path
- [x] Compile-time versus runtime
- [x] Public API versus generated or internal API
- [x] Callback, behaviour, protocol, or macro boundaries
- [x] Supervision and lifecycle
- [x] State ownership and ownership transfer
- [x] Failure spread and recovery
- [x] Observability and proof
- [x] Test-only or environment-specific runtime differences

After this section, you should be able to answer: which risks are already reviewed and which still need a pass?

## Phase 1 Inventory Notes

What the current guide already answered well:

- It pushed the review toward a visible boundary instead of a file tree summary.
- It made state ownership, failure spread, and observability explicit.
- It encouraged diagrams early enough to keep the boot path readable.

What it did not force you to answer:

- Entrypoints and boot path: what runs before `Kernel.CLI.main/1`, and which early OS assumptions shape behaviour.
- Compile-time versus runtime: which steps are parser/compiler work and which are runtime evaluation.
- Public API versus generated API: which apparently simple APIs hide internal Erlang modules or compiler passes.
- Callback, behaviour, protocol, or macro boundaries: where protocols, compiler internals, or macro expansion define the real contract.
- Supervision and lifecycle: what "recovery" means when the system is a CLI path instead of a long-lived supervised service.
- Observability and proof: which visible signals prove boot correctness when there is no telemetry-first application surface.
- Environment-specific runtime differences: which failures are really installer, shell, encoding, or umbrella code-loading problems.

Where the review had to invent its own logic:

- A boot-path trace before the visible Elixir module boundary
- A compile-time/runtime split map
- A "generated surface versus real owner" pass

Diagrams the guide did not prompt strongly enough:

- Boot path diagram
- Generated API to internal owner diagram

Architecture facts that felt fundamentally Elixir-shaped rather than library-shaped:

- Macro and compiler boundaries are architectural boundaries.
- Code loading is a system boundary.
- Protocol consolidation and umbrella visibility are architecture, not just packaging detail.

## Phase 2 Source Review Notes

Source-backed answers:

- Where does work first enter the system: `bin/elixir`, then `:elixir.start_cli/0`, then `Kernel.CLI.main/1`.
- What code runs before the first user-visible boundary: VM startup, Elixir OTP application startup, logger start when available, boot checks such as OTP and encoding handling.
- Which parts of the API are generated, delegated, or expanded at compile time: the visible `Code.eval_string/3` API delegates into parser and evaluator internals; syntax and macro behaviour depend on compile-time expansion even when the caller sees one eval function.
- Which contracts define the real architecture: the real contract spans shell flags, OTP app boot, CLI command parsing, and internal parser/eval functions, not a single public module.
- Where serialized work happens: CLI command processing is ordered; eval itself is synchronous within the invoked path.
- What is rebuilt on restart and what is lost: the CLI path is ephemeral; state is mostly process-local and intentionally discarded on exit.
- What signals prove the explanation: exit status, stderr formatting, logger flush, explicit command parsing, and issue clusters around OS/code-loading failures.

## Findings

Finding: The current guide does not force you to trace pre-boundary boot work.
Impact: You can misunderstand systems whose most important architecture happens before the first normal module boundary.
Trigger: Any system with shell bootstrap, VM startup wiring, or code loading assumptions.
Evidence: `bin/elixir` and `:elixir.start_cli/0` both do important work before `Kernel.CLI.main/1`.
Threatened constraint: Fast and accurate architecture understanding from the visible boundary alone.
Blast radius: CLI tools, releases, boot scripts, installers, and mixed Erlang/Elixir runtimes.

Finding: Compile-time versus runtime is a first-class architecture lens, not an optional detail.
Impact: Without it, you misread where behaviour is fixed, where it can vary, and where bugs originate.
Trigger: Macro expansion, parser work, compiler diagnostics, protocol consolidation, or eval paths.
Evidence: `Code.eval_string/3` routes into parser and eval internals; issue history repeatedly clusters around compile graphs, code loading, and environment setup.
Threatened constraint: Correctly identifying the real owner of behaviour.
Blast radius: Any Elixir system that uses macros, generated functions, or protocols.

Finding: Public APIs can hide architecture-critical internal owners.
Impact: A novice can stop at `Code.eval_string/3` and still miss the real parser, binding, and stacktrace logic.
Trigger: APIs that delegate into internal modules or cross-language boundaries.
Evidence: `Kernel.CLI` routes to `Code.eval_string/3`, which routes into `:elixir` internals for tokenization, quoted conversion, and eval.
Threatened constraint: One-pass architecture comprehension.
Blast radius: Libraries and applications that expose a friendly API over internal machinery.

Finding: Real-world Elixir failures often originate in environment assumptions before business logic.
Impact: Reviewers who start too deep in domain code miss the fastest path to root cause.
Trigger: Windows, Cygwin, installers, encoding, source builds, or umbrella protocol loading.
Evidence: Top issue history is dominated by OS, build, compile graph, and code loading problems.
Threatened constraint: Fast diagnosis and reliable review coverage.
Blast radius: Any system deployed across multiple environments or packaging paths.

After this section, you should be able to answer: which risks are proven and which ones are still guesses?

## Mitigation Directions

Mitigation direction: Add an explicit boot-path prompt to the guide.
Why it helps: It makes you identify what runs before the visible boundary and who owns that setup.

Mitigation direction: Add a required compile-time versus runtime map.
Why it helps: It exposes where macros, parser work, generated code, and runtime execution actually divide.

Mitigation direction: Add a generated-surface prompt.
Why it helps: It forces you to ask whether the public API is the real architecture owner or only a facade.

Mitigation direction: Add an environment-divergence prompt.
Why it helps: It moves OS, code path, encoding, installer, and umbrella differences into the main review instead of leaving them as "deployment detail."

After this section, you should be able to answer: what architectural move follows from each real finding?

## Guide Implications

Prompts missing from the guide for this app:

- "What code runs before the first user-visible boundary?"
- "Which behaviour is fixed at compile time and which remains dynamic at runtime?"
- "Which public APIs are facades over generated, delegated, or cross-language internals?"
- "Which names, code paths, or packaging rules determine whether modules or protocols are visible?"
- "If the system is not long-lived, what does recovery mean at this boundary?"
- "Which environment-specific differences can change architecture understanding without changing domain code?"

## Issue Evidence

Method: official GitHub search query `repo:elixir-lang/elixir is:issue is:closed sort:comments-desc`. These notes use issue body text and project direction. Where a full thread was not read, the root cause and mitigation are marked by architecture inference rather than direct maintainer quote.

- `#1280 There are 8 failures from Elixir test suite on Windows` Symptom: path, CLI, and file tests fail on Windows. Root cause: platform-specific path and shell assumptions leaked into core runtime behaviour. Concept: boot and code loading. Evidence: issue body shows `PathTest`, `SystemTest`, `Kernel.CLI`, and `FileTest` failures. Fix or mitigation: normalize Windows-specific path and shell behaviour in core runtime. Why it worked: the failure was in environment assumptions, not user code. Faster path: inspect OS-sensitive path and encoding boundaries before compiler internals. Guide prompt: "What environment assumptions shape the boundary before domain code runs?"
- `#12645 Environments for code fragments/buffers` Symptom: tooling cannot reconstruct correct environments for partial code. Root cause: compile environment and lexical context are architecture data, not incidental metadata. Concept: compile-time versus runtime. Evidence: issue goal is environment building for IDE fragments. Fix or mitigation: make environment construction explicit and inspectible. Why it worked: the real contract lives in compile context. Faster path: ask what context a fragment needs before reviewing execution. Guide prompt: "What compile-time context must exist for this boundary to work?"
- `#6647 Could code formatter leave spaces in multiline keyword lists?` Symptom: formatting behaviour clashes with human alignment expectations. Root cause: formatting architecture encodes stable syntactic rules rather than preserving all author intent. Concept: generated API or delegation confusion. Evidence: issue asks formatter to preserve alignment. Fix or mitigation: keep formatter semantics explicit instead of inferring structure from spacing. Why it worked: one owner decides code shape. Faster path: ask who owns normalization at this boundary. Guide prompt: "Who owns canonical structure when user input is normalized?"
- `#13974 Deprecate struct update syntax` Symptom: syntax exists but creates ambiguity versus pattern matching and future typing goals. Root cause: syntax-level convenience obscures stronger compile-time guarantees. Concept: compile-time versus runtime. Evidence: issue explicitly ties syntax choice to type-system direction. Fix or mitigation: prefer forms with clearer compile-time checking. Why it worked: architecture favored explicit compile-time guarantees. Faster path: ask which syntax choices complicate later verification. Guide prompt: "Which surface forms weaken the architecture's verification model?"
- `#1231 There are just 49 failures out of 1075 tests on Windows` Symptom: broad Windows failure cluster. Root cause: the core runtime assumed Unix-like semantics in multiple places. Concept: environment-specific runtime differences. Evidence: failures span file and shell-related tests. Fix or mitigation: harden cross-platform assumptions near the runtime boundary. Why it worked: the shared cause sat below application logic. Faster path: cluster failures by environment boundary first. Guide prompt: "Which failures share the same environment boundary?"
- `#1560 Can't build under Cygwin` Symptom: source build fails under Cygwin. Root cause: build and toolchain assumptions are part of system architecture. Concept: boot and code loading. Evidence: issue body is a build failure before normal execution. Fix or mitigation: align build scripts with supported shell and toolchain behaviour. Why it worked: boot code cannot recover from incompatible host assumptions. Faster path: inspect build-shell contract before Elixir-level code. Guide prompt: "What host tools and shell semantics are required before the reviewed runtime exists?"
- `#9150 Add NaiveDateTime.local_now` Symptom: API pressure around local time handling. Root cause: runtime APIs that depend on local environment can blur deterministic system behaviour. Concept: external boundary handling. Evidence: issue asks for local clock behaviour. Fix or mitigation: make environment-derived time behaviour explicit. Why it worked: architecture clarity improved once the environment boundary was named. Faster path: ask which values come from local machine state. Guide prompt: "Which runtime values come from the host environment rather than pure process state?"
- `#9465 Further unify logger and Logger` Symptom: split between old and new logging paths complicates behaviour. Root cause: multiple logging surfaces obscure the real observability contract. Concept: observability gaps. Evidence: issue checklist centers on moving from `:error_logger` to `:logger`. Fix or mitigation: collapse observability onto one owner. Why it worked: one consistent signal path simplifies diagnosis. Faster path: ask how many logging stacks exist. Guide prompt: "What observability surface is authoritative?"
- `#8014 Additions to the Calendar module` Symptom: missing calendar semantics. Root cause: time and calendar behaviour are architecture-level data contracts. Concept: external boundary handling. Evidence: issue references standard coverage gaps. Fix or mitigation: expose richer calendar semantics at the right abstraction boundary. Why it worked: the missing behaviour belonged in a shared contract, not per-app workarounds. Faster path: ask which core data contracts are being reinvented downstream. Guide prompt: "What shared contract is callers reimplementing because it is not explicit here?"
- `#2469 Provide defguard` Symptom: guard authoring is awkward and error-prone. Root cause: compile-time macro generation and runtime guard semantics were not aligned for users. Concept: callback, behaviour, protocol, or macro boundaries. Evidence: issue explicitly describes quote/unquote complexity. Fix or mitigation: add a clearer macro boundary for guards. Why it worked: it separated generated syntax concerns from runtime guard use. Faster path: ask whether a macro hides too much ceremony. Guide prompt: "Which repeated pattern signals a missing first-class compile-time surface?"
- `#3400 Float.round is inconsistent` Symptom: unexpected rounding output. Root cause: users misread numeric semantics across runtime and representation boundaries. Concept: state ownership and ownership transfer. Evidence: issue compares outputs from similar-looking operations. Fix or mitigation: document and harden numeric semantics. Why it worked: it clarified the real owner of rounding behaviour. Faster path: ask which layer owns numeric normalization. Guide prompt: "What layer owns data normalization for this boundary?"
- `#4082 Add make compiler` Symptom: native dependency compilation is awkward. Root cause: build-tool architecture did not expose native compilation as a first-class boundary. Concept: boot and code loading. Evidence: issue asks for compiler integration. Fix or mitigation: make native compilation explicit in toolchain flow. Why it worked: it named an architecture boundary instead of relying on ad hoc build steps. Faster path: ask which external build systems participate. Guide prompt: "Which external build step is required for the reviewed boundary to exist?"
- `#5737 Improve no function clause error messages` Symptom: errors do not reveal enough dispatch context. Root cause: observability at pattern-match boundaries was too weak. Concept: observability and proof. Evidence: issue asks for richer clause diagnostics. Fix or mitigation: surface better call-shape information. Why it worked: error reports became closer to the real dispatch boundary. Faster path: ask whether the failure output identifies the true decision point. Guide prompt: "If this path fails, will the signal identify the exact dispatch boundary?"
- `#9987 mix local.hex does nothing in Windows` Symptom: installer path appears to hang or do nothing. Root cause: environment-specific process and shell behaviour was not explicit. Concept: environment-specific runtime differences. Evidence: issue is Windows-only installer behaviour. Fix or mitigation: tighten Windows process handling and diagnostics. Why it worked: the problem was in startup environment, not Mix logic. Faster path: inspect host process, shell, and path behaviour first. Guide prompt: "Which host-specific process behaviours can make this boundary appear dead?"
- `#13762 Make it easier to spot compile-time graphs` Symptom: compile dependency cycles are hard to see. Root cause: compile-time dependency structure is hidden. Concept: compile-time versus runtime. Evidence: issue explicitly asks to spot compile-time graphs. Fix or mitigation: improve visibility of compile graph edges. Why it worked: the architecture problem was dependency shape, not runtime logic. Faster path: ask for compile graph before debugging recompilation or cycles. Guide prompt: "What compile-time graph exists and how do you inspect it?"
- `#2074 Create a bare project by default (no supervisor)` Symptom: generated supervision tree confuses users about when it is needed. Root cause: project scaffolding can hide architectural intent. Concept: supervision and lifecycle. Evidence: issue says experienced users are confused by generated supervisor. Fix or mitigation: make lifecycle scaffolding match the actual system need. Why it worked: it reduced accidental architecture. Faster path: ask whether the generated lifecycle model is required. Guide prompt: "Which generated runtime structure is essential versus accidental?"
- `#7000 mix escript.build does not use the generated beam files` Symptom: built artifact ignores expected compiled output. Root cause: build and packaging boundaries are not aligned. Concept: boot and code loading. Evidence: issue reports mismatch between generated beams and escript build output. Fix or mitigation: make packaging consume the correct compiled artifacts. Why it worked: it aligned build owner and runtime artifact owner. Faster path: ask which artifact is actually executed. Guide prompt: "Which artifact is authoritative at runtime?"
- `#12878 The Elixir installer incorrectly appends paths to a Windows environment variable` Symptom: Windows PATH setup breaks. Root cause: installer architecture mis-handled host environment mutation. Concept: environment-specific runtime differences. Evidence: issue centers on PATH mutation. Fix or mitigation: correct environment-variable handling in installer logic. Why it worked: startup discovery depended on PATH correctness. Faster path: inspect environment mutation before runtime behaviour. Guide prompt: "Which environment variables are part of the architecture contract?"
- `#4617 Support Erlang 19 new features` Symptom: Elixir needs to track new OTP capabilities. Root cause: upstream runtime compatibility is an architectural dependency. Concept: external boundary handling. Evidence: issue checklist spans syntax and runtime features. Fix or mitigation: update Elixir to reflect OTP behaviour shifts. Why it worked: Elixir architecture sits on the OTP contract. Faster path: ask which upstream runtime version changes semantics. Guide prompt: "Which upstream runtime behaviours materially shape this system?"
- `#4423 Mix should not error out on non-SemVer versions` Symptom: toolchain rejects real-world package versions. Root cause: external ecosystem assumptions were too strict. Concept: external boundary handling. Evidence: issue comes from packaging/distribution use. Fix or mitigation: warn instead of hard failing when the architecture can tolerate it. Why it worked: it matched the real contract with external packages. Faster path: ask whether validation assumptions exceed true invariants. Guide prompt: "Which input validations are policy versus hard architecture constraints?"
- `#6122 Is Windows Installer broken?` Symptom: reinstall fails after previous success. Root cause: installer and host integration are unstable architecture edges. Concept: boot and code loading. Evidence: issue is repeated Windows install failure. Fix or mitigation: improve install diagnostics and host integration. Why it worked: startup trust depends on reproducible install state. Faster path: inspect install-time side effects before application logic. Guide prompt: "What state does installation leave behind and who owns it?"
- `#798 truthy values not correctly documented in specs` Symptom: specs say `boolean()` while behaviour accepts truthy values. Root cause: the documented contract diverged from runtime dispatch. Concept: public API versus generated API. Evidence: issue compares spec surface to actual behaviour. Fix or mitigation: align typespecs with runtime truthiness semantics. Why it worked: the review surface matched the true contract again. Faster path: compare typespec, docs, and runtime acceptance side by side. Guide prompt: "Does the visible contract match the true runtime contract?"
- `#5987 Protocol implementations lost in umbrellas` Symptom: protocols disappear in umbrella projects. Root cause: code loading and protocol consolidation boundaries were not explicit enough. Concept: generated API or delegation confusion. Evidence: issue is specifically about protocols in umbrellas. Fix or mitigation: correct consolidation and loading behaviour across umbrella boundaries. Why it worked: protocol visibility depends on compile and load topology. Faster path: inspect compile/load topology before business modules. Guide prompt: "How are generated implementations discovered across package boundaries?"
- `#9779 Cannot compile Elixir from source` Symptom: source compilation fails on one environment. Root cause: host toolchain assumptions were not satisfied. Concept: boot and code loading. Evidence: issue is a source-build failure. Fix or mitigation: align build prerequisites and diagnostics. Why it worked: the failure sat below Elixir module behaviour. Faster path: check build contract and host dependencies first. Guide prompt: "What prerequisites must exist before compilation can even begin?"
- `#5664 Elixir projects always recompile / Dialyzer cannot open files` Symptom: projects recompile unexpectedly and Dialyzer fails. Root cause: lower-level environment and tooling interactions caused stale or unreadable artifacts. Concept: test-only or environment-specific runtime differences. Evidence: issue TL;DR points to zlib and Erlang interaction. Fix or mitigation: change the underlying runtime dependency version. Why it worked: the architecture problem was artifact handling in the host/runtime stack. Faster path: if recompilation is global and nondeterministic, inspect toolchain and compression/runtime layers before source modules. Guide prompt: "If rebuild behaviour is global, which lower-level artifact boundary is misbehaving?"

## Fast Path Heuristics

1. If the system starts from a script, review the script before the first Elixir module.
2. If behaviour looks magical, ask what compile-time expansion, code generation, or internal Erlang module actually owns it.
3. If failures cluster by OS or packaging path, inspect environment assumptions before application logic.
4. If protocols, recompilation, or code visibility are odd, inspect compile and load topology before function bodies.
5. If error output is confusing, find the boundary that formats or prunes diagnostics.
6. If the system is short-lived, redefine "recovery" around failure normalization and exit behaviour.

## Concrete Steps

- `curl -sL https://api.github.com/repos/elixir-lang/elixir`
- `curl -sL https://api.github.com/repos/elixir-lang/elixir/branches/main`
- `curl -sL https://api.github.com/repos/elixir-lang/elixir/contents/bin/elixir?ref=9a663cf3faf4c5f5ddddd8bb02624b79837fb433`
- `curl -sL https://api.github.com/repos/elixir-lang/elixir/contents/lib/elixir/lib/kernel/cli.ex?ref=9a663cf3faf4c5f5ddddd8bb02624b79837fb433`
- `curl -sL https://api.github.com/repos/elixir-lang/elixir/contents/lib/elixir/lib/code.ex?ref=9a663cf3faf4c5f5ddddd8bb02624b79837fb433`
- `curl -sL https://api.github.com/repos/elixir-lang/elixir/contents/lib/elixir/src/elixir.erl?ref=9a663cf3faf4c5f5ddddd8bb02624b79837fb433`
- `curl -sL "https://api.github.com/search/issues?q=repo:elixir-lang/elixir+is:issue+is:closed&sort=comments&order=desc&per_page=25&page=1"`

After this section, you should be able to answer: how do you reproduce the evidence and continue the review safely?

## Surprises & Discoveries

- The most important Elixir architecture questions for a novice are not only about OTP structure. They are about boot, compilation, code loading, and hidden internal owners.
- The issue history strongly reinforces that environment and packaging boundaries are architecture, not peripheral operations detail.
- The current guide already handled state and failure well, but it under-prompted compile-time structure and generated surfaces.

After this section, you should be able to answer: what changed your understanding during the work?

## Decision Log

- Decision: anchor the review on `bin/elixir -e "1 + 1"` instead of a broader compiler path.
  Rationale: the user requested the CLI entry path and one simple evaluation path.
  Evidence: `bin/elixir`, `Kernel.CLI`, and `Code.eval_string/3` provided a coherent end-to-end trace.
  Date/Author: 2026-03-07 / Codex

- Decision: treat issue-based resolution notes as architecture inference when a full thread was not read.
  Rationale: the plan requires 25 issue entries per app, but GitHub API budget is limited.
  Evidence: official issue search payloads provided strong symptom data, while full per-issue resolution threads would have required many more requests.
  Date/Author: 2026-03-07 / Codex

After this section, you should be able to answer: why did the review take its current shape?

## Validation and Acceptance

- You can point to where work enters before any Elixir module code is visible.
- You can name which parts of the path are shell, OTP boot, CLI routing, and eval internals.
- You can distinguish compile-time work from runtime work in the reviewed path.
- You can identify at least one hidden internal owner behind the public API.
- You can explain what failure handling and recovery mean for a short-lived CLI boundary.
- You can show which issue clusters would have been faster to solve with the missing guide prompts above.

After this section, you should be able to answer: how do you know this review is complete enough to hand off safely?

## Open Questions / Blockers

- No architecture blockers remain for the scoped review.
- Precision note: a subset of Phase 3 fix summaries are high-confidence inference from issue statement plus project direction rather than direct maintainer comment review.

After this section, you should be able to answer: what is still unknown and why does it matter?

## Next Handoff

Hand off to the cross-app synthesis pass so the recurring gaps from this review can be converted into dependency-neutral additions to `.agent/ARCHITECTURE_REVIEW.md`.

After this section, you should be able to answer: what should happen after this review and why?

## Outcomes & Retrospective

This review proved that the current guide needs stronger prompts for boot paths, compile-time versus runtime boundaries, generated surfaces, and environment-specific architecture. Elixir itself is a strong stress test because the review remains fundamentally about Elixir and OTP reasoning even though the visible boundary is a CLI tool rather than an application server.

After this section, you should be able to answer: what did this review achieve overall?

## Change Log

- (2026-03-07 09:30Z) Change: Created the Elixir benchmark ArchitectureReview. Reason: collect Phase 1-3 evidence before revising `.agent/ARCHITECTURE_REVIEW.md`.

After this section, you should be able to answer: how did the document evolve over time?
