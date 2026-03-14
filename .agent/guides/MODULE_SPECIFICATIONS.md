# Module Specifications

This document describes how to write complete public module specifications in Elixir. Treat the reader as a complete beginner to this repository: they have only the current working tree and the single Module Specifications document you provide. There is no memory of prior specifications and no external context.

A module specification is the full public contract for a module. Use it to state what the module is for, which public entry points a caller should use, which public types and values the module defines, which rules apply across the module as a whole, and which guarantees callers may rely on without reading the implementation.

Write the specification so a beginner can choose the right module, call it correctly, and understand the public boundary without opening private helpers or tracing internal data flow.

## How to use module specifications and MODULE_SPECIFICATIONS.md

When defining, revising, reviewing, or relying on a public module's contract, follow MODULE_SPECIFICATIONS.md to the _letter_. If it is not in your context, refresh your memory by reading the entire file. Apply it to determine what the module is for, which public entry points a caller should start with, which module-level rules callers may rely on, which public types and shared options must be documented, and which details must stay out of the public contract because they are only implementation details.

Write the module specification as soon as the module's job and public boundary can be stated clearly. Do not wait for the implementation structure to imply the contract. Complete the specification before the module grows enough that different readers could reasonably disagree about what belongs in it.

Use the module specification to settle module-level design questions. State which responsibility the module owns, which responsibility it does not own, which public functions a caller should start with, which public types matter, which options or defaults are shared across functions, and which rules remain true across every public entry point. Do not leave those facts to implementation reading.

Use the module specification to protect the public boundary during implementation. Update it before other code depends on any new public entry point, public type, shared option, ordering guarantee, error model, or struct field. If it is unclear whether a rule belongs to one function or to the module as a whole, resolve that question in the module specification before proceeding.

Use the module specification to drive review. Read the specification before reading the implementation. If the implementation exposes caller-visible behaviour, concepts, or obligations that are missing from @moduledoc, treat that as a defect until the public boundary is documented or the implementation is changed.

Always keep module specifications current. They remain part of the public interface for as long as the module exists.

## Use Module Specifications In ExecPlans

Before describing implementation steps, the `ExecPlan` should make clear which one module owns the behavior and which nearby modules participate without owning it. The purpose of that section is not to inventory files. It is to let the reader see where the behavior lives, what that module is responsible for, what it is not responsible for, which public entry points matter, which shared rules and failure behavior must continue to hold, and how the surrounding modules hand work to one another.

Naming files to change is not a substitute for that explanation. If the implementer could still ask which module actually owns the behavior, the plan has not established the module boundary clearly enough.

## Requirements

A public module specification is acceptable only when all of the following are true.

* The reader can tell what the module is for, when to use it, and when to choose a different module without reading the implementation.

* The specification identifies the public entry points and tells the reader where to start.

* The module-level documentation states the cross-function rules that callers may rely on.

* The public boundary documents public types, struct fields, shared options, defaults, and caller-visible failure behaviour.

* The documentation describes the module abstractly from the caller's point of view. It does not depend on private helper names, internal traversal steps, or current storage layout unless those details are explicitly public.

* The module includes examples that show real usage of the public entry points together.

* The implementation can be refactored or replaced without breaking anything the module specification promises.

## Guidelines

Require every public module to be understandable from its `@moduledoc`, public types, public function documentation, and examples alone. A complete beginner must be able to decide whether to use the module and how to begin using it without opening private functions.

Treat the module specification as correct only when it explains the module's role in the system, defines the public data model, records the rules shared by the public entry points, and leaves the implementation free to change behind that boundary.

## Begin with the module responsibility and boundary

Define the module's responsibility before documenting its functions one by one. State the job of the module in one sentence. Then define the boundary around that job. State when callers should use this module, when they should not, and which nearby modules handle adjacent responsibilities.

Write from the caller's point of view. State what the module offers, how its public functions relate to one another, and which facts remain true across the entire module. Keep internal pipeline structure, helper names, and temporary data reshaping out of the contract unless callers are expected to rely on them.

Do not document the current implementation path. Document the stable public meaning of the module.

## Put module-level rules in `@moduledoc`

Use `@moduledoc` for module-level behaviour that does not belong to a single function.

Start with one sentence that states what the module does. Then state the module's role, the main public entry points, the public data model, and the shared rules a caller must know before choosing a function.

Document cross-function rules once at the module level instead of repeating them inconsistently across function documentation. Put shared options, normalization rules, ordering guarantees, ownership rules, concurrency expectations, struct invariants, lifecycle constraints, and composition rules in `@moduledoc` when they apply across multiple public entry points.

When the module participates in a larger workflow, name the nearby modules and state the handoff clearly. State where the data comes from, what this module is responsible for changing, and where callers go next.

If the module defines a public struct, document which fields are public and what each public field means. If some fields are private implementation state, say so directly and tell callers not to rely on them.

## Use public types to name the module's model

Use `@type` and `@typedoc` to name the important public concepts the module exposes. Name return values, option shapes, identifier types, struct shapes, and other concepts that appear across multiple public functions.

Use named types to keep the module specification readable. If the same shape appears in more than one `@spec`, give it one public type name and explain it once.

When the module is meant to hide a representation, document the value in terms of what it means to callers, not how it is stored internally. State what the value represents and how callers may use it. Do not describe the internal container or intermediate format unless that detail is part of the public contract.

## Keep function contracts inside the module boundary

Require every public function to keep its own function specifications using `@doc`, `@spec`, and examples. The module specification does not replace function specifications.

Use the module specification for shared context and module-level rules. Use function specifications for per-function contracts. Keep that division clear.

State a rule in `@moduledoc` when it applies across every public function in the module. State a rule in a function's `@doc` when it applies only to that function.

Do not copy full function contracts into the module specification. Use the module specification to orient the caller and define shared behaviour. Do not restate every argument list and per-function rule there.

## Write abstract specifications, not representation tours

Describe the module in terms of observable behaviour and stable concepts. Do not explain it by walking through helper functions, private callbacks, internal state transitions, or the exact data structure chosen by the current implementation.

Do this especially when the module provides an abstraction. A caller must be able to use the module correctly without knowing whether the implementation uses a map, a keyword list, an ETS table, a process dictionary entry, or helper modules.

Remove implementation details from the specification unless callers are allowed to rely on them. If a detail is not part of the public contract, do not leave it in the documentation.

## Use examples to show the whole module in motion

Require module examples. Use them to show how a caller starts with the module and gets a result.

Show the first successful path through the module. If the module defines a workflow, show the steps in order. If important edge cases or failure modes are part of normal use, include examples for those cases too.

Use examples to connect the module-level explanation to the underlying public functions. A module example should show which function to call first, which values move between calls, and what the caller should expect to observe.

If the module still feels abstract after the examples are added, the module specification is incomplete.

## Separate the interface from the implementation

Keep the full public specification in the public interface: `@moduledoc`, public types, public function documentation, public specs, and examples.

Keep private notes in comments near the code that needs them. Put algorithm choices, performance tradeoffs, helper relationships, temporary workarounds, and internal data-shape explanations in comments when those details are only for maintainers.

Do not require callers to read the implementation to discover the contract. Do not copy the public contract into multiple internal places where it can drift.

## A practical pattern to follow

Write a public module specification in a fixed order.

Write one sentence that states what the module does for the caller.

Write a short orientation that explains when to use the module, when not to use it, and which public entry points a new caller should start with.

Name the public types and data shapes with `@typedoc` and `@type`.

Write the `@moduledoc` sections that define the module-level rules, shared options, invariants, and workflow.

Write the per-function `@doc`, `@spec`, and examples for each public entry point.

Add at least one module-level example that shows the module in realistic use from start to finish.

Read the `@moduledoc`, public types, public function documentation, and examples without reading the implementation. If a beginner would still need to inspect private functions to decide whether the module fits the problem or how to begin using it, treat the specification as incomplete.

## Compact ExecPlan Module Spec Template

In plans for features, bug fixes, and behavior-preserving refactors that touch a public boundary, the module-level section should usually say enough for the reader to understand the module's responsibility, its boundary, the public entry points that matter, the shared rules or invariants that give the module its shape, the important handoffs to adjacent modules, and the public concepts that matter to the work. A brief module-level example often helps because it shows how the module is meant to be used rather than merely naming its parts.

## Incomplete Module Specifications In Plans

Module-level plan writing becomes weak when it begins with helper flow, file lists, or implementation internals instead of the responsibility the caller relies on. It also becomes weak when it never explains why this module is the right home for the behavior, when it leaves the nearby module boundaries implicit, or when it omits the shared invariants and failure behavior that the implementation is supposed to preserve. A junior implementer should be able to orient themselves from the plan alone.

## Good Example of a Module Specification

    defmodule MyApp.PageWindow do
      @moduledoc """
      Build normalized page windows for offset-based pagination.

      Use this module when a caller has page-oriented input such as `page`
      and `page_size` and needs a validated pagination value that can be
      passed to query-building code. Do not use this module to run database
      queries or to count records. `MyApp.PageWindow` only validates and
      normalizes pagination input.

      Start with `new/1` when the caller has raw input. Use `offset/1` and
      `limit/1` after a page window has been validated. All public functions
      in this module treat page numbers as one-based.

      ## Public model

      A page window represents a validated page request with three public
      values:

      * `page` - the requested page number
      * `page_size` - the number of entries per page
      * `offset` - the zero-based number of entries skipped before the page

      The module guarantees that every successful page window has a page
      greater than or equal to `1` and a page size greater than or equal to
      `1`.

      ## Shared failure behaviour

      `new/1` returns `{:error, :invalid_page}` when `:page` is missing,
      not an integer, or less than `1`.

      `new/1` returns `{:error, :invalid_page_size}` when `:page_size` is
      missing, not an integer, or less than `1`.

      ## Examples

          iex> {:ok, window} = MyApp.PageWindow.new(page: 2, page_size: 25)
          iex> MyApp.PageWindow.offset(window)
          25

          iex> MyApp.PageWindow.new(page: 0, page_size: 25)
          {:error, :invalid_page}
      """
      @typedoc """
      A validated page window.
      """
      @type t :: %__MODULE__{
              page: pos_integer(),
              page_size: pos_integer()
            }

      @typedoc """
      Options accepted by `new/1`.
      """
      @type option :: {:page, integer()} | {:page_size, integer()}

      defstruct [:page, :page_size]

      @doc """
      Validate and normalize pagination input into a page window.

      Returns `{:ok, window}` when both `:page` and `:page_size` are integers
      greater than or equal to `1`.

      Returns `{:error, :invalid_page}` when `:page` is missing, not an
      integer, or less than `1`.

      Returns `{:error, :invalid_page_size}` when `:page_size` is missing,
      not an integer, or less than `1`.
      """
      @spec new([option()]) :: {:ok, t()} | {:error, :invalid_page | :invalid_page_size}
      def new(opts) do
        page = Keyword.get(opts, :page)
        page_size = Keyword.get(opts, :page_size)

        cond do
          not is_integer(page) or page < 1 ->
            {:error, :invalid_page}

          not is_integer(page_size) or page_size < 1 ->
            {:error, :invalid_page_size}

          true ->
            {:ok, %__MODULE__{page: page, page_size: page_size}}
        end
      end

      @doc """
      Return the zero-based offset represented by a validated page window.
      """
      @spec offset(t()) :: non_neg_integer()
      def offset(%__MODULE__{page: page, page_size: page_size}) do
        (page - 1) * page_size
      end

      @doc """
      Return the page size represented by a validated page window.
      """
      @spec limit(t()) :: pos_integer()
      def limit(%__MODULE__{page_size: page_size}), do: page_size
    end

This example shows the standard this document requires. The `@moduledoc` tells the caller what the module is for, what it is not for, where to start, the public model, and the shared failure rules. The public type names the validated page window. The public functions each keep their own focused contracts. The examples show both the happy path and an invalid-input path. Model new module specifications on this same structure: module role and shared rules in `@moduledoc`, reusable concepts in public types, per-function contracts in `@doc` and `@spec`, and concrete proof in examples.
