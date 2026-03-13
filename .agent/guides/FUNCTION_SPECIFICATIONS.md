# Function Specifications

This document describes how to write complete public function specifications in Elixir. Treat the reader as a complete beginner to this repository: they have only the current working tree and the single Function Specifications document you provide. There is no memory of prior specifications and no external context.

A function specification is the full public contract for a function. Use it to state what the function accepts, what it returns, what must already be true before a guaranteed result applies, and which failures are part of the public behaviour. Write the specification so a beginner can use the function correctly without reading the implementation.

Write with clarity first, then stop. Include every fact the caller needs to use the function correctly. Do not add explanation that does not change how the caller should use the function.

## How to use function specifications and FUNCTION_SPECIFICATIONS.md

When defining, revising, reviewing, or relying on a public function's contract, follow FUNCTION_SPECIFICATIONS.md to the _letter_. If it is not in your context, refresh your memory by reading the entire file. Apply it to determine what the function accepts, what it returns, which preconditions or failure behaviours are part of the contract, which options and defaults must be documented, which examples are required, and which caller-visible rules must be stated before implementation or testing can be treated as correct.

Write the function specification as soon as the intended behaviour can be stated clearly. Do not wait for the implementation to imply the contract. Complete the specification before different readers could reasonably disagree about what the function should do.

Use the specification to settle design questions. State accepted inputs, promised outputs, options, defaults, failure behaviour, and important edge cases. Do not leave those facts to future code, tests, or comments. Resolve the behaviour first. Then implement it.

Use the specification to protect the public boundary during implementation. Update it before relying on any new return shape, error case, option, or caller-visible rule. If it is unclear whether a detail is public or private, treat that as an unresolved specification problem and resolve it in the specification first.

Use the specification to drive tests and review. Write tests from the contract, not from internal structure. Read the specification before reading the implementation during review. If the implementation and the specification disagree, treat that as a defect until one of them is corrected.

Keep function specifications current. When you notice a change in behaviour or contract, update the specification immediately. The specification is the source of truth for the public interface.

## Requirements

A public function specification is acceptable only when all of the following are true.

- The reader can understand the required inputs, the promised outputs, the accepted options, the defaults, and the defined failure behaviour without reading the implementation.

- The documentation states what the function does. It does not merely restate the function name.

- The documentation excludes internal mechanics unless callers are explicitly allowed to rely on them.

- The return shapes and the documentation expose every important failure case.

- The function includes examples. Include at least one success example. Include edge or failure examples when those cases are part of the contract.

- The implementation can change without breaking anything the specification promises.

## Guidelines

Require every public function to be understandable from its `@doc`, `@spec`, and examples alone. A complete beginner must be able to use the function correctly without opening the function body.

Treat the specification as correct only when it states what callers may rely on, the typespec exposes the visible argument and return shapes, the examples demonstrate real usage, and the implementation remains free to change without breaking those promises.

## Begin with the promise, not the mechanism

Decide what the function guarantees before you write the `@spec`. State what result the caller is entitled to rely on after a valid call. State what conditions must already hold before that guarantee applies. Those facts are the core of the specification.

Write from the caller's point of view. State what inputs the function accepts, what outputs it returns, and what happens in important edge cases. Keep internal details out of the contract unless callers are meant to depend on them. Traversal order, helper structure, intermediate data reshaping, and internal state organization are private unless the documentation explicitly makes them public.

Do not document the current implementation path. Document the behaviour that must remain true even if the implementation is refactored or replaced.

## Treat `@spec` as the type line of the contract

Use `@spec` to declare the public call shapes of the function. Show the argument shapes the caller may pass and the return shapes the caller may receive.

Write Elixir typespecs in forms that people and tools already understand: `String.t()`, `map()`, `keyword()`, structs, named types, and tagged tuples such as `{:ok, value}` and `{:error, reason}`. Use more specific built-in types when they match the contract. For example, use `non_neg_integer()` when the return value cannot be negative.

When the same concept appears more than once, define it with `@type` and explain it with `@typedoc`. Use named types to keep the public API consistent and readable.

Do not use `@spec` as a substitute for the full contract. In Elixir, a typespec describes shape and supports tooling. It does not fully describe behaviour. It does not tell the reader what an option means, which error reason applies to which case, whether input is normalized, or which matching item is returned. Put those rules in `@doc`.

Apply one rule consistently: if a caller can satisfy the `@spec` and still misuse the function, the missing rule belongs in the documentation.

## Use `@doc` to define the behaviour

Use `@doc` to state what the function means. Start with one sentence that says what the function does. Then state the rules the caller must follow and the guarantees the function makes.

State accepted inputs when the typespec alone is not enough. State what the return values mean. If the function raises, name the exception. If the function returns an error tuple, name the error reasons and explain when each one is returned.

If the function accepts keyword options, document each option by name. State the default value, the accepted values, whether the option may appear more than once, how unknown options are handled, and the effect on behaviour. If an option is invalid, state exactly what the function does.

State important edge cases explicitly. If empty input, missing values, duplicates, `nil`, or out-of-range values change the result, document that behaviour.

Do not write vague summaries such as "Parses a value" or "Normalizes options." State what value is parsed, what counts as valid input, what normalization occurs if it matters to the caller, and what result is returned.

## Make a clear decision about invalid input

Choose one model for each invalid-input case and document it.

The first model is a precondition. Use it when the function only promises behaviour if a condition is already true before the call. If you choose this model, state the precondition plainly.

The second model is defined failure behaviour. Use it when the function is expected to handle invalid input by returning an error or raising an exception. If you choose this model, show the failure in the return shapes or raised exception and explain it in `@doc`.

Do not mix those models for the same invalid-input case. If the function returns `{:error, :invalid_*}` for a case, that case is part of the function's defined behaviour. It is not only a precondition.

If a caller violates a documented precondition and the specification does not also define failure behaviour for that case, the call is outside the contract. Do not imply a guaranteed result for that case.

If you document an error return or raised exception as part of the contract, keep that behaviour stable. Callers are allowed to rely on documented failure behaviour in the same way they rely on documented success behaviour.

Do not hide important failure inside a value that looks like normal success data.

## Use examples to remove doubt

Examples are required. Add them after the prose and the typespec are written.

Use examples to show the default call, option-driven behaviour when options exist, and the most important edge or failure cases. Each example must match the documented contract exactly.

Use doctests when the example is stable and side-effect free. When an example cannot be used as a doctest because it depends on time, I/O, external state, or side effects, include the example anyway and make the expected result explicit.

If the contract is still unclear after you add examples, the specification is not finished.

## Write strong specifications

Weak specifications allow bad implementations. Avoid vague guarantees.

Do not write "returns an index of `x`." State whether the function returns the first match, the last match, or any match. Do not write "handles options." State which options exist, what each option does, and what the defaults are.

Write only the guarantees the caller needs, but write all of them. Strengthen the specification until it rules out useless or surprising implementations. Stop before you promise private implementation choices that callers do not need.

## Separate public contracts from private notes

Use `@doc` for behaviour that callers may rely on. Use code comments for implementation notes.

Put workarounds, tradeoffs, algorithm notes, and other private details in comments near the code that needs them. Put caller-visible rules in the function specification. Keep that boundary clear so the public contract stays stable while the private implementation evolves.

## A practical pattern to follow

When you write a public function specification, use this order.

First, write one sentence that states what the function does for the caller.

Second, write the `@spec` that captures the argument and return shapes.

Third, write the `@doc` that defines behaviour, caller obligations, options, defaults, failure behaviour, and important edge cases.

Fourth, add examples. Include at least one success example. Include edge or failure examples when those cases are part of the contract.

Fifth, read the `@doc`, `@spec`, and examples without reading the implementation. If a beginner still has to inspect the function body to use the function correctly, the specification is incomplete.

## Good Example of a Function Specification

    defmodule MyApp.Lists do
      @typedoc """
      Options accepted by `index_of/3`.
      """
      @type index_of_option :: {:from, integer()}

      @doc """
      Return the index of the first occurrence of `value` in `items`.

      The first item in the list has index `0`.

      Returns `{:ok, index}` when `value` exists in `items` at or after `:from`.
      Returns `{:error, :not_found}` when `value` does not exist in `items` at or after `:from`.
      Returns `{:error, :invalid_option}` when `:from` is negative.

      ## Options

      - `:from` - index where the search starts. Defaults to `0`.
        Accepted values are integers greater than or equal to `0`.
        Returns `{:error, :invalid_option}` when `:from` is negative.

      ## Examples

          iex> MyApp.Lists.index_of([:a, :b, :c], :b)
          {:ok, 1}

          iex> MyApp.Lists.index_of([:a, :b, :a], :a, from: 1)
          {:ok, 2}

          iex> MyApp.Lists.index_of([:a, :b, :c], :x)
          {:error, :not_found}

          iex> MyApp.Lists.index_of([:a, :b, :c], :a, from: -1)
          {:error, :invalid_option}
      """
      @spec index_of([term()], term(), [index_of_option()]) ::
              {:ok, non_neg_integer()} | {:error, :not_found | :invalid_option}
      def index_of(items, value, opts \\ []) do
        from = Keyword.get(opts, :from, 0)

        if from < 0 do
          {:error, :invalid_option}
        else
          items
          |> Enum.with_index()
          |> Enum.drop(from)
          |> Enum.find_value({:error, :not_found}, fn {item, index} ->
            if item == value, do: {:ok, index}
          end)
        end
      end
    end

This example shows the standard this document requires. The `@spec` shows the public call shapes. The `@typedoc` and `@type` name the option shape. The `@doc` states the behaviour, the option default, the accepted option values, and the meaning of each return value. The examples prove the default case, the option-driven case, the not-found case, and the invalid-option case. Model new function specifications on this same structure: public shapes in `@spec`, behaviour in `@doc`, and concrete proof in examples.
