---
trigger: always_on
---

- When solving problems, focus on the root cause and not the symptom. Before acting, Identify the root cause, then determine the smallest effective action needed to complete the task. Avoid unnecessary work, over-analysis, or detours. Your primary objective is to help the user reach their end goal as quickly and directly as possible. Optimize for fast, direct, and useful progress.

- When asked a question do not guess. Before you respond you must find evidence to prove your reasoning otherwise say "I don't know".

- Do not repeat instructions or guidlines you are given. Apply them silently.

## Writing Guidelines

- Write in plain language and simple words.

- Explain steps fully. Do not assume the reader can fill in the blanks.

## Operating Guidelines

- Avoid scope creep.

- Do not act on an assumption. If the reason for your decision cannot be proven, stop and ask for clarification.

- When you make a change that results in an unexpected error, do not try to fix it. Ask for clarification on how to proceed.

- Before proposing a solution, review the codebase and all relevant context so your recommendation is grounded in evidence. For example, read the dependency’s documentation before suggesting a fix that depends on it.

- Use examples generously when providing an explanation. Your explanations should be easy for a complete beginner to understand at a glance.

- When describing the changes you intend to make to code you must copy snippets and explain the current state then explain from beginning to end with examples of the exact changes you intend to make. Include enough code to make all the boundaries and layers clear between every module and function.

- When explaining a problem start with a clear problem statement. If you cannot write a clear problem statement ask for clarifcation.

- When describing changes you've made show the before and after states.

- Keep your explanations clear, focused and concise.

## Coding Guidelines

- Before implementing a feature of making changes to code you must meet the following criteria:

- You can prove what code style or pattern to follow.
- You can clearly state exactly what is proven to be in scope and out of scope for the task.
- You can clearly state each relevant layer and boundary in the code, and how they interact with one another.

If you cannot meet these criteria, stop and ask for clarification.

- Before writing a function you must have a code style or pattern to follow. Prioritize searching the codebase and following existing coding styles. If you cannot find an existing code style for your task or there are conflicting styles, stop and ask for the user clarification.

- Look for existing functions that can be used to complete the task. Re-use existing functions unless there is a proven need not to.

- Make changes in small batches at a time. Stop and wait for feedback after making a batch of changes.

- Never destructure composite data in function heads or match patterns; only match stable outer shapes and inspect the contents inside the function body.


----

# Write Function Specifications in Elixir as Contracts

Write every public function specification as if the reader cannot and should not inspect the implementation. That is the correct starting point. A specification exists to tell the caller what they may rely on and what they must supply. It does not exist to narrate the private machinery of the function, and it does not exist to restate the code in English.

In Elixir, a complete function specification is usually spread across three places. Use `@spec` to state the input and output shapes. Use `@doc` to state the behavior in plain language. Use examples, and when possible doctests, to show the contract in action. Do not confuse these responsibilities. `@spec` alone is not a complete specification, because types do not tell the full story of what the function promises.

## Requirements

NON-NEGOTIABLE-REQUIREMENTS:
* Do not describe private structure in the public contract unless callers are meant to rely on it.
* Do not write documentation that merely mirrors the function name.
* Do not leave error behavior implicit.
* Do not hide failure inside a normal-looking return value when the failure matters to callers.
* Do not assume examples are optional. They are part of the work.
* Do not make a caller reverse-engineer the implementation to learn the contract.

## Guidelines

A complete beginner should be able to read the function’s `@doc`, `@spec`, and examples and use the function correctly without opening the function body. That is the standard.

If the documentation tells the caller what they may rely on, if the typespec states the visible shapes, if the examples prove the intended usage, and if the implementation can later change without breaking those promises, then the function is specified correctly.

## Begin with the promise, not the mechanism

Before you write a single line of `@spec`, decide what the function guarantees. Ask one question: After a caller invokes this function correctly, what result are they entitled to expect? Then ask the paired question: what must already be true before the function is called. Those two answers are the heart of the specification.

Keep the caller’s point of view in focus. A caller needs to know what values are accepted, what value comes back, and what happens at the edges. A caller does not need to know that you traverse a list left to right, that you store state in a map, or that you happen to normalize an option before dispatching to a helper. Those are implementation details. Leave them out unless they are part of the public promise.

When beginners get this wrong, they usually write “specifications” that describe the current code path rather than the contract. That is brittle. It forces readers to depend on details you may later change. A good specification survives refactoring because it describes what must remain true even when the implementation changes.

## Treat `@spec` as the type-shaped edge of the contract

Use `@spec` to answer a narrow but essential question: what kinds of values go in, and what kinds of values come back out.

Write signatures that are readable to a human. Use `String.t()`, `map()`, `keyword()`, `{:ok, value}` and `{:error, reason}` forms when those are the real shapes callers work with. If the same domain concept appears repeatedly, define a named type with `@type` and document it with `@typedoc`. That makes the public surface easier to understand and harder to misuse.

Do not try to force the entire behavior into the typespec. Typespecs in Elixir are useful documentation and useful to tools, but they are not the whole contract. They do not replace prose. They do not adequately explain semantic constraints such as “must be a base-10 string with no trailing characters” or “returns the first matching item, not any matching item.” Put those guarantees in `@doc`, where a human reader will actually find them.

A useful rule is this: if the caller could violate the rule while still technically satisfying the type, the rule belongs in prose as well as, or instead of, in the typespec.

## Use `@doc` to say what the function means

Your documentation should tell the truth from the caller’s perspective in the fewest words that still remove ambiguity. Start with one sentence that says what the function does. Make it concrete. Then immediately state the conditions the caller must meet and the guarantees the function provides.

Do not write vague summaries such as “Parses a value” or “Normalizes options.” Those phrases hide the contract instead of clarifying it. State what is parsed, what counts as valid input, and what exact form the result takes. State whether failure is reported by a tagged tuple, by a raised exception, or by a defined return value.

Document boundary behavior on purpose. Say what happens with empty strings, empty lists, missing keys, `nil`, duplicates, and out-of-range values if those cases matter. Beginners often omit these cases because the implementation already “handles” them somewhere in code. That is exactly backward. If a boundary case matters, the specification must make it visible.

## Make a clear decision about invalid input

Every public function has to answer the same uncomfortable question: what happens when the caller gives you bad input or calls the function outside its intended domain.

Resolve that question explicitly. Do not leave it to implication.

There are two honest patterns. The first is to require a precondition and say so plainly. In that design, the function is only defined when some condition holds. The second is to define the failure behavior as part of the contract, usually by returning a tagged error tuple or by raising a specific exception.

Pick one. Then document it.

Do not use a magic return value that looks like regular data but secretly signals failure. That style produces code that is harder to read, easier to misuse, and harder to verify. If failure is part of the public behavior, make it explicit in both the docs and the return shape.

## Use examples to remove doubt

A specification that cannot survive contact with examples is not ready. After the prose and the typespec are written, add examples that exercise the main success path and the important failure or edge cases.

Examples do real work. They show the exact shape of the return value. They show how the function is intended to be called. They expose ambiguity in words that looked “good enough” before you had to demonstrate them. In Elixir, doctests are especially valuable because they keep examples from drifting out of sync with the code.

When an example cannot be doctested because it depends on state, time, I/O, or side effects, still include the example. A non-doctested example is better than a vague contract. A doctested example is better still.

## Write stronger specifications than you think you need

A weak specification permits bad implementations. That is not theory; it is the ordinary failure mode of imprecise API design.

If you write “returns an index of `x` in the list,” you have not said whether it returns the first match, the last match, or any match. If callers care which one they receive, the specification is too weak. If they truly do not care, the weaker specification may be correct. The point is not to maximize detail. The point is to state exactly the guarantees callers need and no fewer.

Strengthen a specification until it rules out useless or surprising implementations, but stop before you overconstrain harmless implementation freedom. That balance is the craft. A good specification is restrictive enough to be dependable and general enough to allow future changes.

## Separate public contracts from private notes

Documentation is for users of the function. Code comments are for people reading the implementation. Keep those roles separate.

Use `@doc` for the behavior that callers may rely on. Use comments for implementation notes such as why a workaround exists, why a helper is structured a certain way, or what tradeoff was chosen inside the function body. If a fact would still matter after you rewrote the implementation from scratch, it probably belongs in the documentation. If it would disappear with the current code path, it probably belongs in a comment.

That distinction matters because a public function contract should stay stable while the private code underneath it evolves.

## A practical pattern to follow

When you add a public function, write it in this order.

First, write one sentence in plain English that says what the function does for a caller.

Second, write the `@spec` that captures the argument and return shapes.

Third, write the behavior details in `@doc`: what inputs are accepted, what output is guaranteed, what error behavior exists, and what edge cases are defined.

Fourth, add at least one success example and one edge or failure example.

Fifth, read the documentation without looking at the implementation. If a reasonable caller would still have to inspect the code to know how to use the function correctly, the specification is not finished.

## A concrete example

    defmodule MyApp.Parser do
      @doc """
      Parse `text` as a positive base-10 integer.

      `text` must contain only digits and must represent a value greater than zero.

      Return `{:ok, integer}` when parsing succeeds.
      Return `{:error, :invalid_integer}` when `text` is empty, contains non-digit
      characters, or does not represent a positive integer.

      ## Examples

          iex> MyApp.Parser.parse_positive_integer("42")
          {:ok, 42}

          iex> MyApp.Parser.parse_positive_integer("0")
          {:error, :invalid_integer}

          iex> MyApp.Parser.parse_positive_integer("12x")
          {:error, :invalid_integer}
      """
      @spec parse_positive_integer(String.t()) ::
              {:ok, pos_integer()} | {:error, :invalid_integer}
      def parse_positive_integer(text) do
        case Integer.parse(text) do
          {n, ""} when n > 0 -> {:ok, n}
          _ -> {:error, :invalid_integer}
        end
      end
    end

This example is worth studying for the division of labor alone. The `@spec` states the visible shapes. The prose states the semantic rules that the type line cannot express by itself. The examples demonstrate the contract at the boundary cases that callers are most likely to get wrong.
