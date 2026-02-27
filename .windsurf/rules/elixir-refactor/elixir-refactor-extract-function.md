---
trigger: model_decision
description: A single function clause contains multiple step groups separated by blank lines/comments. A contiguous block reads existing bindings and produces one result or side effect. The same inline block appears more than once, or nesting makes the clause hard to scan.
---

# Extract Function

## When to use

Use when any of the following are true:

- A function or clause contains 2 or more step groups that do different jobs (for example: validation, calculation, formatting, logging, persistence). A "step group" means a contiguous set of lines that together perform one purpose (for example "print details", "calculate total", "build payload", "validate params").

- A contiguous block of 2+ lines uses current bindings to produce one result or perform one action that can be named.

- The same logic appears more than once in the same module.

- A block can be moved to a function by passing required values as arguments without changing behaviour.

- A comment is used to label a block (for example `# print details`, `# calculate total`, `# normalize input`). The comment usually indicates the block is one extractable step.

## Problem

You have a code fragment inside one function clause that performs a distinct step, but it is written inline with other steps.

Example:

```elixir
defmodule Billing do
  def print_owing(account) do
    print_banner()

    # Print details.
    IO.puts("name: #{account.name}")
    IO.puts("amount: #{outstanding(account)}")
  end

  defp print_banner(), do: IO.puts("==== Owing ====")

  defp outstanding(account) do
    # imagine real computation here
    account.balance_cents / 100
  end
end
```

## Solution

Move the relevant code fragment into a new function (usually `defp`) and replace the original fragment with a function call. Pass in the bindings the fragment depends on. Return any value the caller still needs afterward. This converts an inline step into a named function with explicit inputs and outputs.

Example:

```elixir
defmodule Billing do
  def print_owing(account) do
    print_banner()
    print_details(account, outstanding(account))
  end

  defp print_details(account, outstanding_amount) do
    IO.puts("name: #{account.name}")
    IO.puts("amount: #{outstanding_amount}")
  end

  defp print_banner(), do: IO.puts("==== Owing ====")

  defp outstanding(account) do
    account.balance_cents / 100
  end
end
```

## Why Refactor

This improves code in the following ways:

- It reduces the number of lines and responsibilities in the original function or clause.

- It gives a name and arity to a step that was previously implicit in inline code.

- It allows duplicate logic to be replaced by one function call.

- It isolates behaviour behind clear function boundaries, which also makes pattern-matching clauses easier to reason about.

- It reduces the chance of accidental edits to unrelated lines when changing one step.

## Benefits

- The code is easier to read. Ensure the new function has a descriptive name that clearly indicates its purpose (for example, `print_details/2`, `print_banner/0`).

- Less code duplication. Extracted helper functions can often be reused at other call sites in the same module.

- Isolates independent parts of code, meaning errors are less likely (such as rebinding the wrong value).

## How to Refactor

1. Identify a contiguous fragment that performs one step.
2. List every binding the fragment reads that is defined outside the fragment.
3. List every value the fragment produces that is needed later by the caller.
4. Create a new function with arguments for required inputs (default to `defp` unless this is part of the module API).
5. If the step branches by input shape, prefer multiple function clauses with pattern matching and guards.
6. Move the fragment into the new function.
7. If the fragment produces a needed value, return it from the new function.
8. Replace the original fragment with a call to the new function, preserving evaluation order.
9. Run formatter and tests after each extraction.
10. Repeat while the original function still contains multiple step groups.

## Validation

The refactoring is successful if all of the following are true:

- Behaviour is unchanged (tests pass).

- The extracted function has a single stated purpose (one step group).

- The original function has fewer lines, fewer branches, or fewer step groups than before.

- All external dependencies of the extracted code are now explicit arguments (or explicit module calls).

- No duplicated copy of the extracted logic remains in the same module unless intentionally preserved.

## Eliminates Code Smell

- `Duplicate Code`
- `Long Function`
- `Feature Envy`
- `Switch Statements`
- `Message Chains`
- `Comments`
- `Data Class`

## Similar Refactoring Techniques

- `Move Method`

## Helps Other Refactoring Techniques

- `Introduce Parameter Object`
- `Form Template Method`
- `Parameterize Method `

## Anti-Refactoring

- [Inline Function](./INLINE_FUNCTION.md)
