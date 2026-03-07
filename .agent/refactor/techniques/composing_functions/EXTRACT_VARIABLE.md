# Extract Variable

## When to use

Use when any of the following are true:

- The expression contains 2 or more operations (for example arithmetic, comparison, boolean operators, or function calls). An operation can be any of the following:

  - arithmetic (`+`, `-`, `*`, `/`)
  - comparison (`>`, `<`, `==`, `!=`, etc.)
  - boolean logic (`and`, `or`, `&&`, `||`, `not`, `!`)
  - function call
  - map/struct access chain (for example `state.user.profile.name`)
  - conditional fragment (part of an `if`, `case`, or `cond`)
  - pipeline segment that combines multiple transformations

- The expression contains a nested function call (a call used as an argument to another call).

- The expression mixes data access, transformation, and decision in one expression.

- The same expression (or a structurally identical expression) appears more than once in the same function or clause.

- A sub-part of the expression represents a distinct domain value that can be named (for example: `subtotal_cents`, `tax_rate`, `active_user?`).

## Problem

You have one expression doing multiple steps inline, which hides intent and makes the clause harder to evolve safely.

Example:

```elixir
def render_banner(state) do
  if String.contains?(String.upcase(state.platform), "MAC") and
       String.contains?(String.upcase(state.browser), "IE") and
       was_initialized?(state) and
       state.resize > 0 do
    :ok
  else
    :noop
  end
end
```

## Solution

Bind part of the expression, or the whole expression, to a new local name that describes the domain value it represents. Then use that binding in place of the original sub-expression.

Example:

```elixir
def render_banner(state) do
  is_mac_os = String.contains?(String.upcase(state.platform), "MAC")
  is_ie = String.contains?(String.upcase(state.browser), "IE")
  was_resized = state.resize > 0

  if is_mac_os and is_ie and was_initialized?(state) and was_resized do
    :ok
  else
    :noop
  end
end
```

## Why Refactor

This improves code in the following ways:

- It reduces the number of operations evaluated in a single expression.
- It gives a stable name to an intermediate result, which makes later changes safer (you can change one step without rewriting the full expression).
- It removes duplicated expression fragments when the same calculation is used multiple times.
- It makes debugging easier because intermediate values can be inspected directly.
- It creates smaller units that can be extracted further (for example into `defp` functions).

## Benefits

- More readable code. Use binding names that clearly state intent so the code explains itself and needs fewer comments.

## Drawbacks

- You introduce more local bindings, but this is usually offset by better readability.
- Be careful when extracting boolean subexpressions that currently benefit from short-circuit evaluation. In Elixir, `and`/`or` and `&&`/`||` short-circuit; if you precompute both sides into bindings, both expressions will run, which can hurt performance or change behaviour if they do expensive work or have side effects.

## How to Refactor

1. Identify an expression (or pipeline segment) and choose a sub-expression that represents one step of the work.
2. Verify the chosen sub-expression is safe to evaluate where you plan to bind it (same order, same branch, same available bindings).
3. Create a new local binding for the sub-expression.
4. Replace the original sub-expression with the new binding.
5. Repeat if the remaining expression still contains multiple steps.
6. Run formatter and tests after each extraction.

## Validation

The refactor is successful if all of the following are true:

- The original behaviour is unchanged (tests pass).

- The extracted binding is assigned exactly the same value the sub-expression produced before.

- The remaining expression has fewer operations than before.

- The new binding name describes a value (not an implementation detail).

- No duplicated copy of the extracted sub-expression remains in the same function clause (unless intentionally kept).

## Eliminates Code Smell

- `Comments`

## Similar Refactoring Techniques

- `Extract Function`

## Anti-Refactoring

- `Inline Temp`
