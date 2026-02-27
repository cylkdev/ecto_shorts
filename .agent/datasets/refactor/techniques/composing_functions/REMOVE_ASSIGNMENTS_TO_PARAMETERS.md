# Remove Assignments to Parameters

Use when any of the following are true:

- A parameter name is rebound 1 or more times inside the function clause.

- The parameter is read both before and after rebinding.

- Multiple transformations are applied by repeatedly reusing the same parameter name (for example `limit = ...` then `limit = ...`).

- The parameter’s original meaning (input value) and transformed meaning (derived value) are both needed to reason about the clause.

- A later expression depends on knowing which version of the parameter name is in scope.

## Problem

A parameter from the function head is rebound in the function body.

In Elixir, values are immutable, but names can be rebound. That means a parameter name can be reused for a new value later in the clause, so the same name refers to different values at different points.

```elixir
def normalize_limit(limit, opts) do
  limit = if opts[:clamp] && limit < 0, do: 0, else: limit
  limit = if opts[:max] && limit > opts[:max], do: opts[:max], else: limit
  limit
end
```

## Solution

Keep the parameter name for the original input value, and bind each transformed value to a new local name that describes the result.

This makes the data flow explicit: `input value` -> `intermediate value` -> `final value`.

```elixir
def normalize_limit(limit, opts) do
  clamped_limit =
    if opts[:clamp] && limit < 0 do
      0
    else
      limit
    end

  normalized_limit =
    if opts[:max] && clamped_limit > opts[:max] do
      opts[:max]
    else
      clamped_limit
    end

  normalized_limit
end
```

## Why Refactor

This improves code in the following ways:

- One binding name no longer represents multiple values in the same clause.

- The parameter name continues to mean the original function input.

- Each transformation step has its own explicit binding.

- It reduces bugs caused by accidentally using the wrong rebound value.

- It makes later refactorings easier (for example, extracting a private helper for one transformation step).

## Benefits

Each part of the function has one clear responsibility. This makes maintenance easier because you can change one transformation step without changing the meaning of parameter inputs.

This refactoring helps to extract `repetitive code to separate functions`.

## How to Refactor

1. Find a function where a parameter name is rebound in the function body.

2. Identify the first rebinding of that parameter.

3. Create a new local binding name for the transformed value (name the result, not the operation syntax).

4. Replace that rebinding with an assignment to the new local binding.

5. Update subsequent uses:
  
  - keep using the original parameter name when the original input is intended

  - use the new binding name when the transformed value is intended

6. Repeat for each later rebinding of the same parameter.

7. Run formatter and tests after each rename/rebinding removal.

## Validation

The refactoring is successful if all of the following are true:

- Behaviour is unchanged (tests pass).

- No parameter name is rebound in the function body.

- The original parameter name refers only to the original input value.

- Each transformation stage has a distinct local binding name.

- Later expressions no longer depend on remembering which version of the parameter is currently in scope.

## Similar Refactoring Techniques

- `Split Temporary Variable`

## Helps Other Refactoring Techniques

- `Extract Function`
