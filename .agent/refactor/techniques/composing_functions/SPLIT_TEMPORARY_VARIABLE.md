# Split Temporary Variable

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## When to use

Use when any of the following are true:

- The same local binding name is bound to 2 or more different derived values in one function.

- A local name is doing more than one job. A name is "doing more than one job" if its bindings represent different conceptual values rather than the same value carried forward unchanged.

- A later rebinding is not just a final return formatting step, but a different calculation stage.

- You must read surrounding lines to know what the current binding means.

- You want to extract one calculation step, but the current name is reused across multiple steps.

## Problem

A local binding name is rebound to store different intermediate values in the same function clause (excluding loop-style accumulator patterns where rebinding is the point of the construct).

```elixir
def distance_travelled(seconds) do
  result = 0.5 * 10 * seconds * seconds
  if seconds > 5 do
    result = result + (seconds - 5) * 20
    result
  else
    result
  end
end
```

## Solution

Introduce a separate local binding for each distinct intermediate value.
Keep one name per value meaning. Do not reuse the same local name for different calculation stages. Replace repeated rebinding of one name with multiple clearly named bindings.

```elixir
def distance_travelled(seconds) do
  primary_distance = 0.5 * 10 * seconds * seconds

  if seconds > 5 do
    secondary_distance = (seconds - 5) * 20
    primary_distance + secondary_distance
  else
    primary_distance
  end
end
```

## Why Refactor

This improves code in the following ways:

- One local name no longer refers to multiple derived values.

- Each calculation stage becomes explicit and independently inspectable.

- It reduces mistakes caused by using a later rebinding where an earlier value was intended.

- It makes later refactorings easier (especially `Extract Function` and `Extract Variable`).

- It improves debugging because each intermediate value has a stable name.

## Benefits

- Code becomes easier to trace because each binding has one meaning.

- It also prepares the code for `Extract Function`, since intermediate values already have clear names and boundaries.

## How to Refactor

1. Find a local binding name that is rebound to represent different values in the same function clause.

2. Identify the first binding and name it for the value it actually represents.

3. Replace uses that refer to that first value with the new name.

4. For the next rebinding, introduce a different local binding name that describes the new value.

5. Replace later uses so each line refers to the correct binding for that stage.

6. Repeat until each distinct intermediate value has its own name.

7. Run formatter and tests after each change.

## Validation

The refactoring is successful if all of the following are true:

- Behaviour is unchanged (tests pass).

- No local binding name is reused for different conceptual values in the same function clause.

- Each intermediate calculation stage has a distinct local binding.

- The refactored function can be read line-by-line without tracking changing meaning for one local name.

- At least one previously reused temporary now has a name that states the value it holds.

## Helps Other Refactoring Techniques

- `Extract Function`

## Similar Refactoring Techniques

- `Extract Variable`
- `Remove Assignments to Parameters`

## Anti-Refactoring

- `Inline Temp`
