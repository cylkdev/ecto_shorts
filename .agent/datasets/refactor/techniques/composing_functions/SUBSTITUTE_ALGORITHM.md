# Substitute Algorithm

## When to use

Use when any of the following are true:

- The function computes the same result as a simpler algorithm you can state and test.

- The implementation uses manual branching/iteration where a standard library function expresses the same behaviour directly.

- The current algorithm contains special-case logic that disappears when rewritten with a better approach.

- The requirements changed, and adapting the old algorithm would require more changes than replacing it.

- You can define input/output equivalence tests for the function and use them to validate a replacement implementation.

- The problem is not the function boundary, but the algorithm inside the function body.

## Problem

A function’s current implementation produces the required result, but the algorithm is unnecessarily complex, overly manual, or harder to change than a simpler alternative.

```elixir
def found_person(people) do
  Enum.reduce_while(people, "", fn person, _acc ->
    cond do
      person == "Don" -> {:halt, "Don"}
      person == "John" -> {:halt, "John"}
      person == "Kent" -> {:halt, "Kent"}
      true -> {:cont, ""}
    end
  end)
end
```

## Solution

Replace the body of the function (or the internal algorithm portion) with a different algorithm that produces the same externally observable behaviour.

This often means replacing custom recursion, nested `cond`, or manual state tracking with clearer `Enum`, `MapSet`, `Map`, or pattern-matching based implementations.

```elixir
def found_person(people) do
  candidates = MapSet.new(["Don", "John", "Kent"])

  Enum.find(people, "", fn person ->
    MapSet.member?(candidates, person)
  end)
end
```

## Why Refactor

This improves code in the following ways:

- It reduces branching or step count for the same output behaviour.

- It replaces custom logic with standard library behaviour when appropriate.

- It can improve correctness by removing ad hoc edge-case handling.

- It can improve maintainability because the new algorithm is easier to test and change.

- It can improve performance when the replacement uses a more suitable approach or data structure.


## How to Refactor

1. Define the current behaviour with tests before changing the algorithm (including edge cases).

2. Isolate unrelated work from the function so the algorithm portion is easier to replace.

3. Implement the replacement algorithm in a separate function (or temporary `defp`) first.

4. Run both implementations against the same test cases (or compare outputs in temporary checks).

5. If outputs differ, identify whether:
   - the old implementation had a bug, or
   - the new implementation changed behaviour unintentionally.

6. Switch the original function to use the new algorithm once behaviour matches the intended result.

7. Remove the old implementation.

8. Run tests again.

## Validation

- Behaviour is unchanged for the intended contract (tests pass).
- The function now uses the replacement algorithm at the original call boundary.
- The old algorithm implementation has been removed (or intentionally kept only temporarily during migration).
- The replacement reduces at least one measurable source of complexity (for example: fewer branches, fewer manual loop steps, fewer special cases, or use of a standard library primitive).
- Any changed behaviour is explicitly documented and covered by updated tests.


## Eliminates Code Smell

- `Duplicate Code`
- `Long Function`