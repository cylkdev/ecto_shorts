# Long Function

A "long function" is a function that contains multiple distinct steps in one body and is long enough that a human novice reader would likely have to mentally split it into parts to understand it. If a function is more than about 10 lines, start asking whether parts should be extracted.

Ask these questions in order to decide whether to refactor with `Extract Function`:

1. Does the function contain an inline comment (`# ...`) that explains a specific block of executable statements (not a TODO/license/doc comment)?

- Yes: Extract that block into a named function.
- No: Continue.

2. Does the function contain a repeated contiguous sequence of 2+ executable statements (exact or parameterizable duplication) within the same module/file?

- Yes: Extract the repeated sequence.
- No: Continue.

3. Can the function body be split into 2 or more contiguous statement groups where each group uses a different operation category from this fixed set?

- Categories:
  - validation (guards, validation checks, error tuples)
  - transformation (map/list/struct shaping)
  - querying/fetching (repo/API/file reads)
  - persistence/output (repo writes, file writes, IO)
  - formatting/presentation (string building, rendering)
  - control orchestration (branch routing only)

- Yes (2+ categories present in separate groups): Extract at least one group.
- No: Continue.

4. Does any single `if`, `case`, `cond`, `with`, or function clause body contain more than 5 executable statements?

- Yes: Extract that branch body (or sub-block).
- No: Continue.

This is deterministic because statement count is countable.

5. Does any loop-like block or callback body (`Enum.map`, `Enum.reduce`, `for`, etc.) contain more than 3 executable statements?

- Yes: Extract callback logic.
- No: Continue.

6. Does the function introduce more than 7 local variables before returning?

Count only variables assigned in the function body (exclude function parameters and pattern-match destructuring in the function head).

- Yes: Extract one or more logical blocks.
- No: Continue.

7. Is the function body longer than 10 executable statements (excluding blank lines and comments)?

- Yes: Mandatory extraction review:
  - If any of checks 3, 4, 5, or 6 is also true, refactor now.
  - If none are true, do not refactor solely for length.
- No: Continue.

8. Does a contiguous block compute a value that is used later, and that block can be replaced by a single function call returning that value (or tuple/map) without changing behaviour?

- Yes: Extract the block.
- No: Continue.

## Signs and Symptoms

A function contains too many lines of code. Generally, any function longer than ten lines should make you start asking questions.

## Reasons for the Problem

This happens when you keep adding more pattern matches, branches, and logic to the same function instead of extracting small, single-purpose functions. It feels faster to add “just one more clause” than to name and design a new function, but the result is a long function that’s hard to read, test, and compose. Over time it stops matching Elixir’s preference for small, clear functions and becomes difficult to understand and maintain.

## Treatment

If a comment must be added to explain what a section of code does, that’s a sign it should be extracted into its own function with a clear, descriptive name. Even a single line can be extracted if the name makes the intent obvious, because clear function names remove the need for explanatory comments. This keeps functions short, composable, and easy to test.

- To reduce the length of a function body, use `Extract Function`.

- If local variables and parameters interfere with extracting a function, use `Replace Temp with Query`, `Introduce Parameter Struct`, or `Preserve Whole Struct`.

- If none of the previous techniques help, move the entire function to a focused module via `Replace Function with Module`.

- Conditional operators and loops are a good clue that code can be moved to a separate function. For conditionals, use `Decompose Conditional`. If loops are in the way, try `Extract Function`.

## Payoff

Modules composed of small, focused functions tend to live the longest. The longer a function becomes, the harder it is to read, reason about, test, and change safely.

Long functions also create a place where duplicate logic can hide. When similar branches or steps are repeated inside the same function body instead of being extracted into a shared helper, the duplication is harder to see and harder to remove.

## Performance

Having more small functions does not meaningfully hurt performance in practice, so it isn’t something you should optimize for prematurely. The cost of an extra function call on the BEAM is negligible compared to the benefits in clarity and maintainability.

Because the code is easier to read and reason about, it becomes much simpler to spot the real performance issues and apply the right optimizations when they actually matter.
