---
trigger: always_on
description: Apply in any module with many pattern-matched clauses of the same function (e.g., DSL or macro helpers). For consistency, every clause must use do ... end; forbid do: one-liners within these grouped definitions.
---

Rule: Always Use `do ... end` for Function Definitions in Pattern-Dense Modules

---

In Elixir modules that define multiple clauses of the same function — particularly in DSL builders or macro-heavy modules always use the `do ... end` multi-line block style, regardless of how short the body is.

One-liner definitions using do: (even when split across multiple lines with commas) create inconsistency and visual noise, especially when some clauses use blocks and others use inline definitions. For clarity, consistency, and maintainability, any function clause that is part of a grouped, pattern-matched series should use a block.

This applies even when:

- The function head spans multiple lines with trailing commas.
- The function body is a single expression.
- The total line length is short.

The following are invalid under this rule:


```elixir

def some_func(:option, input),

do: do_something(input)

def some_func(:option, nil), do: fallback()

```

These should be written as:

```elixir

def some_func(:option, input) do
	do_something(input)
end

def some_func(:option, nil) do
	fallback()
end

```

---

Required

- All function clauses that share the same name with different pattern-matched heads must be written using `do ... end`
- This applies even if the function head spans multiple lines using commas
- Applies to private and public functions in DSLs, macro systems, and expression generators
- Ensures vertical consistency and structural readability

---

Not Allowed

- `do:` one-liners, regardless of line length, when part of grouped pattern clauses
- Mixing inline and block definitions within the same function family

---

Exceptions

- One-liners may only be used **outside** grouped multi-clause definitions
- They must fit within a single physical line (no trailing commas or multi-line heads)
- They should only be used for trivial utility functions that are visually isolated

---

Rationale

1. Pattern-matching complexity makes compactness a liability: When multiple clauses share a function name, clarity trumps brevity.

2. Multi-line heads using commas are functionally equivalent to full definitions: If the head spans lines, the body should never be forced into a one-liner for the sake of brevity.

3. Consistency helps navigate and maintain function groups: Developers can scan for function heads and know exactly where bodies begin and end.