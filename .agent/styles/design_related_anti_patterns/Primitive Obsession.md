# Primitive Obsession

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

**Problem**

Using generic primitives (strings, integers, floats, bare maps) to represent rich domain concepts leads to code that is easy to misuse. Callers can pass "any string" where only a specific kind of string is valid.

This tends to leak validation and parsing concerns throughout the codebase.

**Example**

Representing money as a float:

```elixir
def total_with_tax(subtotal, tax_rate) do
  subtotal + subtotal * tax_rate
end
```

Floating-point is a poor fit for currency, and nothing enforces a currency or rounding strategy.

**Refactoring**

Use an explicit representation. A simple, common choice is integer "cents" plus an explicit currency:

```elixir
defmodule MyApp.Money do
  defstruct [:amount_cents, :currency]
end

def total_with_tax(%MyApp.Money{amount_cents: cents, currency: currency}, tax_rate) do
  taxed = cents + trunc(cents * tax_rate)
  %MyApp.Money{amount_cents: taxed, currency: currency}
end
```

Even if you later adopt a full-featured money library, the key idea remains: make the domain concept explicit.
