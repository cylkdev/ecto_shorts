# Design-related anti-patterns

This document outlines design anti-patterns that tend to create confusing APIs, brittle code, or systems that are hard to evolve. The themes here are: make interfaces predictable, make data explicit, and keep responsibilities focused.

---

## Alternative return types

**Problem**

A single function should not return fundamentally different shapes based on options like `return: :raw` vs `return: :wrapped`. It forces callers to read implementation details, makes types unclear, and leads to “guess the shape” code at call sites.

If you need multiple return shapes, they should be different functions with different names.

**Example**

One function returns a string by default, but returns an `{:ok, value} | {:error, reason}` tuple when configured:

```elixir
defmodule MyApp.Token do
  def parse(token, opts \\ []) do
    return = Keyword.get(opts, :return, :value)

    case do_parse(token) do
      {:ok, value} when return === :tuple -> {:ok, value}
      {:ok, value} -> value
      {:error, reason} when return === :tuple -> {:error, reason}
      {:error, _reason} -> nil
    end
  end

  defp do_parse(token) do
    if is_binary(token) and byte_size(token) > 0 do
      {:ok, String.trim(token)}
    else
      {:error, :invalid}
    end
  end
end
```

**Refactoring**

Expose distinct functions whose names encode the contract:

```elixir
defmodule MyApp.Token do
  def parse(token) do
    case parse_tuple(token) do
      {:ok, value} -> value
      {:error, _} -> nil
    end
  end

  def parse_tuple(token) do
    do_parse(token)
  end

  defp do_parse(token) do
    if is_binary(token) and byte_size(token) > 0 do
      {:ok, String.trim(token)}
    else
      {:error, :invalid}
    end
  end
end
```

Each function now has a single, stable return shape.

---

## Boolean obsession

**Problem**

Booleans are easy to add, but many booleans quickly become unclear at the call site: `enabled?: true`, `premium?: false`, `trial?: true`, `admin?: false`, etc.

Booleans also fail to express “impossible states”. For example, a user cannot be both `trial?` and `paid?`, but two booleans allow it.

**Example**

```elixir
defmodule MyApp.Account do
  defstruct [:id, :email, :trial?, :paid?]
end
```

Now callers must constantly reason about combinations (`trial? && paid?`), and the data model permits nonsense.

**Refactoring**

Represent the concept as an enum-like atom (or a struct) that encodes valid states:

```elixir
defmodule MyApp.Account do
  defstruct [:id, :email, :plan]

  @type plan :: :trial | :paid
end
```

For more complex cases, use a dedicated struct (for example, `%MyApp.Plan{}`) so the model can evolve without adding more booleans everywhere.

---

## Exceptions for control-flow

**Problem**

Using exceptions as normal control-flow (for example, calling `foo!/1` and rescuing) makes intent unclear and can obscure real failures. Exceptions are best reserved for truly exceptional situations or programmer errors.

Prefer explicit, typed returns for expected failure modes (for example, `{:ok, value} | {:error, reason}`), then handle them with pattern matching.

**Example**

```elixir
def get_required(map, key) do
  try do
    Map.fetch!(map, key)
  rescue
    KeyError -> :missing
  end
end
```

**Refactoring**

```elixir
def get_required(map, key) do
  case Map.fetch(map, key) do
    {:ok, value} -> {:ok, value}
    :error -> {:error, :missing}
  end
end
```

Now control-flow is obvious and exceptions remain for unexpected problems.

---

## Primitive obsession

**Problem**

Using generic primitives (strings, integers, floats, bare maps) to represent rich domain concepts leads to code that is easy to misuse. Callers can pass “any string” where only a specific kind of string is valid.

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

Use an explicit representation. A simple, common choice is integer “cents” plus an explicit currency:

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

---

## Unrelated multi-clause function

**Problem**

Multi-clause functions are great when each clause is a coherent part of the same concept. They become an anti-pattern when a single function name is used as a dumping ground for unrelated behaviours (“because it’s convenient to pattern match”).

That produces functions that are hard to discover and hard to change because “what does this function do?” depends entirely on the input type.

**Example**

```elixir
def format(%MyApp.User{} = user), do: format_user(user)
def format(%MyApp.Invoice{} = invoice), do: format_invoice(invoice)
def format(term), do: inspect(term)
```

This `format/1` is effectively three different functions disguised as one.

**Refactoring**

Prefer separate modules or separate function names:

```elixir
defmodule MyApp.UserFormatter do
  def format(%MyApp.User{} = user), do: format_user(user)
  defp format_user(user), do: user.email
end

defmodule MyApp.InvoiceFormatter do
  def format(%MyApp.Invoice{} = invoice), do: format_invoice(invoice)
  defp format_invoice(invoice), do: invoice.id
end
```

If you truly need polymorphism, consider protocols, but use them intentionally and keep the contract explicit.

---

## Using application configuration for libraries

**Problem**

Libraries should not fetch application configuration to decide how they behave (for example, `Application.fetch_env!/2` inside library functions). It hides dependencies, makes code harder to test, and creates “action at a distance” where behaviour changes based on runtime config.

This is especially problematic when multiple consumers want different behaviour at the same time.

**Example**

```elixir
defmodule SomeLib.Client do
  def base_url do
    Application.fetch_env!(:some_lib, :base_url)
  end
end
```

Every caller is now coupled to global app config to use the library.

**Refactoring**

Prefer explicit configuration passed as arguments or stored in a struct:

```elixir
defmodule SomeLib.Client do
  defstruct [:base_url]

  def new(opts) do
    %__MODULE__{base_url: Keyword.fetch!(opts, :base_url)}
  end

  def base_url(%__MODULE__{base_url: base_url}), do: base_url
end
```

Application configuration still has a place, but it should be used at the application boundary to build the client (for example, in your app’s supervision tree), not inside the library’s core logic.
