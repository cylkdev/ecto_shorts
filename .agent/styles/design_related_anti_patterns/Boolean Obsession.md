# Boolean Obsession

**Problem**

Booleans are easy to add, but many booleans quickly become unclear at the call site: `enabled?: true`, `premium?: false`, `trial?: true`, `admin?: false`, etc.

Booleans also fail to express "impossible states". For example, a user cannot be both `trial?` and `paid?`, but two booleans allow it.

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
