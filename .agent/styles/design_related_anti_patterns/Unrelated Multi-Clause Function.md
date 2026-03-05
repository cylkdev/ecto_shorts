# Unrelated Multi-Clause Function

**Problem**

Multi-clause functions are great when each clause is a coherent part of the same concept. They become an anti-pattern when a single function name is used as a dumping ground for unrelated behaviors ("because it's convenient to pattern match").

That produces functions that are hard to discover and hard to change because "what does this function do?" depends entirely on the input type.

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
