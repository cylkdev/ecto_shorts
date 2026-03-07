# Comments

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Category

Dispensables

## Description

Comments that explain *what* code does rather than *why* it does it. When code requires extensive comments to be understood, it's often a sign that the code itself should be clearer. Good code is self-documenting through meaningful names and clear structure.

## Signs and Symptoms

- Comments that restate what the code obviously does.
- Comments used to label sections within a function (indicating the function should be split).
- Commented-out code left "just in case."
- Comments that are outdated or contradict the actual code.
- Long explanatory comments before complex logic.
- Comments apologizing for bad code ("This is a hack, but...").

## Causes

- Writing comments instead of refactoring unclear code.
- Fear of deleting code (commenting out instead).
- Lack of meaningful function and variable names.
- Complex logic that hasn't been simplified.
- Documentation requirements interpreted as "comment everything."

## Example

```elixir
defmodule OrderProcessor do
  def process(order) do
    # Check if order is valid
    # Order must have items and a customer
    if order.items != [] and order.customer_id != nil do
      # Calculate the subtotal
      # Loop through items and sum prices
      subtotal = Enum.reduce(order.items, 0, fn item, acc ->
        # Multiply price by quantity and add to accumulator
        acc + item.price * item.quantity
      end)

      # Calculate tax (10%)
      tax = subtotal * 0.1

      # Calculate total
      total = subtotal + tax

      # Old discount logic - keeping just in case
      # discount = if order.coupon do
      #   apply_coupon(order.coupon, total)
      # else
      #   0
      # end

      # Create invoice record
      # This is a bit hacky but it works
      invoice = %Invoice{
        order_id: order.id,
        subtotal: subtotal,
        tax: tax,
        total: total
      }

      # Save to database
      Repo.insert(invoice)
    else
      # Return error if invalid
      {:error, :invalid_order}
    end
  end
end
```

## Refactored

```elixir
defmodule OrderProcessor do
  @tax_rate 0.1

  def process(order) do
    with :ok <- validate(order),
         totals <- calculate_totals(order),
         {:ok, invoice} <- create_invoice(order, totals) do
      {:ok, invoice}
    end
  end

  defp validate(%{items: [], customer_id: _}), do: {:error, :no_items}
  defp validate(%{items: _, customer_id: nil}), do: {:error, :no_customer}
  defp validate(_order), do: :ok

  defp calculate_totals(order) do
    subtotal = calculate_subtotal(order.items)
    tax = subtotal * @tax_rate
    %{subtotal: subtotal, tax: tax, total: subtotal + tax}
  end

  defp calculate_subtotal(items) do
    Enum.reduce(items, 0, &(&1.price * &1.quantity + &2))
  end

  defp create_invoice(order, totals) do
    %Invoice{
      order_id: order.id,
      subtotal: totals.subtotal,
      tax: totals.tax,
      total: totals.total
    }
    |> Repo.insert()
  end
end
```

## Treatment

- **Extract Function**: Replace commented code blocks with well-named functions.
- Use descriptive variable names instead of comments explaining them.
- **Extract Variable**: Extract complex expressions into named variables.
- Remove commented-out code; version control already preserves history.
- **Replace Magic Number with Module Attribute**: Replace magic-number comments with named constants.

## When Comments Are Valuable

Not all comments are bad. Keep comments that explain:

- **Why** (not what): Business reasons, edge cases, non-obvious decisions
- **External constraints**: API limitations, regulatory requirements
- **Warnings**: Performance implications, security considerations
- **TODOs**: With ticket references for tracking

```elixir
defmodule PaymentProcessor do
  # Stripe requires amounts in cents, not dollars
  # See: https://stripe.com/.docs/currencies#zero-decimal
  defp to_cents(dollars), do: round(dollars * 100)

  # Rate limit: max 100 requests/second per Stripe docs
  # TODO(TICKET-123): Implement proper rate limiting
  defp charge(amount, token) do
    # ...
  end
end
```

## Elixir-Specific Documentation

Use `@doc` and `@moduledoc` for API documentation, not inline comments:

```elixir
defmodule MyApp.Calculator do
  @moduledoc """
  Provides financial calculation utilities.
  """

  @doc """
  Calculates compound interest.

  ## Examples

      iex> Calculator.compound_interest(1000, 0.05, 12)
      1795.86

  """
  @spec compound_interest(number(), float(), pos_integer()) :: float()
  def compound_interest(principal, rate, periods) do
    principal * :math.pow(1 + rate, periods)
  end
end
```

## Related Smells

- `Long Function`
- `Duplicate Code`
- `Dead Code`

## Related Refactoring Techniques

- `Extract Function`
- `Extract Variable`
- `Extract Module`
- `Replace Magic Number with Module Attribute`
