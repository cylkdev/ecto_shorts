# Long Function

## Category

Bloaters

## Description

A function contains too many lines, too many responsibilities, or too many levels of nesting. In Elixir, this often manifests as a single function clause that handles multiple distinct steps inline rather than delegating to smaller, named helpers.

## Signs and Symptoms

- A function body exceeds 15-20 lines of logic (excluding documentation and simple pattern matching).
- The function contains multiple distinct "step groups" (validation, transformation, persistence, formatting) written inline.
- Comments are used to label sections within the function body.
- Deep nesting of `case`, `cond`, `if`, or `with` expressions.
- Difficulty naming what the function does in a single phrase.
- The function requires scrolling to read in its entirety.

## Causes

- Incremental feature additions without refactoring.
- Fear of creating "too many" small functions.
- Copy-paste development where logic is duplicated inline.
- Lack of clear boundaries between steps in a pipeline.

## Example

```elixir
defmodule OrderProcessor do
  def process(order) do
    # Validate order
    if is_nil(order.customer_id) do
      {:error, :missing_customer}
    else
      if order.total <= 0 do
        {:error, :invalid_total}
      else
        # Calculate tax
        tax_rate = get_tax_rate(order.region)
        tax = order.total * tax_rate
        total_with_tax = order.total + tax

        # Apply discount
        discount = if order.coupon do
          calculate_discount(order.coupon, total_with_tax)
        else
          0
        end
        final_total = total_with_tax - discount

        # Persist
        case Repo.insert(%Invoice{
          customer_id: order.customer_id,
          total: final_total,
          tax: tax
        }) do
          {:ok, invoice} ->
            # Send notification
            Mailer.send_receipt(order.customer_id, invoice)
            {:ok, invoice}
          {:error, changeset} ->
            {:error, changeset}
        end
      end
    end
  end
end
```

## Refactored

```elixir
defmodule OrderProcessor do
  def process(order) do
    with :ok <- validate(order),
         totals <- calculate_totals(order),
         {:ok, invoice} <- persist_invoice(order, totals) do
      send_receipt(order.customer_id, invoice)
      {:ok, invoice}
    end
  end

  defp validate(%{customer_id: nil}), do: {:error, :missing_customer}
  defp validate(%{total: total}) when total <= 0, do: {:error, :invalid_total}
  defp validate(_order), do: :ok

  defp calculate_totals(order) do
    tax = calculate_tax(order)
    discount = calculate_discount(order, order.total + tax)
    %{tax: tax, final_total: order.total + tax - discount}
  end

  defp calculate_tax(order) do
    order.total * get_tax_rate(order.region)
  end

  defp calculate_discount(%{coupon: nil}, _total), do: 0
  defp calculate_discount(%{coupon: coupon}, total), do: apply_coupon(coupon, total)

  defp persist_invoice(order, totals) do
    Repo.insert(%Invoice{
      customer_id: order.customer_id,
      total: totals.final_total,
      tax: totals.tax
    })
  end

  defp send_receipt(customer_id, invoice) do
    Mailer.send_receipt(customer_id, invoice)
  end
end
```

## Treatment

- **Extract Function**: Move each step group into its own named function.
- **Replace Nested Conditional with Guard Clauses**: Use pattern matching and guards to flatten conditionals.
- **Introduce Parameter Object**: If many values are passed between steps, group them into a struct or map.
- **Use `with` for Sequential Operations**: Replace nested `case` expressions with `with` chains.

## Why Refactor

- Smaller functions are easier to read, test, and reuse.
- Named functions document intent better than inline comments.
- Pattern matching clauses replace nested conditionals.
- Each function can be tested in isolation.

## Performance Considerations

Extracting functions has negligible runtime cost in Elixir. The BEAM optimizes function calls efficiently, and the clarity benefits far outweigh any micro-overhead.

## Related Smells

- `Duplicate Code`
- `Comments`
- `Switch Statements`

## Related Refactoring Techniques

- `Extract Function`
- `Replace Nested Conditional with Guard Clauses`
- `Introduce Parameter Object`
- `Substitute Algorithm`
