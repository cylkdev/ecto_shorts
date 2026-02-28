# Temporary Field

## Category

Abstraction Abusers

## Description

A struct or map contains fields that are only populated under certain conditions or during specific operations. These fields are `nil` or meaningless most of the time, making the data structure confusing and error-prone.

## Signs and Symptoms

- Struct fields that are frequently `nil` and only set in specific code paths.
- Conditional logic that checks if a field is populated before using it.
- Fields that are only relevant during certain "phases" of processing.
- Documentation needed to explain when a field will be present.
- Pattern matching that must handle both present and absent field values.

## Causes

- Using a single struct to represent multiple states or phases.
- Adding fields for caching or memoization without proper encapsulation.
- Evolving requirements that added optional fields over time.
- Avoiding the creation of separate structs for different states.

## Example

```elixir
defmodule Order do
  defstruct [
    :id,
    :items,
    :customer_id,
    # Only set after calculation
    :subtotal,
    :tax,
    :total,
    # Only set after payment
    :payment_id,
    :paid_at,
    # Only set after shipping
    :tracking_number,
    :shipped_at,
    :carrier
  ]
end

defmodule OrderProcessor do
  def calculate_totals(%Order{} = order) do
    subtotal = Enum.sum(Enum.map(order.items, & &1.price))
    tax = subtotal * 0.1
    %{order | subtotal: subtotal, tax: tax, total: subtotal + tax}
  end

  def process_payment(%Order{total: nil}) do
    {:error, :totals_not_calculated}
  end

  def process_payment(%Order{total: total} = order) do
    case PaymentGateway.charge(order.customer_id, total) do
      {:ok, payment_id} ->
        {:ok, %{order | payment_id: payment_id, paid_at: DateTime.utc_now()}}
      error ->
        error
    end
  end

  def ship(%Order{paid_at: nil}) do
    {:error, :not_paid}
  end

  def ship(%Order{} = order) do
    {:ok, tracking} = ShippingService.create_shipment(order)
    {:ok, %{order | tracking_number: tracking.number, shipped_at: DateTime.utc_now(), carrier: tracking.carrier}}
  end
end
```

## Refactored

```elixir
defmodule Order.Draft do
  @enforce_keys [:id, :items, :customer_id]
  defstruct [:id, :items, :customer_id]

  @type t :: %__MODULE__{
    id: String.t(),
    items: [map()],
    customer_id: String.t()
  }
end

defmodule Order.Calculated do
  @enforce_keys [:id, :items, :customer_id, :subtotal, :tax, :total]
  defstruct [:id, :items, :customer_id, :subtotal, :tax, :total]

  @type t :: %__MODULE__{
    id: String.t(),
    items: [map()],
    customer_id: String.t(),
    subtotal: Decimal.t(),
    tax: Decimal.t(),
    total: Decimal.t()
  }
end

defmodule Order.Paid do
  @enforce_keys [:id, :items, :customer_id, :subtotal, :tax, :total, :payment_id, :paid_at]
  defstruct [:id, :items, :customer_id, :subtotal, :tax, :total, :payment_id, :paid_at]
end

defmodule Order.Shipped do
  @enforce_keys [:id, :items, :customer_id, :subtotal, :tax, :total, :payment_id, :paid_at, :tracking_number, :shipped_at, :carrier]
  defstruct [:id, :items, :customer_id, :subtotal, :tax, :total, :payment_id, :paid_at, :tracking_number, :shipped_at, :carrier]
end

defmodule OrderProcessor do
  alias Order.{Draft, Calculated, Paid, Shipped}

  @spec calculate_totals(Draft.t()) :: Calculated.t()
  def calculate_totals(%Draft{} = order) do
    subtotal = Enum.sum(Enum.map(order.items, & &1.price))
    tax = subtotal * 0.1

    %Calculated{
      id: order.id,
      items: order.items,
      customer_id: order.customer_id,
      subtotal: subtotal,
      tax: tax,
      total: subtotal + tax
    }
  end

  @spec process_payment(Calculated.t()) :: {:ok, Paid.t()} | {:error, term()}
  def process_payment(%Calculated{} = order) do
    case PaymentGateway.charge(order.customer_id, order.total) do
      {:ok, payment_id} ->
        {:ok, %Paid{
          id: order.id,
          items: order.items,
          customer_id: order.customer_id,
          subtotal: order.subtotal,
          tax: order.tax,
          total: order.total,
          payment_id: payment_id,
          paid_at: DateTime.utc_now()
        }}
      error ->
        error
    end
  end

  @spec ship(Paid.t()) :: {:ok, Shipped.t()} | {:error, term()}
  def ship(%Paid{} = order) do
    {:ok, tracking} = ShippingService.create_shipment(order)

    {:ok, %Shipped{
      id: order.id,
      items: order.items,
      customer_id: order.customer_id,
      subtotal: order.subtotal,
      tax: order.tax,
      total: order.total,
      payment_id: order.payment_id,
      paid_at: order.paid_at,
      tracking_number: tracking.number,
      shipped_at: DateTime.utc_now(),
      carrier: tracking.carrier
    }}
  end
end
```

## Treatment

- **Extract Struct**: Create separate structs for each state or phase.
- **Replace Data Value with Struct**: Move temporary data into a dedicated struct.
- **Use State Machine Pattern**: Model state transitions explicitly.
- **Introduce Null Object**: For optional associated data, use a null object pattern.

## Why Refactor

- Each struct has only the fields it needs—no `nil` checks required.
- Type specs document valid state transitions.
- Pattern matching enforces correct operation order at compile time.
- Dialyzer can catch invalid state transitions.
- Code is self-documenting about what data is available when.

## Alternative: Embedded Struct

For simpler cases, embed optional data in a dedicated field:

```elixir
defmodule Order do
  defstruct [:id, :items, :customer_id, :payment, :shipment]
end

defmodule Order.Payment do
  defstruct [:payment_id, :paid_at]
end

defmodule Order.Shipment do
  defstruct [:tracking_number, :shipped_at, :carrier]
end
```

## Related Smells

- `Large Module`
- `Primitive Obsession`
- `Long Function`

## Related Refactoring Techniques

- `Extract Module`
- `Replace Data Value with Struct`
- `Introduce Null Object`
