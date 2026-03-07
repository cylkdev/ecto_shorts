# Message Chains

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Category

Couplers

## Description

A sequence of calls where one function calls another, which calls another, forming a chain. The caller must know the entire structure of intermediate values to navigate to the final value. This creates tight coupling to the internal structure of multiple modules.

## Signs and Symptoms

- Long chains of field access: `order.customer.address.city.name`
- Sequential function calls: `get_order(id) |> get_customer() |> get_address() |> get_city()`
- Knowledge of nested data structures required to access a value.
- Changes to intermediate structures break distant code.
- Nil checks needed at each step of the chain.

## Causes

- Deep nesting of data structures.
- Lack of convenience functions for common access patterns.
- Over-reliance on raw data access instead of encapsulated queries.
- Exposing internal structure instead of providing focused APIs.

## Example

```elixir
defmodule MyApp.ShippingCalculator do
  def calculate_rate(order) do
    # Message chain - must know entire structure
    country = order.customer.shipping_address.country
    region = order.customer.shipping_address.region
    postal_code = order.customer.shipping_address.postal_code

    # Another chain
    weight = Enum.reduce(order.items, 0, fn item, acc ->
      acc + item.product.shipping_details.weight * item.quantity
    end)

    # Yet another chain
    is_prime = order.customer.membership.benefits.free_shipping

    calculate(country, region, postal_code, weight, is_prime)
  end

  def format_shipping_label(order) do
    # Repeated chain access
    address = order.customer.shipping_address

    """
    #{order.customer.name}
    #{address.street_line_1}
    #{address.street_line_2}
    #{address.city}, #{address.region} #{address.postal_code}
    #{address.country}
    """
  end
end
```

## Refactored

```elixir
defmodule MyApp.Order do
  defstruct [:id, :items, :customer]

  def shipping_address(%__MODULE__{customer: customer}) do
    MyApp.Customer.shipping_address(customer)
  end

  def total_weight(%__MODULE__{items: items}) do
    Enum.reduce(items, 0, fn item, acc ->
      acc + MyApp.Item.shipping_weight(item)
    end)
  end

  def has_free_shipping?(%__MODULE__{customer: customer}) do
    MyApp.Customer.has_free_shipping?(customer)
  end
end

defmodule MyApp.Customer do
  defstruct [:name, :shipping_address, :membership]

  def shipping_address(%__MODULE__{shipping_address: address}), do: address

  def has_free_shipping?(%__MODULE__{membership: nil}), do: false
  def has_free_shipping?(%__MODULE__{membership: membership}) do
    MyApp.Membership.has_benefit?(membership, :free_shipping)
  end
end

defmodule MyApp.Address do
  defstruct [:street_line_1, :street_line_2, :city, :region, :postal_code, :country]

  def format(%__MODULE__{} = address) do
    [
      address.street_line_1,
      address.street_line_2,
      "#{address.city}, #{address.region} #{address.postal_code}",
      address.country
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join("\n")
  end

  def shipping_zone(%__MODULE__{country: country, region: region, postal_code: postal}) do
    {country, region, postal}
  end
end

defmodule MyApp.Item do
  defstruct [:product, :quantity]

  def shipping_weight(%__MODULE__{product: product, quantity: qty}) do
    MyApp.Product.weight(product) * qty
  end
end

defmodule MyApp.ShippingCalculator do
  alias MyApp.{Order, Address}

  def calculate_rate(order) do
    # No more chains - each module provides what we need
    address = Order.shipping_address(order)
    {country, region, postal} = Address.shipping_zone(address)
    weight = Order.total_weight(order)
    free_shipping? = Order.has_free_shipping?(order)

    calculate(country, region, postal, weight, free_shipping?)
  end

  def format_shipping_label(order) do
    customer_name = order.customer.name
    address = Order.shipping_address(order)

    """
    #{customer_name}
    #{Address.format(address)}
    """
  end
end
```

## Treatment

- **Hide Delegate**: Add functions that provide direct access to commonly needed values.
- **Extract Function**: Create helper functions for repeated chain access.
- **Move Function**: Move chain-dependent logic to the module that owns the data.

## Why Refactor

- Callers don't need to know internal structure.
- Changes to nested structures only affect their own modules.
- Nil handling can be centralized in delegate functions.
- Code is more readable with meaningful function names.
- Easier to test with simpler dependencies.

## The Law of Demeter

The Law of Demeter (or "principle of least knowledge") suggests a function should only call:
- Functions on its own module
- Functions on parameters passed to it
- Functions on values it creates
- Functions on its direct dependencies

Avoid: `a.b().c().d()` - this violates the law by reaching through multiple values.

## Elixir Pipelines vs Message Chains

Pipelines are not message chains when each step transforms data:

```elixir
# Good: pipeline transforms data at each step
order
|> validate()
|> calculate_totals()
|> apply_discounts()
|> persist()

# Bad: chain navigates through structure
order.customer.address.city.name
```

## Related Smells

- `Feature Envy`
- `Inappropriate Intimacy`
- `Middle Man`

## Related Refactoring Techniques

- `Hide Delegate`
- `Extract Function`
- `Move Function`
