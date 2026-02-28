# Hide Delegate

## When to use

Use when any of the following are true:

- Callers repeatedly traverse nested structs/maps to fetch one value.
- Calling modules know too much about internal relationships.
- Internal shape changes force widespread call-site edits.
- Message chains are common in callers.

## Problem

Callers reach through one domain boundary into another module's internal shape.

```elixir
order.customer.address.city
```

This leaks `customer` and `address` layout into every caller.

## Solution

Expose a direct function on the boundary module and keep traversal internal.

```elixir
defmodule Order do
  def shipping_city(%Order{customer: %{address: %{city: city}}}), do: city
end
```

Callers switch to `Order.shipping_city(order)`.

## Why Refactor

This improves code in the following ways:

- Reduces call-site coupling to nested data shapes.
- Isolates internal structure behind a stable API.
- Shortens calling code.
- Decreases ripple effects from schema changes.

## Benefits

- Better encapsulation across modules.
- Fewer message chains.
- Easier struct/schema evolution.

## Drawbacks

- Overuse can create too many forwarding functions.
- Excessive forwarding can introduce a middle module smell.

## How to Refactor

1. Find repeated nested access patterns in callers.
2. Add explicit query/access functions at the boundary module.
3. Move traversal logic inside the boundary module.
4. Update callers to use the boundary functions.
5. Run formatter and tests.

## Validation

- Callers no longer traverse internal nested shapes directly.
- Behaviour is unchanged (tests pass).
- Internal structure can evolve with minimal caller edits.

## Eliminates Code Smell

- `Message Chains`
- `Inappropriate Intimacy`

## Similar Refactoring Techniques

- `Remove Middle Man`
