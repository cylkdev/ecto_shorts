# Inappropriate Intimacy

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Category

Couplers

## Description

Two modules are too tightly coupled, with each knowing too much about the other's internal implementation details. They access each other's private data, depend on internal structure, or have circular dependencies.

## Signs and Symptoms

- Modules that frequently change together.
- Direct access to another module's internal struct fields that aren't part of its public API.
- Circular dependencies between modules.
- One module reaching deep into another's data structures.
- Knowledge of another module's implementation details (not just its interface).
- Difficulty testing one module without the other.

## Causes

- Modules that evolved together without clear boundaries.
- Shortcuts taken to "just get it working."
- Lack of defined public APIs for modules.
- Over-familiarity between related concepts.
- Splitting a module without properly defining interfaces.

## Example

```elixir
defmodule MyApp.Order do
  defstruct [:id, :items, :customer, :internal_state, :_cache]

  def new(items, customer) do
    %__MODULE__{
      id: Ecto.UUID.generate(),
      items: items,
      customer: customer,
      internal_state: %{validated: false, processed: false},
      _cache: %{}
    }
  end
end

defmodule MyApp.OrderProcessor do
  alias MyApp.Order

  def process(order) do
    # Inappropriate: directly accessing internal state
    if order.internal_state.validated do
      # Inappropriate: modifying internal state directly
      updated_state = Map.put(order.internal_state, :processed, true)
      order = %{order | internal_state: updated_state}

      # Inappropriate: using internal cache
      cached = Map.get(order._cache, :total)
      total = cached || calculate_total(order)

      # Inappropriate: modifying cache directly
      order = %{order | _cache: Map.put(order._cache, :total, total)}

      {:ok, order}
    else
      {:error, :not_validated}
    end
  end

  def calculate_total(order) do
    # Inappropriate: knowing the internal structure of items
    Enum.reduce(order.items, 0, fn item, acc ->
      # Assuming internal price structure
      acc + item._internal_price * item.quantity
    end)
  end
end

defmodule MyApp.OrderValidator do
  alias MyApp.Order

  def validate(order) do
    # Inappropriate: directly modifying internal state
    updated_state = Map.put(order.internal_state, :validated, true)
    %{order | internal_state: updated_state}
  end
end
```

## Refactored

```elixir
defmodule MyApp.Order do
  @enforce_keys [:items, :customer]
  defstruct [:id, :items, :customer, :status]

  @type status :: :draft | :validated | :processed

  def new(items, customer) do
    %__MODULE__{
      id: Ecto.UUID.generate(),
      items: items,
      customer: customer,
      status: :draft
    }
  end

  # Public API for state transitions
  def validate(%__MODULE__{status: :draft} = order) do
    {:ok, %{order | status: :validated}}
  end
  def validate(%__MODULE__{}), do: {:error, :invalid_status}

  def mark_processed(%__MODULE__{status: :validated} = order) do
    {:ok, %{order | status: :processed}}
  end
  def mark_processed(%__MODULE__{}), do: {:error, :not_validated}

  # Public API for queries
  def validated?(%__MODULE__{status: :validated}), do: true
  def validated?(%__MODULE__{}), do: false

  def total(%__MODULE__{items: items}) do
    Enum.reduce(items, 0, fn item, acc ->
      acc + item.price * item.quantity
    end)
  end
end

defmodule MyApp.OrderProcessor do
  alias MyApp.Order

  def process(order) do
    # Uses public API only
    if Order.validated?(order) do
      total = Order.total(order)

      with {:ok, processed_order} <- Order.mark_processed(order) do
        {:ok, processed_order, total}
      end
    else
      {:error, :not_validated}
    end
  end
end

defmodule MyApp.OrderValidator do
  alias MyApp.Order

  def validate(order) do
    # Uses public API
    Order.validate(order)
  end
end
```

## Treatment

- **Hide Delegate**: Create public functions that hide internal details.
- **Move Function**: Move functions to the module whose data they manipulate.
- **Extract Module**: If two modules are too intertwined, extract shared logic.
- **Change Bidirectional Association to Unidirectional**: Remove circular dependencies.

## Why Refactor

- Modules can change independently.
- Clear public APIs make modules easier to understand.
- Reduced coupling makes testing easier.
- Internal implementation can change without affecting other modules.
- Eliminates circular dependency issues.

## Detecting Inappropriate Intimacy

Ask these questions:

1. Does module A access fields of module B's structs that aren't documented as public?
2. Does module A modify module B's internal state directly?
3. Do both modules need to change when one's implementation changes?
4. Is there a circular dependency (A calls B, B calls A)?

## Establishing Boundaries

```elixir
defmodule MyApp.Order do
  # Private - prefixed with underscore, not documented
  defstruct [:id, :items, :customer, :_internal_state]

  # Public API - documented, stable
  @doc "Returns the order total"
  def total(%__MODULE__{} = order), do: calculate_total(order.items)

  @doc "Checks if order can be shipped"
  def shippable?(%__MODULE__{} = order), do: # ...

  # Private implementation
  defp calculate_total(items), do: # ...
end
```

## Related Smells

- `Feature Envy`
- `Message Chains`
- `Shotgun Surgery`

## Related Refactoring Techniques

- `Hide Delegate`
- `Move Function`
- `Extract Module`
- `Change Bidirectional Association to Unidirectional`
