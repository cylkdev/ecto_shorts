# Data Module

## Category

Dispensables

## Description

A module that contains only data (struct definition and fields) with no behavior. While structs are valuable in Elixir, a module that only defines a struct without any functions operating on that data often indicates that behavior is scattered elsewhere or missing entirely.

## Signs and Symptoms

- A module that only contains `defstruct` with no functions.
- All functions that operate on the struct live in other modules.
- The struct is passed around and manipulated externally.
- No validation, transformation, or query functions in the module.
- The module feels like a "dumb" data container.

## Causes

- Separating data from behavior (anemic domain model).
- Treating Elixir structs like database records without logic.
- Over-application of "single responsibility" to mean "data OR behavior."
- Migrating from languages where data classes are common.

## Example

```elixir
defmodule MyApp.Order do
  defstruct [:id, :items, :customer_id, :status, :total]
end

# All behavior lives elsewhere
defmodule MyApp.OrderService do
  alias MyApp.Order

  def calculate_total(%Order{items: items}) do
    Enum.sum(Enum.map(items, & &1.price * &1.quantity))
  end

  def can_cancel?(%Order{status: status}) do
    status in [:pending, :confirmed]
  end

  def cancel(%Order{} = order) do
    if can_cancel?(order) do
      {:ok, %{order | status: :cancelled}}
    else
      {:error, :cannot_cancel}
    end
  end

  def add_item(%Order{items: items} = order, item) do
    %{order | items: [item | items]}
  end
end

defmodule MyApp.OrderValidator do
  alias MyApp.Order

  def valid?(%Order{items: items, customer_id: cid}) do
    items != [] and cid != nil
  end
end
```

## Refactored

```elixir
defmodule MyApp.Order do
  @enforce_keys [:customer_id]
  defstruct [:id, :customer_id, :status, items: []]

  @type t :: %__MODULE__{
    id: String.t() | nil,
    customer_id: String.t(),
    status: :pending | :confirmed | :shipped | :cancelled,
    items: [map()]
  }

  # Construction with validation
  def new(customer_id, items \\ []) when is_binary(customer_id) do
    %__MODULE__{
      customer_id: customer_id,
      items: items,
      status: :pending
    }
  end

  # Queries about the data
  def total(%__MODULE__{items: items}) do
    Enum.sum(Enum.map(items, & &1.price * &1.quantity))
  end

  def empty?(%__MODULE__{items: []}), do: true
  def empty?(%__MODULE__{}), do: false

  def cancellable?(%__MODULE__{status: status}) do
    status in [:pending, :confirmed]
  end

  # Transformations that return new structs
  def add_item(%__MODULE__{items: items} = order, item) do
    %{order | items: [item | items]}
  end

  def cancel(%__MODULE__{} = order) do
    if cancellable?(order) do
      {:ok, %{order | status: :cancelled}}
    else
      {:error, :cannot_cancel}
    end
  end

  def confirm(%__MODULE__{status: :pending} = order) do
    {:ok, %{order | status: :confirmed}}
  end

  def confirm(%__MODULE__{}), do: {:error, :invalid_status}
end
```

## Treatment

- **Move Function**: Move functions that operate on the struct into the struct's module.
- **Extract Function**: If external modules have struct-specific logic, extract and move it.
- **Encapsulate Field**: Add functions to access and modify fields with validation.

## What Belongs in a Data Module

A well-designed struct module should include:

1. **Struct definition** with `@enforce_keys` and defaults
2. **Type specification** (`@type t :: ...`)
3. **Constructor functions** (`new/1`, `build/1`)
4. **Query functions** (predicates, calculations on the data)
5. **Transformation functions** (functions that return modified structs)
6. **Validation** (if using Ecto, changesets; otherwise, validation functions)

## Why Refactor

- Behavior is co-located with the data it operates on.
- The struct module becomes the authority on its own data.
- Easier to find all operations related to a concept.
- Better encapsulation and information hiding.
- Reduces coupling between modules.

## When Data-Only Modules Are Acceptable

- **DTOs for external APIs**: Structs that map to JSON responses.
- **Ecto schemas**: Behavior lives in context modules by convention.
- **Protocol implementations**: The struct exists to satisfy a protocol.
- **Configuration structs**: Simple value objects for settings.

## Related Smells

- `Feature Envy`
- `Shotgun Surgery`
- `Lazy Module`

## Related Refactoring Techniques

- `Move Function`
- `Extract Function`
- `Encapsulate Field`
