# Speculative Generality

## Category

Dispensables

## Description

Code that was written to handle future requirements that never materialized. This includes unused parameters, overly abstract interfaces, hooks for extensibility that are never extended, and configuration options that are never configured.

## Signs and Symptoms

- Functions with parameters that are always passed the same value.
- Abstract behaviours with only one implementation.
- Configuration options that are never changed from defaults.
- "Plugin" architectures with no plugins.
- Generic names like `process/1` when only one type is ever processed.
- Comments like "for future use" or "in case we need to..."
- Unused callback implementations in behaviours.

## Causes

- Anticipating requirements that never come.
- Over-engineering "just in case."
- Applying design patterns without concrete need.
- Fear of future refactoring.
- Misinterpreting YAGNI (You Aren't Gonna Need It).

## Example

```elixir
# Abstract behaviour with only one implementation
defmodule MyApp.StorageStrategy do
  @callback store(data :: term(), opts :: keyword()) :: {:ok, term()} | {:error, term()}
  @callback retrieve(id :: term(), opts :: keyword()) :: {:ok, term()} | {:error, term()}
  @callback delete(id :: term(), opts :: keyword()) :: :ok | {:error, term()}
end

defmodule MyApp.S3Storage do
  @behaviour MyApp.StorageStrategy

  @impl true
  def store(data, opts) do
    # Only implementation, opts are never used differently
    bucket = Keyword.get(opts, :bucket, "default-bucket")
    # ...
  end

  # ... other callbacks
end

defmodule MyApp.StorageService do
  # Strategy is always S3Storage, never changes
  def store(data, strategy \\ MyApp.S3Storage, opts \\ []) do
    strategy.store(data, opts)
  end
end

# Overly generic processor
defmodule MyApp.DataProcessor do
  # type is always :user in practice
  def process(data, type, opts \\ []) do
    preprocessor = Keyword.get(opts, :preprocessor, &default_preprocess/1)
    postprocessor = Keyword.get(opts, :postprocessor, &default_postprocess/1)
    validator = Keyword.get(opts, :validator, &default_validate/1)

    data
    |> preprocessor.()
    |> do_process(type)
    |> validator.()
    |> postprocessor.()
  end

  # These hooks are never customized
  defp default_preprocess(data), do: data
  defp default_postprocess(data), do: data
  defp default_validate(data), do: {:ok, data}

  defp do_process(data, :user), do: process_user(data)
  defp do_process(data, :order), do: process_order(data)  # Never called
  defp do_process(data, :product), do: process_product(data)  # Never called
end
```

## Refactored

```elixir
# Direct implementation without unnecessary abstraction
defmodule MyApp.S3Storage do
  @bucket "default-bucket"

  def store(data) do
    # Direct implementation
    ExAws.S3.put_object(@bucket, generate_key(), data)
    |> ExAws.request()
  end

  def retrieve(id) do
    ExAws.S3.get_object(@bucket, id)
    |> ExAws.request()
  end

  def delete(id) do
    ExAws.S3.delete_object(@bucket, id)
    |> ExAws.request()
  end

  defp generate_key, do: Ecto.UUID.generate()
end

# Simplified processor for the actual use case
defmodule MyApp.UserProcessor do
  def process(user_data) do
    user_data
    |> validate()
    |> transform()
  end

  defp validate(data) do
    # Actual validation logic
    {:ok, data}
  end

  defp transform(data) do
    # Actual transformation logic
    data
  end
end
```

## Treatment

- **Collapse Module Hierarchy**: Remove abstract layers with single implementations.
- **Inline Module**: Merge the abstraction into its only consumer.
- **Remove Parameter**: Delete unused or always-same-value parameters.
- **Remove Dead Code**: Delete unused branches and callbacks.
- **Rename to Specific**: Change generic names to reflect actual usage.

## Why Refactor

- Simpler code is easier to understand and maintain.
- Removes indirection that adds no value.
- Reduces the surface area for bugs.
- Makes actual behaviour clearer.
- Easier onboarding for new developers.

## When Abstraction Is Warranted

Keep abstractions when:

- **Multiple implementations exist today**: Not "might exist someday."
- **Testing requires it**: Mocking/stubbing needs an interface.
- **External contract**: The abstraction is part of a public API.
- **Concrete plans**: A second implementation is actively being developed.

```elixir
# Justified: we actually have multiple implementations
defmodule MyApp.PaymentGateway do
  @callback charge(amount :: integer(), token :: String.t()) :: {:ok, String.t()} | {:error, term()}
end

defmodule MyApp.StripeGateway do
  @behaviour MyApp.PaymentGateway
  # ...
end

defmodule MyApp.PayPalGateway do
  @behaviour MyApp.PaymentGateway
  # ...
end
```

## YAGNI Principle

"You Aren't Gonna Need It" - don't build features until they're actually needed.

- Build the simplest thing that works today.
- Refactor when requirements actually change.
- Trust that future you can handle future problems.
- Elixir makes refactoring relatively safe with pattern matching and immutability.

## Related Smells

- `Dead Code`
- `Lazy Module`
- `Refused Bequest`

## Related Refactoring Techniques

- `Collapse Module Hierarchy`
- `Inline Module`
- `Remove Parameter`
- `Rename Function`
