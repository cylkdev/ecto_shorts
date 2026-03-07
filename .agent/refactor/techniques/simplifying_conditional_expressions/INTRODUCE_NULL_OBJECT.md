# Introduce Null Object

## When to use

Use when any of the following are true:

- `nil` checks are repeated before calling behaviour.
- A missing collaborator should behave as a no-op/default actor.
- Call sites become noisy with defensive conditionals.

## Problem

Many conditionals guard against `nil` collaborators.

```elixir
if notifier do
  notifier.send_receipt(order)
end
```

## Solution

Provide a module implementing the same contract with neutral behaviour.

```elixir
defmodule Notifier do
  @callback send_receipt(map()) :: :ok
end

defmodule NullNotifier do
  @behaviour Notifier
  def send_receipt(_order), do: :ok
end
```

Callers always call a notifier module; default is `NullNotifier`.

## Why Refactor

- Removes repeated `nil` conditionals.
- Keeps call sites linear.
- Encodes absence behaviour explicitly.

## How to Refactor

1. Identify repeated `nil` checks for one collaborator.
2. Define/confirm behaviour contract.
3. Add null module with neutral implementation.
4. Inject/select null module where collaborator is absent.
5. Remove `nil` guard conditionals.
6. Run formatter and tests.

## Validation

- `nil` checks are reduced at call sites.
- Missing-collaborator behaviour is explicit and tested.
- Behaviour is unchanged for both real and null collaborator paths.

## Eliminates Code Smell

- `Repeated Conditional`
- `Shotgun Surgery`
