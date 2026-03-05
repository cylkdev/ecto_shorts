# Alternative Modules with Different Interfaces

## Category

Abstraction Abusers

## Description

Two or more modules perform similar jobs but have different function names, arities, or return value formats. This makes them non-interchangeable and forces callers to handle each variant specially.

## Signs and Symptoms

- Modules that do the same conceptual thing but have different function names.
- Similar modules with different return value formats (e.g., one returns `{:ok, value}`, another returns `value` or `nil`).
- Conditional logic in callers to handle different module interfaces.
- Difficulty swapping implementations without changing calling code.
- Documentation that compares "Module A does X this way, Module B does X that way."

## Causes

- Different developers implementing similar functionality independently.
- Wrapping third-party libraries without normalizing interfaces.
- Evolving requirements leading to inconsistent API design.
- Lack of defined contracts (behaviors) for interchangeable modules.

## Example

```elixir
defmodule Payments.Stripe do
  def process_charge(amount, currency, card_token) do
    # Returns {:ok, charge_id} or {:error, message}
    case StripeAPI.create_charge(amount, currency, card_token) do
      %{"id" => id} -> {:ok, id}
      %{"error" => %{"message" => msg}} -> {:error, msg}
    end
  end

  def get_charge_status(charge_id) do
    StripeAPI.retrieve_charge(charge_id)["status"]
  end
end

defmodule Payments.PayPal do
  def charge(params) do
    # Different function name, different params format
    # Returns %{success: true, transaction_id: id} or %{success: false, reason: reason}
    case PayPalAPI.execute_payment(params.amount, params.currency, params.payer_id) do
      {:ok, response} -> %{success: true, transaction_id: response.id}
      {:error, reason} -> %{success: false, reason: reason}
    end
  end

  def status(transaction_id) do
    # Different function name
    PayPalAPI.get_payment(transaction_id).state
  end
end

# Caller must handle both interfaces
defmodule Checkout do
  def process(order, :stripe) do
    case Payments.Stripe.process_charge(order.total, order.currency, order.card_token) do
      {:ok, charge_id} -> {:ok, %{provider: :stripe, id: charge_id}}
      {:error, msg} -> {:error, msg}
    end
  end

  def process(order, :paypal) do
    result = Payments.PayPal.charge(%{
      amount: order.total,
      currency: order.currency,
      payer_id: order.payer_id
    })

    if result.success do
      {:ok, %{provider: :paypal, id: result.transaction_id}}
    else
      {:error, result.reason}
    end
  end
end
```

## Refactored

```elixir
defmodule Payments.Gateway do
  @type charge_result :: {:ok, String.t()} | {:error, String.t()}
  @type status :: :pending | :completed | :failed | :refunded

  @callback charge(amount :: integer(), currency :: String.t(), token :: String.t()) :: charge_result()
  @callback get_status(transaction_id :: String.t()) :: status()
end

defmodule Payments.Stripe do
  @behaviour Payments.Gateway

  @impl true
  def charge(amount, currency, token) do
    case StripeAPI.create_charge(amount, currency, token) do
      %{"id" => id} -> {:ok, id}
      %{"error" => %{"message" => msg}} -> {:error, msg}
    end
  end

  @impl true
  def get_status(charge_id) do
    case StripeAPI.retrieve_charge(charge_id)["status"] do
      "succeeded" -> :completed
      "pending" -> :pending
      "failed" -> :failed
      _ -> :pending
    end
  end
end

defmodule Payments.PayPal do
  @behaviour Payments.Gateway

  @impl true
  def charge(amount, currency, token) do
    case PayPalAPI.execute_payment(amount, currency, token) do
      {:ok, response} -> {:ok, response.id}
      {:error, reason} -> {:error, to_string(reason)}
    end
  end

  @impl true
  def get_status(transaction_id) do
    case PayPalAPI.get_payment(transaction_id).state do
      "approved" -> :completed
      "created" -> :pending
      "failed" -> :failed
      _ -> :pending
    end
  end
end

# Caller uses unified interface
defmodule Checkout do
  @gateways %{
    stripe: Payments.Stripe,
    paypal: Payments.PayPal
  }

  def process(order, provider) do
    gateway = @gateways[provider]

    case gateway.charge(order.total, order.currency, order.payment_token) do
      {:ok, transaction_id} ->
        {:ok, %{provider: provider, id: transaction_id}}
      {:error, reason} ->
        {:error, reason}
    end
  end
end
```

## Treatment

- **Extract Behavior**: Define a behavior that all implementations must follow.
- **Rename Function**: Align function names across modules.
- **Normalize Return Values**: Ensure all implementations return the same format.
- **Add Adapter Layer**: Wrap inconsistent third-party APIs with a consistent interface.

## Why Refactor

- Modules become truly interchangeable.
- Callers don't need conditional logic for different implementations.
- Adding new implementations requires only implementing the behavior.
- Testing is simplified with consistent interfaces.
- Dialyzer can verify implementations satisfy the contract.

## Checklist for Consistent Interfaces

- [ ] Same function names
- [ ] Same arities (or consistent use of options)
- [ ] Same return value formats
- [ ] Same error handling patterns
- [ ] Documented behavior with `@callback`
- [ ] Type specs that match across implementations

## Related Smells

- `Switch Statements`
- `Duplicate Code`
- `Shotgun Surgery`

## Related Refactoring Techniques

- `Extract Behavior`
- `Rename Function`
- `Move Function`
- `Introduce Adapter`
