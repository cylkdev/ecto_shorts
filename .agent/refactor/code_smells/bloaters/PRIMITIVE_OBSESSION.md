# Primitive Obsession

## Category

Bloaters

## Description

Using primitive types (strings, integers, atoms, plain maps) to represent domain concepts instead of creating dedicated structs or types. This leads to scattered validation logic, unclear function signatures, and easy-to-miss bugs.

## Signs and Symptoms

- Functions accept generic types like `String.t()` or `map()` when a specific domain type would be clearer.
- The same validation logic is repeated across multiple functions.
- Maps with string or atom keys are passed around without a defined structure.
- Function names include the type they operate on (e.g., `validate_email_string/1`).
- Constants are used to represent domain values that could be typed.
- Type confusion errors occur at runtime (e.g., passing cents when dollars expected).

## Causes

- Avoiding the "overhead" of creating structs.
- Starting simple and never refactoring as complexity grows.
- Unfamiliarity with Elixir's struct and type system.
- Over-reliance on dynamic typing.

## Example

```elixir
defmodule Payments do
  def charge(amount, currency, card_number, expiry_month, expiry_year, cvv) do
    if amount <= 0 do
      {:error, :invalid_amount}
    else
      if String.length(card_number) != 16 do
        {:error, :invalid_card}
      else
        # Process payment...
        {:ok, %{amount: amount, currency: currency}}
      end
    end
  end

  def refund(amount, currency, transaction_id) do
    if amount <= 0 do
      {:error, :invalid_amount}
    else
      # Process refund...
      {:ok, %{amount: amount, currency: currency}}
    end
  end
end
```

## Refactored

```elixir
defmodule Payments.Money do
  @enforce_keys [:amount, :currency]
  defstruct [:amount, :currency]

  @type t :: %__MODULE__{
    amount: pos_integer(),
    currency: :usd | :eur | :gbp
  }

  def new(amount, currency) when amount > 0 and currency in [:usd, :eur, :gbp] do
    {:ok, %__MODULE__{amount: amount, currency: currency}}
  end

  def new(_amount, _currency), do: {:error, :invalid_money}
end

defmodule Payments.Card do
  @enforce_keys [:number, :expiry_month, :expiry_year, :cvv]
  defstruct [:number, :expiry_month, :expiry_year, :cvv]

  @type t :: %__MODULE__{
    number: String.t(),
    expiry_month: 1..12,
    expiry_year: pos_integer(),
    cvv: String.t()
  }

  def new(number, expiry_month, expiry_year, cvv) do
    with :ok <- validate_number(number),
         :ok <- validate_expiry(expiry_month, expiry_year),
         :ok <- validate_cvv(cvv) do
      {:ok, %__MODULE__{
        number: number,
        expiry_month: expiry_month,
        expiry_year: expiry_year,
        cvv: cvv
      }}
    end
  end

  defp validate_number(number) when byte_size(number) == 16, do: :ok
  defp validate_number(_), do: {:error, :invalid_card_number}

  defp validate_expiry(month, year) when month in 1..12 and year > 2020, do: :ok
  defp validate_expiry(_, _), do: {:error, :invalid_expiry}

  defp validate_cvv(cvv) when byte_size(cvv) in [3, 4], do: :ok
  defp validate_cvv(_), do: {:error, :invalid_cvv}
end

defmodule Payments do
  alias Payments.{Money, Card}

  @spec charge(Money.t(), Card.t()) :: {:ok, map()} | {:error, atom()}
  def charge(%Money{} = money, %Card{} = card) do
    # Process payment with validated types
    {:ok, %{amount: money.amount, currency: money.currency}}
  end

  @spec refund(Money.t(), String.t()) :: {:ok, map()} | {:error, atom()}
  def refund(%Money{} = money, transaction_id) do
    # Process refund with validated money
    {:ok, %{amount: money.amount, currency: money.currency}}
  end
end
```

## Treatment

- **Replace Primitive with Struct**: Create a struct to represent the domain concept.
- **Introduce Type Alias**: Use `@type` to document expected shapes.
- **Extract Validation**: Move validation into the struct's constructor.
- **Use `@enforce_keys`**: Ensure required fields are always present.

## Why Refactor

- Structs make function signatures self-documenting.
- Validation happens once at construction, not scattered throughout.
- Pattern matching on structs catches type errors at compile time.
- Dialyzer can verify type correctness.
- Domain concepts become explicit in the codebase.

## Common Primitives to Replace

| Primitive | Domain Concept | Solution |
|-----------|----------------|----------|
| `integer()` | Money amount | `Money.t()` struct with currency |
| `String.t()` | Email address | `Email.t()` struct with validation |
| `String.t()` | Phone number | `Phone.t()` struct with formatting |
| `map()` | User data | `User.t()` struct |
| `integer()` | Timestamp | `DateTime.t()` |
| `String.t()` | UUID | Consider a wrapper or at least `@type` |

## Related Smells

- `Data Clumps`
- `Long Parameter List`
- `Duplicate Code`

## Related Refactoring Techniques

- `Replace Data Value with Struct`
- `Introduce Parameter Object`
- `Extract Module`
