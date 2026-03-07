# Feature Envy

## Category

Couplers

## Description

A function that uses data or functions from another module more than from its own module. The function "envies" the features of another module and would be better placed there.

## Signs and Symptoms

- A function calls many functions from another module.
- A function accesses many fields from a struct defined in another module.
- The function's logic is more about the other module's data than its own.
- Changes to the other module frequently require changes to this function.
- The function name includes the other module's concept (e.g., `format_user/1` in `OrderModule`).

## Causes

- Incremental development that added logic in the wrong place.
- Misunderstanding of module boundaries.
- Convenience of adding code where you're currently working.
- Lack of clear domain modeling.

## Example

```elixir
defmodule MyApp.OrderProcessor do
  alias MyApp.Customer

  def calculate_discount(order) do
    customer = order.customer

    # This function envies Customer - it knows too much about customer internals
    base_discount = if customer.membership_level == :gold do
      0.15
    else
      if customer.membership_level == :silver do
        0.10
      else
        0.05
      end
    end

    loyalty_bonus = if customer.years_as_member > 5 do
      0.02
    else
      0
    end

    birthday_bonus = if customer.birth_date &&
                        customer.birth_date.month == Date.utc_today().month do
      0.05
    else
      0
    end

    total_discount = base_discount + loyalty_bonus + birthday_bonus
    order.total * total_discount
  end

  def format_customer_summary(order) do
    customer = order.customer

    # More feature envy - formatting customer data in OrderProcessor
    """
    Customer: #{customer.first_name} #{customer.last_name}
    Email: #{customer.email}
    Member since: #{customer.joined_at}
    Level: #{customer.membership_level}
    """
  end
end
```

## Refactored

```elixir
defmodule MyApp.Customer do
  defstruct [:id, :first_name, :last_name, :email, :membership_level,
             :years_as_member, :birth_date, :joined_at]

  @discount_rates %{gold: 0.15, silver: 0.10, bronze: 0.05}

  def discount_rate(%__MODULE__{membership_level: level}) do
    Map.get(@discount_rates, level, 0.05)
  end

  def loyalty_bonus(%__MODULE__{years_as_member: years}) when years > 5, do: 0.02
  def loyalty_bonus(%__MODULE__{}), do: 0

  def birthday_bonus(%__MODULE__{birth_date: nil}), do: 0
  def birthday_bonus(%__MODULE__{birth_date: birth_date}) do
    if birth_date.month == Date.utc_today().month, do: 0.05, else: 0
  end

  def total_discount_rate(%__MODULE__{} = customer) do
    discount_rate(customer) + loyalty_bonus(customer) + birthday_bonus(customer)
  end

  def full_name(%__MODULE__{first_name: first, last_name: last}) do
    "#{first} #{last}"
  end

  def summary(%__MODULE__{} = customer) do
    """
    Customer: #{full_name(customer)}
    Email: #{customer.email}
    Member since: #{customer.joined_at}
    Level: #{customer.membership_level}
    """
  end
end

defmodule MyApp.OrderProcessor do
  alias MyApp.Customer

  def calculate_discount(order) do
    # Now delegates to Customer module
    discount_rate = Customer.total_discount_rate(order.customer)
    order.total * discount_rate
  end

  def format_customer_summary(order) do
    Customer.summary(order.customer)
  end
end
```

## Treatment

- **Move Function**: Move the function to the module whose data it uses most.
- **Extract Function**: If only part of the function envies another module, extract that part.
- **Introduce Delegate**: If the function must stay, delegate to the appropriate module.

## Why Refactor

- Functions live with the data they operate on.
- Changes to data structure only affect one module.
- Better encapsulation and information hiding.
- Clearer module responsibilities.
- Easier to test in isolation.

## Identifying Feature Envy

Count the references in a function:
- How many times does it access fields from its own module's structs?
- How many times does it access fields from other modules' structs?
- How many functions does it call from other modules?

If external references dominate, the function likely has feature envy.

## When Feature Envy Is Acceptable

- **Utility functions**: Functions that intentionally operate on external data.
- **Adapters/Transformers**: Functions that convert between modules.
- **Orchestration**: High-level functions that coordinate multiple modules.

```elixir
# Acceptable: this is an orchestration function
defmodule MyApp.CheckoutService do
  def process(order) do
    with {:ok, validated} <- OrderValidator.validate(order),
         {:ok, charged} <- PaymentProcessor.charge(validated),
         {:ok, shipped} <- ShippingService.ship(charged) do
      {:ok, shipped}
    end
  end
end
```

## Related Smells

- `Data Module`
- `Inappropriate Intimacy`
- `Message Chains`

## Related Refactoring Techniques

- `Move Function`
- `Extract Function`
- `Hide Delegate`
