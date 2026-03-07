# Divergent Change

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Category

Change Preventers

## Description

A single module must be modified for many different, unrelated reasons. When a module changes frequently for different types of requirements, it indicates the module has too many responsibilities and should be split.

## Signs and Symptoms

- The same module is modified in most pull requests, but for different reasons.
- Changes to database logic, business rules, and presentation all touch the same module.
- The module has functions that operate on different domains or concerns.
- Developers frequently have merge conflicts in the same module.
- The module's name is generic (e.g., `OrderManager`, `UserService`, `DataProcessor`).

## Causes

- Treating modules as organizational containers rather than cohesive units.
- "God module" anti-pattern where one module does everything.
- Lack of clear separation between layers (data, business logic, presentation).
- Incremental feature additions without refactoring.

## Example

```elixir
defmodule MyApp.OrderManager do
  # Changed when database schema changes
  def get_order(id) do
    Repo.get(Order, id)
    |> Repo.preload([:items, :customer])
  end

  def save_order(order) do
    Repo.insert_or_update(order)
  end

  # Changed when business rules change
  def calculate_total(order) do
    subtotal = Enum.sum(Enum.map(order.items, & &1.price * &1.quantity))
    tax = subtotal * get_tax_rate(order.customer.region)
    shipping = calculate_shipping(order)
    subtotal + tax + shipping
  end

  def apply_discount(order, coupon) do
    # Complex discount logic
  end

  # Changed when validation rules change
  def validate_order(order) do
    # Validation logic
  end

  # Changed when notification requirements change
  def send_confirmation(order) do
    Mailer.deliver_order_confirmation(order)
  end

  def send_shipping_notification(order) do
    Mailer.deliver_shipping_update(order)
  end

  # Changed when reporting requirements change
  def generate_invoice_pdf(order) do
    # PDF generation logic
  end

  def export_to_csv(orders) do
    # CSV export logic
  end
end
```

## Refactored

```elixir
defmodule MyApp.Orders.Repository do
  # Only changes when data access patterns change
  def get(id) do
    Repo.get(Order, id)
    |> Repo.preload([:items, :customer])
  end

  def save(order) do
    Repo.insert_or_update(order)
  end

  def list_by_customer(customer_id) do
    Repo.all(from o in Order, where: o.customer_id == ^customer_id)
  end
end

defmodule MyApp.Orders.Calculator do
  # Only changes when pricing/business rules change
  def total(order) do
    subtotal(order) + tax(order) + shipping(order)
  end

  def subtotal(order) do
    Enum.sum(Enum.map(order.items, & &1.price * &1.quantity))
  end

  def tax(order) do
    subtotal(order) * TaxRates.for_region(order.customer.region)
  end

  def shipping(order) do
    ShippingCalculator.calculate(order)
  end

  def apply_discount(order, coupon) do
    DiscountEngine.apply(order, coupon)
  end
end

defmodule MyApp.Orders.Validator do
  # Only changes when validation rules change
  def validate(order) do
    order
    |> validate_items()
    |> validate_customer()
    |> validate_shipping_address()
  end
end

defmodule MyApp.Orders.Notifications do
  # Only changes when notification requirements change
  def send_confirmation(order) do
    Mailer.deliver_order_confirmation(order)
  end

  def send_shipping_update(order) do
    Mailer.deliver_shipping_update(order)
  end
end

defmodule MyApp.Orders.Exporter do
  # Only changes when export/reporting requirements change
  def to_pdf(order) do
    PDFGenerator.generate_invoice(order)
  end

  def to_csv(orders) do
    CSVBuilder.build(orders, columns: [:id, :total, :status])
  end
end
```

## Treatment

- **Extract Module**: Split the module by responsibility or reason for change.
- **Move Function**: Relocate functions to appropriate domain modules.
- Split data access, business logic, and presentation concerns into distinct modules when they change for different reasons.

## Why Refactor

- Each module has a single reason to change.
- Changes are isolated to smaller, focused modules.
- Reduced merge conflicts.
- Easier to understand what each module does.
- Better testability with focused modules.

## Identifying Divergent Change

Ask these questions about a module:

1. What are the different reasons this module might change?
2. Do changes for reason A affect code for reason B?
3. Could different team members work on different parts simultaneously?

If a module changes for multiple unrelated reasons, it has divergent change.

## Related Smells

- `Large Module`
- `Shotgun Surgery` (opposite problem)
- `Long Function`

## Related Refactoring Techniques

- `Extract Module`
- `Move Function`
- `Extract Behaviour`
