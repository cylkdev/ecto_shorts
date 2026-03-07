# Long Parameter List

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Category

Bloaters

## Description

A function accepts too many parameters, making it difficult to call correctly and understand at a glance. In Elixir, more than 3-4 parameters often indicates the need for a struct, keyword list, or refactoring.

## Signs and Symptoms

- Functions with more than 4 positional parameters.
- Callers frequently pass `nil` or default values for unused parameters.
- Parameter order is easy to confuse (e.g., two strings in a row).
- Adding a new parameter requires updating many call sites.
- Function documentation is needed just to understand parameter order.
- Parameters are logically grouped but passed separately.

## Causes

- Incremental feature additions without refactoring.
- Avoiding the creation of intermediate data structures.
- Copying patterns from other languages where long parameter lists are common.
- Functions that do too much and need many inputs.

## Example

```elixir
defmodule Notifications do
  def send_email(to, from, subject, body, cc, bcc, reply_to, priority, attachments, template_id) do
    # Build and send email...
  end

  def send_sms(to, from, body, priority, scheduled_at, callback_url, max_retries) do
    # Build and send SMS...
  end
end

# Calling code - easy to make mistakes
Notifications.send_email(
  "user@example.com",
  "noreply@app.com",
  "Welcome!",
  "Hello there...",
  nil,
  nil,
  "support@app.com",
  :high,
  [],
  "welcome_template"
)
```

## Refactored

```elixir
defmodule Notifications.Email do
  @enforce_keys [:to, :subject, :body]
  defstruct [
    :to,
    :from,
    :subject,
    :body,
    :cc,
    :bcc,
    :reply_to,
    :template_id,
    priority: :normal,
    attachments: []
  ]

  @type t :: %__MODULE__{
    to: String.t(),
    from: String.t() | nil,
    subject: String.t(),
    body: String.t(),
    cc: [String.t()] | nil,
    bcc: [String.t()] | nil,
    reply_to: String.t() | nil,
    priority: :low | :normal | :high,
    attachments: [map()],
    template_id: String.t() | nil
  }
end

defmodule Notifications.SMS do
  @enforce_keys [:to, :body]
  defstruct [
    :to,
    :from,
    :body,
    :scheduled_at,
    :callback_url,
    priority: :normal,
    max_retries: 3
  ]
end

defmodule Notifications do
  alias Notifications.{Email, SMS}

  @spec send_email(Email.t()) :: {:ok, map()} | {:error, term()}
  def send_email(%Email{} = email) do
    # Build and send email using email struct fields
    {:ok, %{delivered: true}}
  end

  @spec send_sms(SMS.t()) :: {:ok, map()} | {:error, term()}
  def send_sms(%SMS{} = sms) do
    # Build and send SMS using sms struct fields
    {:ok, %{delivered: true}}
  end
end

# Calling code - clear and self-documenting
Notifications.send_email(%Notifications.Email{
  to: "user@example.com",
  from: "noreply@app.com",
  subject: "Welcome!",
  body: "Hello there...",
  reply_to: "support@app.com",
  priority: :high,
  template_id: "welcome_template"
})
```

## Alternative: Keyword Options

For optional configuration, keyword lists work well:

```elixir
defmodule Notifications do
  @default_opts [priority: :normal, max_retries: 3]

  def send_sms(to, body, opts \\ []) do
    opts = Keyword.merge(@default_opts, opts)
    # Use opts[:priority], opts[:max_retries], etc.
  end
end

# Clear which options are being set
Notifications.send_sms("555-1234", "Hello!", priority: :high, callback_url: "https://...")
```

## Treatment

- **Introduce Parameter Struct**: Group related parameters into a struct.
- Use keyword options for optional parameters with defaults.
- **Preserve Whole Struct**: Pass the entire struct instead of extracting fields.
- **Replace Parameter with Function Call**: If a parameter can be derived, compute it inside the function.

## Why Refactor

- Structs make parameters self-documenting with named fields.
- Default values are defined in one place (the struct definition).
- Adding new optional fields doesn't break existing callers.
- Pattern matching validates the parameter shape.
- Dialyzer can verify the struct type at compile time.

## Guidelines

| Parameter Count | Recommendation |
|-----------------|----------------|
| 1-3 | Positional parameters are fine |
| 4-5 | Consider keyword options for optional params |
| 6+ | Strongly consider a struct |

## Related Smells

- `Primitive Obsession`
- `Data Clumps`
- `Long Function`

## Related Refactoring Techniques

- `Introduce Parameter Struct`
- `Preserve Whole Struct`
- `Replace Parameter with Function Call`
