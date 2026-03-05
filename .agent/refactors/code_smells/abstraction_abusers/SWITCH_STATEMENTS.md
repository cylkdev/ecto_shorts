# Switch Statements

## Category

Abstraction Abusers

## Description

Complex conditional logic based on type checking or value discrimination that is scattered across multiple functions or modules. In Elixir, this manifests as repeated `case`, `cond`, or pattern matching on the same discriminator across the codebase, rather than using polymorphism via behaviors or protocols.

## Signs and Symptoms

- The same `case` or `cond` expression appears in multiple functions.
- Functions check the type of a value using guards like `is_map/1`, `is_list/1`, or struct matching to decide behavior.
- Adding a new variant requires modifying multiple functions.
- Large `case` statements with many clauses handling different "types" of the same concept.
- Atoms or strings are used as type discriminators (e.g., `type: :admin`, `type: :guest`).

## Causes

- Procedural thinking carried over from other languages.
- Unfamiliarity with Elixir protocols and behaviors.
- Starting simple and not refactoring as variants grow.
- Fear of "over-engineering" with protocols.

## Example

```elixir
defmodule NotificationSender do
  def send(notification) do
    case notification.type do
      :email ->
        subject = format_email_subject(notification)
        body = format_email_body(notification)
        Mailer.deliver(notification.recipient, subject, body)

      :sms ->
        message = format_sms_message(notification)
        SMSGateway.send(notification.phone, message)

      :push ->
        payload = format_push_payload(notification)
        PushService.deliver(notification.device_token, payload)
    end
  end

  def format_preview(notification) do
    case notification.type do
      :email -> "Email to #{notification.recipient}: #{notification.subject}"
      :sms -> "SMS to #{notification.phone}: #{String.slice(notification.message, 0, 20)}..."
      :push -> "Push: #{notification.title}"
    end
  end

  # More functions with the same case statement pattern...
end
```

## Refactored with Protocol

```elixir
defprotocol Notification do
  @spec send(t()) :: :ok | {:error, term()}
  def send(notification)

  @spec preview(t()) :: String.t()
  def preview(notification)
end

defmodule EmailNotification do
  @enforce_keys [:recipient, :subject, :body]
  defstruct [:recipient, :subject, :body]
end

defimpl Notification, for: EmailNotification do
  def send(%EmailNotification{} = email) do
    Mailer.deliver(email.recipient, email.subject, email.body)
  end

  def preview(%EmailNotification{recipient: recipient, subject: subject}) do
    "Email to #{recipient}: #{subject}"
  end
end

defmodule SMSNotification do
  @enforce_keys [:phone, :message]
  defstruct [:phone, :message]
end

defimpl Notification, for: SMSNotification do
  def send(%SMSNotification{phone: phone, message: message}) do
    SMSGateway.send(phone, message)
  end

  def preview(%SMSNotification{phone: phone, message: message}) do
    "SMS to #{phone}: #{String.slice(message, 0, 20)}..."
  end
end

defmodule PushNotification do
  @enforce_keys [:device_token, :title, :payload]
  defstruct [:device_token, :title, :payload]
end

defimpl Notification, for: PushNotification do
  def send(%PushNotification{device_token: token, payload: payload}) do
    PushService.deliver(token, payload)
  end

  def preview(%PushNotification{title: title}) do
    "Push: #{title}"
  end
end

# Usage - no case statements needed
defmodule NotificationSender do
  def send(notification), do: Notification.send(notification)
  def format_preview(notification), do: Notification.preview(notification)
end
```

## Alternative: Behavior-based Approach

```elixir
defmodule NotificationStrategy do
  @callback send(map()) :: :ok | {:error, term()}
  @callback preview(map()) :: String.t()
end

defmodule EmailStrategy do
  @behaviour NotificationStrategy

  @impl true
  def send(notification) do
    Mailer.deliver(notification.recipient, notification.subject, notification.body)
  end

  @impl true
  def preview(notification) do
    "Email to #{notification.recipient}: #{notification.subject}"
  end
end

# Dispatch via configuration or factory
defmodule NotificationSender do
  @strategies %{
    email: EmailStrategy,
    sms: SMSStrategy,
    push: PushStrategy
  }

  def send(%{type: type} = notification) do
    @strategies[type].send(notification)
  end
end
```

## Treatment

- **Replace Conditional with Protocol**: Define a protocol and implement it for each variant type.
- **Replace Conditional with Behavior**: Use behaviors when variants are modules, not data.
- **Extract Function**: If the conditional is simple, extract each branch into a named function clause.
- **Use Pattern Matching Clauses**: Replace `case` with multiple function clauses when appropriate.

## Why Refactor

- Adding new variants requires only adding a new implementation, not modifying existing code.
- Each variant's logic is co-located in one place.
- Protocols provide compile-time guarantees about required functions.
- Code is more extensible and follows the Open/Closed Principle.

## When to Keep Switch Statements

- The variants are stable and unlikely to change.
- The conditional logic is simple and appears in only one place.
- The discriminator is a simple enum with 2-3 cases.
- Using protocols would add complexity without clear benefit.

## Related Smells

- `Duplicate Code`
- `Long Function`
- `Parallel Module Hierarchies`

## Related Refactoring Techniques

- `Replace Conditional with Protocol`
- `Replace Conditional with Behavior`
- `Extract Function`
- `Introduce Null Object`
