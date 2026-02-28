## Naming Convention

- Write all absinthe code in snake case.

- Write all field and object names in snake case, in the format `<resource>_<verb>`. The resource comes first, and the verb comes second. The resource should usually be singular for mutations because a mutation represents one logical action, even when the input includes multiple IDs. Good examples are: `user_create`, `user_update`, `user_delete`, and `project_member_assign`.


## Schema Module Structure

An absinthe schema module should follow this structure so there is a standard way to navigate and understand the schema.

- The middleware `GQLErrorMessage.Absinthe.Middleware` should be added as a post resolution middleware.

- When importing types:

If you are importing single type, use the following syntax:

```elixir
import_types Types.Conversation
```

If you are importing multiple types, use the following syntax:

```elixir
import_types Types.{
  Conversation,
  Message,
}
```

- When importing mutations, queries, or subscriptions:

If you are importing single type, use the following syntax:

```elixir
import_types Mutations.Conversation
import_types Queries.Conversation
import_types Subscriptions.Conversation
```

If you are importing multiple types, use the following syntax:

```elixir
import_types Mutations.{
  Conversation,
  Message
}

import_types Queries.{
  Conversation,
  Message
}

import_types Subscriptions.{
  Conversation,
  Message
}
```

### Good Example of Schema Module

```elixir
defmodule Conversation.Schema do
  @moduledoc false
  use AppName, :absinthe_schema

  alias AppName.Schema.{Mutations, Queries, Subscriptions, Types}

  # Required: Import types first.
  # Add import types in alphabetical order of the module name.
  import_types Types.{
    Conversation,
    Message
  }

  import_types Mutations.{
    Conversation,
    Message
  }

  import_types Queries.{
    Conversation,
    Message
  }

  import_types Subscriptions.{
    Conversation,
    Message
  }

  mutation do
    # Add imports in alphabetical order of the module name.
    import_fields :conversation_mutations
    import_fields :message_mutations
  end

  query do
    # Add imports in alphabetical order of the module name.

    # You must import the conversation types module in the schema for `import_fields`
    # to work. For example, add `import_types AppName.Types.Conversation` to make the
    # `conversation_queries` fields available in the schema module.
    import_fields :conversation_queries
    import_fields :message_queries
  end

  subscription do
    # Add imports in alphabetical order of the module name.
    import_fields :conversation_subscriptions
    import_fields :message_subscriptions
  end

  def middleware(middleware, _field, %{identifier: identifier})
      when identifier in [:query, :mutation, :subscription] do
    pre_resolution_middleware() ++ middleware ++ post_resolution_middleware()
  end

  def middleware(middleware, _, _) do
    middleware
  end

  defp pre_resolution_middleware do
    [
      # Include any middleware in the project that should run before the execution
      # of every root level field (query, mutation, subscription). These are typically
      # middleware that checks authentication, authorization, rate limiting, etc.
      #
      # Add middleware in alphabetical order of the module name.
    ]
  end

  defp post_resolution_middleware do
    [
      # Add middleware in alphabetical order of the module name.
      GQLErrorMessage.Absinthe.Middleware
    ]
  end
end
```

## How to name Mutation Fields

Here is how you define these mutation fields in a GraphQL schema:

```graphql
type Mutation {
  userCreate(input: UserCreateInput!): UserCreatePayload!
  userUpdate(input: UserUpdateInput!): UserUpdatePayload!
  userDelete(input: UserDeleteInput!): UserDeletePayload!
}
```

Write it in Absinthe DSL like this:

```elixir
object :user_mutations do
  field :user_create, :user_create_payload do
    arg :input, non_null(:user_create_input)
    resolve &UserResolver.create/3
  end

  field :user_update, :user_update_payload do
    arg :input, non_null(:user_update_input)
    resolve &UserResolver.update/3
  end

  field :user_delete, :user_delete_payload do
    arg :input, non_null(:user_delete_input)
    resolve &UserResolver.delete/3
  end
end
```

## How to name domain-specific mutation fields

This convention is not only for create, update, and delete operations. It is also useful for domain-specific actions.

You should keep the same pattern and use a clear verb that describes the business action. For example, `invoice_archive`, `invoice_send`, `subscription_cancel`, and `expense_report_approve` all remain easy to scan and understand.

Here is how you define these mutation fields in a GraphQL schema:

```graphql
type Mutation {
  invoiceArchive(input: InvoiceArchiveInput!): InvoiceArchivePayload!
  invoiceSend(input: InvoiceSendInput!): InvoiceSendPayload!
  subscriptionCancel(input: SubscriptionCancelInput!): SubscriptionCancelPayload!
  expenseReportApprove(input: ExpenseReportApproveInput!): ExpenseReportApprovePayload!
}
```

Write it in Absinthe DSL like this:

```elixir
object :invoice_mutations do
  field :invoice_archive, :invoice_archive_payload do
    arg :input, non_null(:invoice_archive_input)

    resolve &InvoiceResolver.archive/3
  end

  field :invoice_send, :invoice_send_payload do
    arg :input, non_null(:invoice_send_input)

    resolve &InvoiceResolver.send/3
  end
end

object :subscription_mutations do
  field :subscription_cancel, :subscription_cancel_payload do
    arg :input, non_null(:subscription_cancel_input)

    resolve &SubscriptionResolver.cancel/3
  end
end

object :expense_report_mutations do
  field :expense_report_approve, :expense_report_approve_payload do
    arg :input, non_null(:expense_report_approve_input)

    resolve &ExpenseReportResolver.approve/3
  end
end
```

## How to name Query Fields in Absinthe

Queries usually read better as nouns. For that reason, query fields should typically use the resource name only, not a verb.

Use names like `user`, `users`, `invoice`, and `invoices`. This keeps queries natural and keeps the resource-first action rule focused on mutations, where actions are the main concern.

Here is how you define these query fields in a GraphQL schema:

```graphql
type Query {
  user(id: ID!): User
  users(filter: UserFilterInput): UserConnection!
  invoice(id: ID!): Invoice
  invoices(filter: InvoiceFilterInput): InvoiceConnection!
}
```

Write it in Absinthe DSL like this:

```elixir
object :user_queries do
  field :user, :user do
    arg :id, non_null(:id)

    resolve &UserResolver.find/3
  end

  field :users, non_null(:user_connection) do
    arg :filter, :user_filter_input

    resolve &UserResolver.list/3
  end
end

object :invoice_queries do
  field :invoice, :invoice do
    arg :id, non_null(:id)

    resolve &InvoiceResolver.find/3
  end

  field :invoices, non_null(:invoice_connection) do
    arg :filter, :invoice_filter_input

    resolve &InvoiceResolver.list/3
  end
end
```

## Keep Verbs Consistent

Choose one verb for each meaning and use it consistently across the schema. If your API uses `Delete`, do not also use `Remove` for the same behavior unless they are actually different operations. This matters more over time. In a growing Absinthe API, inconsistent verbs make the schema harder to learn and harder to search. For example, if one resource uses `user_delete` and another uses `invoice_remove`, developers have to remember exceptions. If both mean the same kind of destructive action, they should use the same verb.

## Keep Input and Payload Names Aligned

The mutation field name should map directly to the input and payload type names.

If the field is `project_member_assign`, the matching types should be `project_member_assign_input` and `project_member_assign_payload`.

In Absinthe DSL, that usually means:

- GraphQL field: `project_member_assign`
- Absinthe field macro name: `:project_member_assign`
- Absinthe input object identifier: `:project_member_assign_input`
- Absinthe payload object identifier: `:project_member_assign_payload`
- GraphQL types exposed to clients: `ProjectMemberAssignInput`, `ProjectMemberAssignPayload`

For example:

Here is how you define these mutation fields in a GraphQL schema:

```graphql
type Mutation {
  projectMemberAssign(input: ProjectMemberAssignInput!):  ProjectMemberAssignPayload!
}
```

Write it in Absinthe DSL like this:

```elixir
field :project_member_assign, :project_member_assign_payload do
  arg :input, non_null(:project_member_assign_input)

  resolve &ProjectMemberResolver.assign/3
end
```
