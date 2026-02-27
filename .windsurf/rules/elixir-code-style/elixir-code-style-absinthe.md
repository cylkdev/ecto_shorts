---
trigger: model_decision
description: Code defines or modifies Absinthe schema types, queries, mutations, subscriptions, or resolvers. Uses Absinthe DSL macros like object, field, arg, resolve, or mutation. Module imports or uses Absinthe.Schema or Absinthe.Schema.Notation.
---

## When to use

Use when writing or modifying Absinthe code.

### Naming Convention

- Write all absinthe code in snake case.

- Write all field and object names in snake case, in the format `<resource>_<verb>`. The resource comes first, and the verb comes second. The resource should usually be singular for mutations because a mutation represents one logical action, even when the input includes multiple IDs. Good examples are: `user_create`, `user_update`, `user_delete`, and `project_member_assign`.

### How to name Mutation Fields

Here is how you define these mutation fields in a GraphQL schema:

    type Mutation {
      userCreate(input: UserCreateInput!): UserCreatePayload!
      userUpdate(input: UserUpdateInput!): UserUpdatePayload!
      userDelete(input: UserDeleteInput!): UserDeletePayload!
    }

Write it in Absinthe DSL like this:

    # GOOD EXAMPLE, COPY THIS!
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

### How to name domain-specific mutation fields

This convention is not only for create, update, and delete operations. It is also useful for domain-specific actions.

You should keep the same pattern and use a clear verb that describes the business action. For example, `invoice_archive`, `invoice_send`, `subscription_cancel`, and `expense_report_approve` all remain easy to scan and understand.

Here is how you define these mutation fields in a GraphQL schema:

    type Mutation {
      invoiceArchive(input: InvoiceArchiveInput!): InvoiceArchivePayload!
      invoiceSend(input: InvoiceSendInput!): InvoiceSendPayload!
      subscriptionCancel(input: SubscriptionCancelInput!): SubscriptionCancelPayload!
      expenseReportApprove(input: ExpenseReportApproveInput!): ExpenseReportApprovePayload!
    }

Write it in Absinthe DSL like this:

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

### How to name Query Fields in Absinthe

Queries usually read better as nouns. For that reason, query fields should typically use the resource name only, not a verb.

Use names like `user`, `users`, `invoice`, and `invoices`. This keeps queries natural and keeps the resource-first action rule focused on mutations, where actions are the main concern.

Here is how you define these query fields in a GraphQL schema:

    type Query {
      user(id: ID!): User
      users(filter: UserFilterInput): UserConnection!
      invoice(id: ID!): Invoice
      invoices(filter: InvoiceFilterInput): InvoiceConnection!
    }

Write it in Absinthe DSL like this:

    object :user_queries do
      field :user, :user do
        arg :id, non_null(:id)
        resolve &UserResolver.get/3
      end

      field :users, non_null(:user_connection) do
        arg :filter, :user_filter_input
        resolve &UserResolver.list/3
      end
    end

    object :invoice_queries do
      field :invoice, :invoice do
        arg :id, non_null(:id)
        resolve &InvoiceResolver.get/3
      end

      field :invoices, non_null(:invoice_connection) do
        arg :filter, :invoice_filter_input
        resolve &InvoiceResolver.list/3
      end
    end

### Keep Verbs Consistent

Choose one verb for each meaning and use it consistently across the schema. If your API uses `Delete`, do not also use `Remove` for the same behavior unless they are actually different operations. This matters more over time. In a growing Absinthe API, inconsistent verbs make the schema harder to learn and harder to search. For example, if one resource uses `user_delete` and another uses `invoice_remove`, developers have to remember exceptions. If both mean the same kind of destructive action, they should use the same verb.

### Keep Input and Payload Names Aligned

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

    type Mutation {
      projectMemberAssign(
        input: ProjectMemberAssignInput!
      ): ProjectMemberAssignPayload!
    }

Write it in Absinthe DSL like this:

    field :project_member_assign, :project_member_assign_payload do
      arg :input, non_null(:project_member_assign_input)
      resolve &ProjectMemberResolver.assign/3
    end