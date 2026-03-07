# Absinthe

This document defines the naming conventions for Absinthe GraphQL schemas in this project. It covers field names, type names, resolver function names, module names, and file paths.

Absinthe is an Elixir library for building GraphQL APIs. GraphQL is a query language where clients send queries (read), mutations (write), or subscriptions (stream) to a single endpoint and receive structured data back. Absinthe provides an Elixir DSL for defining the schema that describes available operations and data types.

Follow the rules and guidelines in this document when writing or modifying Absinthe code.

## Dependencies

The following dependencies are required to build an absinthe web application. Add them to your `mix.exs` file.

- Name: `absinthe`
- Dependency: `{:absinthe, "~> 1.9"}`
- Purpose: The main Absinthe library.

- Name: `absinthe_relay`
- Dependency: `{:absinthe_relay, "~> 1.6"}`
- Purpose: Adds Relay support to Absinthe.

- Name: `gql_error_message`
- Dependency: `{:gql_error_message, git: "https://github.com/cylkdev/gql_error_message.git", branch: "main"}`
- Purpose: Adds error message support to Absinthe.

## Glossary

- **Root operation** - One of the three entry points in a GraphQL schema: `query` (read data), `mutation` (write data), or `subscription` (stream data).

- **Root field** - A field defined directly on a root operation. Clients call root fields by name in their GraphQL documents.

- **Nested field** - A field defined on an object type, not on a root operation. Nested fields describe properties or relationships of the parent object.

- **Object type** - A named group of fields that describes a domain entity. Defined with `object` in Absinthe. Example: `object :conversation do ... end`.

- **Input object** - A named group of fields used as an argument to a query or mutation. Defined with `input_object` in Absinthe. Example: `input_object :conversation_create_input do ... end`.

- **Payload** - An object type returned by a mutation. It typically contains the result data and a `user_errors` field.

- **Resolver** - A function that fetches or computes the data for a field. Connected to a field with the `resolve` macro.

- **Connection** - A Relay-style pagination wrapper. A connection contains `edges` (each wrapping a `node`, the actual record) and `pageInfo` (cursor-based pagination metadata such as `hasNextPage` and `endCursor`). Defined with `connection field` (on the parent that holds the list) and `connection node_type:` (to declare the wrapper types) in Absinthe.

- **`use MyAppWeb, :absinthe_schema`** - A macro call that imports the Absinthe schema DSL into the root schema module. It is defined in the application's service module and expands to `use Absinthe.Schema`, `use Absinthe.Relay.Schema, :modern`, and shared type imports.

- **`use MyAppWeb, :absinthe_schema_notation`** - A macro call that imports the Absinthe notation DSL into type, query, mutation, and subscription modules. It expands to `use Absinthe.Schema.Notation` and `use Absinthe.Relay.Schema.Notation, :modern`.

- **`import_types`** - An Absinthe macro used in the root schema module to register a type module so its types are available throughout the schema.

- **`import_fields`** - An Absinthe macro used inside `query do`, `mutation do`, or `subscription do` blocks to pull a named object's fields into the root operation.

## Directory Layout

Organize Absinthe files under `lib/<web_app_name>/`:

    lib/<web_app_name>/
      channels/user_socket.ex            # UserSocket module
      schema.ex                          # Root schema module
      schema/
        queries/<resource>.ex            # Query modules
        mutations/<resource>.ex          # Mutation modules
        subscriptions/<resource>.ex      # Subscription modules
        types/<resource>.ex              # Object and input type modules
      resolvers/<resource>_resolver.ex   # Resolver modules

Each `<resource>` is a singular snake_case domain name (e.g. `conversation`, `message`, `user`).

## Naming Rules

### Root Field Names

Root fields are fields on the root `query`, `mutation`, or `subscription` objects.

**Queries** use noun names only. Use the singular resource name for fetching one record and the plural for fetching a list or connection.

- `:conversation` - returns one conversation
- `:conversations` - returns a paginated list (connection)

**Mutations** use `<resource>_<verb>` format. The resource comes first, the verb comes second. Use a singular resource name because each mutation represents one logical action.

- `:conversation_create` - creates a conversation
- `:conversation_update` - updates a conversation
- `:conversation_delete` - deletes a conversation
- `:conversation_archive` - domain-specific action

Choose one verb per meaning and reuse it across the schema. If your API uses `delete`, do not also use `remove` for the same behaviour.

**Subscriptions** use `<resource>_<past_tense_event>` format.

- `:message_created` - fires when a message is created
- `:message_updated` - fires when a message is updated
- `:message_deleted` - fires when a message is deleted

### Nested Field Names

A nested field is a field on an object type, not on a root operation.

- Name nested fields after the property or relationship they return.
- Use noun names: `creator`, `messages`, `profile`, `organization`.
- Do not apply the `<resource>_<verb>` pattern to nested fields.

### Object Type Names

- **Domain object** - Use the singular resource name. Example: `:conversation`, `:user`.
- **Query group** - `<resource>_queries`. Example: `:conversation_queries`.
- **Mutation group** - `<resource>_mutations`. Example: `:conversation_mutations`.
- **Subscription group** - `<resource>_subscriptions`. Example: `:conversation_subscriptions`.

### Input Object Names

- **Mutation input** - `<field_name>_input`. The name mirrors the mutation field. If the field is `:conversation_create`, the input is `:conversation_create_input`.
- **Filter input** - `<field_name>_filter_input`. The name mirrors the query field. If the field is `:conversations`, the filter input is `:conversations_filter_input`.

### Payload Names

- **Mutation payload** - `<field_name>_payload`. The name mirrors the mutation field. If the field is `:conversation_create`, the payload is `:conversation_create_payload`.

### Module Names

- **Root schema** - `<AppName>.Schema`. Example: `MyAppWeb.Schema`.
- **Query module** - `<AppName>.Schema.Queries.<Resource>`. Example: `MyAppWeb.Schema.Queries.Conversation`.
- **Mutation module** - `<AppName>.Schema.Mutations.<Resource>`. Example: `MyAppWeb.Schema.Mutations.Conversation`.
- **Subscription module** - `<AppName>.Schema.Subscriptions.<Resource>`. Example: `MyAppWeb.Schema.Subscriptions.Conversation`.
- **Type module** - `<AppName>.Schema.Types.<Resource>`. Example: `MyAppWeb.Schema.Types.Conversation`.
- **Resolver module** - `<AppName>.<Resource>Resolver`. Example: `MyAppWeb.ConversationResolver`.

### Resolver Function Names

Name each resolver function to exactly match the field name it resolves. This rule applies to root fields and nested fields alike.

- Field `:conversation` - resolver `conversation/3`
- Field `:conversation_create` - resolver `conversation_create/3`
- Field `:messages` (nested) - resolver `messages/3`
- Field `:creator` (nested) - resolver `creator/3`

Resolver functions receive three arguments: `(parent, args, resolution)`. The `parent` is the value of the enclosing object (or the root value for root fields), `args` is a map of field arguments, and `resolution` is the Absinthe resolution struct.

## Schema Root Module

The root schema module registers all types and wires root fields into the three root operations. It uses two key macros:

- `import_types` makes a type module's definitions available in the schema. Call it once per type, query, mutation, or subscription module.
- `import_fields` pulls a named object's fields into a root operation block (`query do`, `mutation do`, or `subscription do`).

Example:

    # lib/my_app_web/schema.ex
    defmodule MyAppWeb.Schema do
      use MyAppWeb, :absinthe_schema

      alias MyAppWeb.Schema.{
        Mutations,
        Queries,
        Subscriptions,
        Types
      }

      import_types Absinthe.Type.Custom

      import_types Types.{
        Conversation,
        Message,
        Organization,
        Profile,
        User,
        UserError
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
          # Always include GQLErrorMessage.Absinthe.Middleware.
          GQLErrorMessage.Absinthe.Middleware
        ]
      end
    end

## Examples

The examples below show a complete conversation resource across all file types. Every name follows the rules above. Inline comments explain which rule applies.

### Query Module

    # lib/my_app_web/schema/queries/conversation.ex
    defmodule MyAppWeb.Schema.Queries.Conversation do
      use MyAppWeb, :absinthe_schema_notation

      alias MyAppWeb.ConversationResolver

      # Object name: <resource>_queries
      object :conversation_queries do
        # Query field: noun only (singular for one record)
        field :conversation, :conversation do
          arg :id, :id

          # Resolver name matches field name
          resolve &ConversationResolver.conversation/3
        end

        # Query field: noun only (plural for connection)
        # "connection field" creates a Relay-style paginated list
        connection field :conversations, node_type: :conversation do
          arg :id, list_of(:id)
          arg :creator_id, list_of(:id)

          # Filter input name: <field_name>_filter_input
          arg :filter, :conversations_filter_input

          # Resolver name matches field name
          resolve &ConversationResolver.conversations/3
        end
      end
    end

### Mutation Module

    # lib/my_app_web/schema/mutations/conversation.ex
    defmodule MyAppWeb.Schema.Mutations.Conversation do
      use MyAppWeb, :absinthe_schema_notation

      alias MyAppWeb.ConversationResolver

      # Object name: <resource>_mutations
      object :conversation_mutations do
        # Mutation field: <resource>_<verb>
        # Payload name: <field_name>_payload
        field :conversation_create, :conversation_create_payload do
          # Input name: <field_name>_input
          arg :input, non_null(:conversation_create_input)

          # Resolver name matches field name
          resolve &ConversationResolver.conversation_create/3
        end
      end
    end

### Subscription Module

    # lib/my_app_web/schema/subscriptions/conversation.ex
    defmodule MyAppWeb.Schema.Subscriptions.Conversation do
      use MyAppWeb, :absinthe_schema_notation

      # Object name: <resource>_subscriptions
      object :conversation_subscriptions do
        # Subscription field: <resource>_<past_tense_event>
        field :conversation_created, :conversation do
          arg :id, :id

          config fn args, _ ->
            {:ok, topic: "conversation:created:#{args[:id] || "*"}"}
          end
        end

        field :conversation_updated, :conversation do
          arg :id, :id

          config fn args, _ ->
            {:ok, topic: "conversation:updated:#{args[:id] || "*"}"}
          end
        end
      end
    end

### Type Module

This module defines the domain object, its connection wrapper, and related input and payload types for the conversation resource.

    # lib/my_app_web/schema/types/conversation.ex
    defmodule MyAppWeb.Schema.Types.Conversation do
      use MyAppWeb, :absinthe_schema_notation

      alias MyAppWeb.ConversationResolver

      # Domain object name: singular resource
      object :conversation do
        field :id, :id
        field :title, :string
        field :description, :string
        field :creator_id, :id

        # Nested field: noun name (not <resource>_<verb>)
        # Resolver name matches field name
        field :creator, :user do
          resolve &ConversationResolver.creator/3
        end

        # Nested connection field: noun name (plural)
        # "connection field" wraps results in edges/nodes for pagination
        connection field :messages, node_type: :message do
          arg :id, list_of(:id)
          arg :filter, :messages_filter_input

          resolve &ConversationResolver.messages/3
        end

        connection field :participants, node_type: :user do
          arg :id, list_of(:id)
          arg :filter, :participants_filter_input

          resolve &ConversationResolver.participants/3
        end

        field :inserted_at, :datetime
        field :updated_at, :datetime
      end

      # Connection definition for Relay pagination.
      # This declares the :conversation_connection and :conversation_edge types.
      # The "edge do end" block is required by absinthe_relay.
      connection node_type: :conversation do
        field :total_edges, :integer do
          resolve &MyAppWeb.SchemaHelper.total_edges/2
        end

        edge do
        end
      end

      # Filter input name: <field_name>_filter_input
      input_object :conversations_filter_input do
        field :id, :id_filter_input
        field :creator_id, :id_filter_input
        field :title, :string_filter_input
      end

      # Mutation input name: <field_name>_input
      input_object :conversation_create_input do
        field :title, non_null(:string)
        field :participant_ids, non_null(list_of(non_null(:id)))
      end

      # Mutation payload name: <field_name>_payload
      object :conversation_create_payload do
        field :conversation, :conversation
        field :user_errors, list_of(:user_error)
      end
    end

### Supporting Type Modules

    # lib/my_app_web/schema/types/user_error.ex
    defmodule MyAppWeb.Schema.Types.UserError do
      use MyAppWeb, :absinthe_schema_notation

      object :user_error do
        field :field, :string
        field :message, :string
      end
    end

---

    # lib/my_app_web/schema/types/user.ex
    defmodule MyAppWeb.Schema.Types.User do
      use MyAppWeb, :absinthe_schema_notation

      alias MyAppWeb.UserResolver

      object :user do
        field :id, :id
        field :display_name, :string

        # Nested field: noun name
        field :profile, :profile do
          resolve &UserResolver.profile/3
        end

        field :inserted_at, :datetime
        field :updated_at, :datetime
      end
    end

---

    # lib/my_app_web/schema/types/profile.ex
    defmodule MyAppWeb.Schema.Types.Profile do
      use MyAppWeb, :absinthe_schema_notation

      alias MyAppWeb.ProfileResolver

      object :profile do
        field :avatar_url, :string
        field :timezone, :string

        # Nested field: noun name
        field :organization, :organization do
          resolve &ProfileResolver.organization/3
        end

        field :inserted_at, :datetime
        field :updated_at, :datetime
      end
    end

---

    # lib/my_app_web/schema/types/organization.ex
    defmodule MyAppWeb.Schema.Types.Organization do
      use MyAppWeb, :absinthe_schema_notation

      object :organization do
        field :id, :id
        field :name, :string
        field :inserted_at, :datetime
        field :updated_at, :datetime
      end

      connection node_type: :organization do
        field :total_edges, :integer do
          resolve &MyAppWeb.SchemaHelper.total_edges/2
        end

        edge do
        end
      end
    end

---

    # lib/my_app_web/schema/types/message.ex
    defmodule MyAppWeb.Schema.Types.Message do
      use MyAppWeb, :absinthe_schema_notation

      alias MyAppWeb.MessageResolver

      object :message do
        field :id, :id
        field :body, :string

        # Nested field: noun name
        field :creator, :user do
          resolve &MessageResolver.creator/3
        end

        field :inserted_at, :datetime
        field :updated_at, :datetime
      end
    end

### Resolver Modules

Each resolver function name matches the field it resolves. All resolvers receive `(parent, args, resolution)`.

    # lib/my_app_web/resolvers/conversation_resolver.ex
    defmodule MyAppWeb.ConversationResolver do
      # Matches root query field :conversation
      def conversation(_parent, %{id: id}, _resolution) do
        {:ok, %{id: id, title: "General"}}
      end

      # Matches root mutation field :conversation_create
      def conversation_create(_parent, %{input: input}, _resolution) do
        {
          :ok,
          %{
            conversation: %{id: "c-123", title: input.title},
            user_errors: []
          }
        }
      end

      # Matches nested field :creator on :conversation
      def creator(conversation, _args, _resolution) do
        {:ok, %{id: conversation.creator_id, display_name: "User One"}}
      end

      # Matches nested connection field :messages on :conversation
      def messages(_conversation, _args, _resolution) do
        {
          :ok,
          [
            %{id: "m-1", body: "Hello", creator_id: "u-1"},
            %{id: "m-2", body: "Welcome", creator_id: "u-2"}
          ]
        }
      end

      # Matches nested connection field :participants on :conversation
      def participants(_conversation, _args, _resolution) do
        {
          :ok,
          [
            %{id: "u-1", display_name: "User One"},
            %{id: "u-2", display_name: "User Two"}
          ]
        }
      end
    end

---

    # lib/my_app_web/resolvers/message_resolver.ex
    defmodule MyAppWeb.MessageResolver do
      # Matches nested field :creator on :message
      def creator(message, _args, _resolution) do
        {:ok, %{id: message.creator_id, display_name: "Creator"}}
      end
    end

---

    # lib/my_app_web/resolvers/user_resolver.ex
    defmodule MyAppWeb.UserResolver do
      # Matches nested field :profile on :user
      def profile(user, _args, _resolution) do
        {
          :ok,
          %{
            avatar_url: "https://example.com/avatars/#{user.id}.png",
            timezone: "America/Vancouver",
            organization_id: "org-1"
          }
        }
      end
    end

---

    # lib/my_app_web/resolvers/profile_resolver.ex
    defmodule MyAppWeb.ProfileResolver do
      # Matches nested field :organization on :profile
      def organization(profile, _args, _resolution) do
        {:ok, %{id: profile.organization_id, name: "Acme Inc"}}
      end
    end

---

## Service Modules

Service modules must satisfy all of the following:

- Module name follows the format `<AppName>` (e.g. `ChatWebService`).
- Defines public function `absinthe_schema/0`.
- Defines public function `absinthe_schema_notation/0`.
- Defines the public macro `__using__/1` that accepts a single atom argument called `which`.

Example:

```elixir
defmodule MyAppWebService do
  @moduledoc """
  Documentation for `MyAppWebService`.
  """

  @doc false
  @spec absinthe_schema :: Macro.t()
  def absinthe_schema do
    quote do
      use Absinthe.Schema
      use Absinthe.Relay.Schema, :modern

      # This module adds support for the following data types:
      #
      # - datetime (UTC)
      # - naive_datetime
      # - date
      # - time
      # - decimal (requires the :decimal dependency)
      import_types Absinthe.Type.Custom

      # requires the :gql_error_message dependency
      import_types GQLErrorMessage.Absinthe.Type

      # If the project has shared absinthe type modules, add them here:
      # import_types <shared_type_module>
    end
  end

  @doc false
  @spec absinthe_schema_notation :: Macro.t()
  def absinthe_schema_notation do
    quote do
      use Absinthe.Schema.Notation
      use Absinthe.Relay.Schema.Notation, :modern

      # requires the :gql_error_message dependency
      require GQLErrorMessage.Absinthe.Type

      # If the project has shared absinthe type modules, add them here:
      # require <shared_type_module>
    end
  end

  @doc false
  defmacro __using__(which) when is_atom(which) do
    apply(__MODULE__, which, [])
  end
end
```

## Mutation Modules

Mutation modules must satisfy all of the following:

- Module names follows the format `<AppName>.Schema.Mutations.<Resource>` (e.g. `MyAppWeb.Schema.Mutations.Message`).

Example:

```elixir
defmodule MyAppWeb.Schema.Mutations.Message do
  @moduledoc """
  GraphQL mutation fields for creating, updating, and deleting messages.

  This module defines the mutation fields that are exposed under the GraphQL
  mutation root for message operations.

  ## How this module is organized

  Each mutation in this module follows the common GraphQL mutation shape:

  - The mutation takes a single `input` argument.
  - The `input` argument uses a distinct input object type (e.g. `:message_create_input`).
  - The mutation returns a distinct payload type (e.g. `:message_create_payload`).

  ## Getting started

  To use the api a client calls one mutation field and passes an `input` object. The client then chooses which response fields to return from the payload.

  Example shape:

      mutation MessageCreate($input: MessageCreateInput!) {
        messageCreate(input: $input) {
          message {
            id
          }
          userErrors {
            field
            message
          }
        }
      }

  ### Why `userErrors` is shown in the examples

  Many GraphQL APIs return validation and business-rule failures in the mutation payload (for example in a `userErrors` field) instead of relying only on top-level GraphQL errors. Requesting `userErrors` makes client behaviour easier to debug.

  ### What this module does not define

  This module only defines the mutation fields and connects them to resolver functions. The input object types, payload object types, and resolver logic are defined elsewhere in the schema and application.
  """

  use MyAppWebService, :absinthe_schema_notation

  alias MyAppWeb.MessageResolver

  @desc """
  Message-related mutation fields.

  Grouping related fields under a dedicated object keeps the mutation surface
  area easier to read.
  """
  object :message_mutations do
    @desc """
    Creates a new message.

    Use this mutation when the client wants to create a message record. Pass the required message data inside the `input` argument.

    ## Arguments

    - `input`: A `MessageCreateInput` object with the fields required to create a message.

    ## Returns

    A `MessageCreatePayload` object.

    ## Example

        mutation MessageCreate($input: MessageCreateInput!) {
          messageCreate(input: $input) {
            message {
              id
            }
            userErrors {
              field
              message
            }
          }
        }

    Example variables:

        {
          "input": {
            "conversationId": "123",
            "body": "Hello"
          }
        }
    """
    field :message_create, :message_create_payload do
      arg :input, non_null(:message_create_input),
        description: """
        Input data used to create a message.

        This should contain the fields required by `MessageCreateInput`, such as
        the message body and any related identifiers your schema requires.
        """

      resolve &MessageResolver.message_create/3
    end

    @desc """
    Updates an existing message.

    Use this mutation when the client wants to change a message that already
    exists. Pass the message identifier and the fields to change inside `input`.

    ## Arguments

    - `input`: A `MessageUpdateInput` object that identifies the target message
      and provides the updated values.

    ## Returns

    A `MessageUpdatePayload` object.

    ## Example

        mutation MessageUpdate($input: MessageUpdateInput!) {
          messageUpdate(input: $input) {
            message {
              id
              body
              updatedAt
            }
            userErrors {
              field
              message
            }
          }
        }

    Example variables:

        {
          "input": {
            "id": "msg_123",
            "body": "Updated text"
          }
        }
    """
    field :message_update, :message_update_payload do
      arg :input, non_null(:message_update_input),
        description: """
        Input data used to update a message.

        This should include the message identifier and the fields that should be
        changed, as defined by `MessageUpdateInput`.
        """

      resolve &MessageResolver.message_update/3
    end

    @desc """
    Deletes an existing message.

    Use this mutation when the client wants to remove a message. Pass the target message identifier inside `input`.

    ## Arguments

    - `input`: A `MessageDeleteInput` object.

    ## Returns

    A `MessageDeletePayload` object.
    
    ## Example

        mutation MessageDelete($input: MessageDeleteInput!) {
          messageDelete(input: $input) {
            deletedMessageId
            userErrors {
              field
              message
            }
          }
        }

    Example variables:

        {
          "input": {
            "id": "msg_123"
          }
        }
    """
    field :message_delete, :message_delete_payload do
      arg :input, non_null(:message_delete_input),
        description: """
        Input data used to delete a message.

        This usually contains the identifier of the message to delete, as defined
        by `MessageDeleteInput`.
        """

      resolve &MessageResolver.message_delete/3
    end
  end
end
```

## Query Modules

Query modules must satisfy all of the following:

- Module names follows the format `<AppName>.Schema.Queries.<Resource>` (e.g. `MyAppWeb.Schema.Queries.Message`).

Example:

```elixir
defmodule MyAppWeb.Schema.Queries.Message do
  @moduledoc """
  GraphQL query fields for reading messages.

  This module defines query fields that let a client retrieve message data.
  In GraphQL, queries are used for reading data. They do not create, update,
  or delete records.

  ## How a client uses queries

  A client calls a query field and passes arguments to that field. The client
  then chooses which fields to return in the response.

  This module defines two query fields:

  - `message(id: ID!)` for fetching one message by id.
  - `messages(filter: MessagesFilterInput)` for fetching a list of messages.

  ## Example query for one message

      query Message($id: ID!) {
        message(id: $id) {
          id
          body
          insertedAt
          updatedAt
        }
      }

  Example variables:

      {
        "id": "msg_123"
      }

  ## Example query for a list of messages

      query Messages($filter: MessagesFilterInput) {
        messages(filter: $filter) {
          edges {
            node {
              id
              body
            }
          }
        }
      }

  Example variables:

      {
        "filter": {
          "conversationId": "conv_123"
        }
      }

  ## Important note about errors

  GraphQL queries can return top-level GraphQL errors (for example, invalid
  syntax or resolver failures). Some APIs also include domain-specific error
  fields in custom payload types. This module's query fields return message
  data types (`:message` and `:message_connection`), so clients should request
  the fields defined on those types and handle top-level GraphQL errors in the
  standard GraphQL response.

  ## What this module does not define

  This module only defines query fields and connects them to resolver functions.
  The object types (such as `:message` and `:message_connection`), input types
  (such as `:messages_filter_input`), and resolver logic are defined elsewhere
  in the schema and application.
  """

  use MyAppWebService, :absinthe_schema_notation

  alias MyAppWeb.MessageResolver

  @desc """
  Message-related query fields.

  Grouping related fields under one object makes the query schema easier to
  read and makes it clear where message read operations are defined.
  """
  object :message_queries do
    @desc """
    Fetches a single message by id.

    Use this query when the client already knows the message id and needs the
    current message data.

    ## Arguments

    - `id`: The unique identifier of the message to retrieve.

    ## Returns

    A `Message` object (`:message` in the Absinthe schema).

    ## Example

        query Message($id: ID!) {
          message(id: $id) {
            id
            body
            insertedAt
            updatedAt
          }
        }

    Example variables:

        {
          "id": "msg_123"
        }
    """
    field :message, :message do
      arg :id, non_null(:id),
        description: """
        The id of the message to retrieve.

        This must be a valid message identifier that exists in the system.
        """

      resolve &MessageResolver.message/3
    end

    @desc """
    Fetches a list of messages.

    Use this query when the client needs multiple messages, optionally filtered
    by criteria such as conversation or other supported fields.

    ## Arguments

    - `filter`: Optional filter criteria used to narrow the results.

    ## Returns

    A non-null message connection (`:message_connection`)

    ## Example

        query Messages($filter: MessagesFilterInput) {
          messages(filter: $filter) {
            edges {
              node {
                id
                body
                insertedAt
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }

    Example variables:

        {
          "filter": {
            "conversationId": "conv_123"
          }
        }
    """
    field :messages, non_null(:message_connection) do
      arg :filter, :messages_filter_input,
        description: """
        Optional filter input used to limit which messages are returned.

        The available filter fields are defined by `MessagesFilterInput`.
        """

      resolve &MessageResolver.messages/3
    end
  end
end
```

## Subscription Modules

Subscription modules must satisfy all of the following:

- Module names follows the format `<AppName>.Schema.Subscriptions.<Resource>` (e.g. `MyAppWeb.Schema.Subscriptions.Message`).

Example:

```elixir
defmodule MyAppWeb.Schema.Subscriptions.Message do
  @moduledoc """
  GraphQL subscriptions for message events.

  This module defines the subscription fields that allow clients to receive
  real-time updates when message records change.

  ## How this module is organized

  Each subscription in this module follows the common GraphQL subscription
  shape:

  - The subscription can declare arguments used to scope which events the
    client should receive (for example, a specific conversation id).
  - The subscription returns an object type that matches the event payload
    (for example, a `:message` or a custom payload type).

  ## Getting started

  To use the API a client subscribes to one of the defined fields and supplies
  any required arguments. The server will push new results to the client
  whenever the corresponding event occurs.

  Example shape:

      subscription MessageCreated($conversationId: ID!) {
        messageCreated(conversationId: $conversationId) {
          id
          body
          insertedAt
        }
      }

  ### What this module does not define

  This module only defines subscription fields and connects them to trigger
  logic. The published payload types (such as `:message`) and the functions
  that broadcast events are defined elsewhere in the schema and application.
  """

  use MyAppWebService, :absinthe_schema_notation

  alias MyAppWeb.MessageNotifier

  @desc """
  Message-related subscription fields.

  Grouping related fields under one object keeps the subscription surface area
  organized and makes it clear where message notifications are defined.
  """
  object :message_subscriptions do
    @desc """
    Subscribes to newly created messages.

    Use this subscription when the client wants to be notified whenever a new
    message is created. Scope by conversation when possible to avoid receiving
    unrelated events.

    ## Arguments

    - `conversation_id`: Optional conversation identifier used to scope the
      stream of events. When provided, only messages from that conversation are
      emitted.

    ## Returns

    A `Message` object (`:message` in the Absinthe schema).

    ## Example

        subscription MessageCreated($conversationId: ID!) {
          messageCreated(conversationId: $conversationId) {
            id
            body
            insertedAt
          }
        }

    Example variables:

        {
          "conversationId": "conv_123"
        }
    """
    field :message_created, :message do
      arg :conversation_id, :id,
        description: "Optional conversation id to scope message creation events."

      config &MessageResolver.message_created/2
    end

    @desc """
    Subscribes to message updates.

    Use this subscription when the client wants to react to changes in existing
    messages (for example, edited content).

    ## Arguments

    - `conversation_id`: Optional conversation identifier used to scope events.

    ## Returns

    A `Message` object (`:message` in the Absinthe schema).
    """
    field :message_updated, :message do
      arg :conversation_id, :id,
        description: "Optional conversation id to scope message update events."

      config &MessageResolver.message_updated/2
    end

    @desc """
    Subscribes to message deletions.

    Use this subscription when the client needs to know when messages are
    removed.

    ## Arguments

    - `conversation_id`: Optional conversation identifier used to scope events.

    ## Returns

    A `Message` object (`:message` in the Absinthe schema). The resolver should
    document what fields are present on deleted messages (for example, only id
    may be available).
    """
    field :message_deleted, :message do
      arg :conversation_id, :id,
        description: "Optional conversation id to scope message deletion events."

      config &MessageResolver.message_deleted/2
    end
  end
end
```

## UserSocket Module

The UserSocket module must satisfy all of the following:

- The module name must follow the format `<AppName>.UserSocket` (e.g. `ChatWebService.UserSocket`).

- The module must define "use Phoenix.Socket" and "use Absinthe.Phoenix.Socket".

Example:

```elixir
defmodule ChatWebService.UserSocket do
  use Phoenix.Socket

  # Required for absinthe
  use Absinthe.Phoenix.Socket,
    schema: ChatWebService.Schema,
    gc_interval: 60_000

  @logger_prefix "ChatWebService.UserSocket"

  @impl true
  def connect(params, socket, _connect_info) do
    with {:ok, context} <- authorize(params) do
      # The context is assigned to both the absinthe socket
      # and the phoenix socket. This allows you to access
      # the context in phoenix controllers and absinthe
      # resolvers.
      socket =
        socket
        |> Absinthe.Phoenix.Socket.put_options(context: context)
        |> assign(:context, context)

      {:ok, socket}
    end
  end

  @impl true
  def id(socket) do
    "user:#{socket.assigns.context.current_user.id}"
  end

  @doc false
  def authorize(params) do
    case extract_bearer_token(params) do
      nil ->
        SharedUtils.Logger.warn(@logger_prefix, "bearer token not found")

        :error

      token ->
        case AuthService.verify_session_token(token) do
          {:ok, context} ->
            SharedUtils.Logger.debug(
              @logger_prefix,
              "authorized context:\n\n #{inspect(context, pretty: true)}"
            )

            {:ok, context}

          {:error, reason} ->
            SharedUtils.Logger.warn(@logger_prefix, "authorization failed: #{inspect(reason)}")

            :error
        end
    end
  end

  defp extract_bearer_token(params) do
    Enum.find_value(params, fn {key, value} ->
      if String.downcase(key) === "authorization" do
        [scheme, token] = String.split(value, " ", parts: 2)

        if String.downcase(scheme) === "bearer" and token !== "" do
          token
        end
      end
    end)
  end
end
```
