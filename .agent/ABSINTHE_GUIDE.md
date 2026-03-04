# Build a Complete Absinthe GraphQL API in This Repository

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan must be maintained in accordance with `.agent/PLANS.md`.

## Purpose / Big Picture

After completing this plan, you will have a working Phoenix + Absinthe GraphQL API with queries, mutations, and subscriptions, built from zero using the naming and module patterns from `.agent/ABSINTHE.md`.

You will be able to:

1. Start a server.
2. Send GraphQL queries and mutations to `/api/graphql`.
3. Open GraphiQL at `/api/graphiql`.
4. Subscribe to `conversationCreated` events over websocket and see live updates when a mutation runs.

This repository currently contains only the `ecto_shorts` library and no Phoenix web API. To stay self-contained and avoid breaking the library project, this plan creates a standalone Phoenix service at `examples/chat_web_service` inside the current working tree.

## Progress

**Legend**

[ ] - Not started
[~] - In progress
[x] - Completed

- [x] (2026-03-04 17:40 America/Toronto) ExecPlan authored and reviewed against `.agent/ABSINTHE.md` patterns.
- [ ] Create the Phoenix service scaffold at `examples/chat_web_service`.
- [ ] Add Absinthe dependencies and fetch packages.
- [ ] Add Absinthe service module macros (`absinthe_schema/0`, `absinthe_schema_notation/0`, `__using__/1`).
- [ ] Add root schema, query/mutation/subscription modules, type modules, resolver modules, and schema helper.
- [ ] Wire router, endpoint, application supervision tree, and user socket for subscriptions.
- [ ] Start server and verify query, mutation, and subscription end-to-end.
- [ ] Run tests and compile checks.

## Surprises & Discoveries

- Observation: `.agent/ABSINTHE.md` lists `absinthe`, `absinthe_relay`, and `gql_error_message`, but examples also require HTTP/websocket integration modules.
  Evidence: The document includes `Absinthe.Phoenix.Socket` and GraphQL endpoint usage patterns, which require `absinthe_phoenix` and `absinthe_plug`.

- Observation: This repository is not already a Phoenix app.
  Evidence: Root `mix.exs` is the `:ecto_shorts` library project and has no endpoint/router modules.

## Decision Log

- Decision: Create a standalone Phoenix app in `examples/chat_web_service` instead of modifying root `:ecto_shorts` as an app.
  Rationale: This keeps the existing library project stable and gives a true beginner a deterministic path to a running API.
  Date/Author: 2026-03-04 / Codex

- Decision: Use `ChatWebService` as the concrete `<AppName>` and `conversation` as the concrete `<resource>`.
  Rationale: `.agent/ABSINTHE.md` uses these names in examples, making pattern matching easier.
  Date/Author: 2026-03-04 / Codex

- Decision: Include missing official setup details for `Absinthe.Plug`, `Absinthe.Phoenix.Endpoint`, and `Absinthe.Subscription` supervision.
  Rationale: Without these steps, subscriptions and HTTP GraphQL endpoints will not work end-to-end.
  Date/Author: 2026-03-04 / Codex

## Outcomes & Retrospective

This plan is intentionally implementation-ready but not yet executed. When execution finishes, update this section with:

1. What worked exactly as planned.
2. Any deviations from file/module naming patterns.
3. Final verification evidence (query result, mutation result, subscription event).
4. Remaining gaps, if any.

## Context and Orientation

This section defines every term you need in plain language and maps it to files you will create.

GraphQL is an API style where the client sends a text document describing exactly which fields it wants. Absinthe is the Elixir GraphQL library. A schema is the server-side contract that defines valid queries, mutations, and subscriptions.

A query reads data. A mutation writes data. A subscription streams data events over websocket.

A resolver is an Elixir function that returns the value for a GraphQL field. In this plan every resolver has three arguments and follows Absinthe conventions:

    (parent, args, resolution)

A connection is Relay-style pagination. It wraps lists in:

1. `edges` (the items).
2. `pageInfo` (cursor metadata like `hasNextPage`).

A socket is Phoenix websocket infrastructure. `Absinthe.Phoenix.Socket` attaches GraphQL subscription behavior to the socket.

A Plug is a composable HTTP middleware module in Phoenix/Plug. `Absinthe.Plug` serves GraphQL over HTTP.

You will create this directory layout inside the generated Phoenix app:

    examples/chat_web_service/
      lib/chat_web_service.ex
      lib/chat_web_service/schema.ex
      lib/chat_web_service/schema_helper.ex
      lib/chat_web_service/channels/user_socket.ex
      lib/chat_web_service/resolvers/conversation_resolver.ex
      lib/chat_web_service/resolvers/message_resolver.ex
      lib/chat_web_service/resolvers/profile_resolver.ex
      lib/chat_web_service/resolvers/user_resolver.ex
      lib/chat_web_service/schema/queries/conversation.ex
      lib/chat_web_service/schema/mutations/conversation.ex
      lib/chat_web_service/schema/subscriptions/conversation.ex
      lib/chat_web_service/schema/types/conversation.ex
      lib/chat_web_service/schema/types/filter.ex
      lib/chat_web_service/schema/types/message.ex
      lib/chat_web_service/schema/types/organization.ex
      lib/chat_web_service/schema/types/profile.ex
      lib/chat_web_service/schema/types/user.ex
      lib/chat_web_service/schema/types/user_error.ex
      lib/chat_web_service/application.ex
      lib/chat_web_service_web/endpoint.ex
      lib/chat_web_service_web/router.ex

## Plan of Work

First scaffold a fresh Phoenix API project under `examples/chat_web_service`. Next add Absinthe dependencies. Then create the service macro module and all schema/resource/resolver files using the exact naming patterns from `.agent/ABSINTHE.md`.

After schema files exist, wire HTTP and websocket transport in router and endpoint, and register the subscription supervisor child in `application.ex`. Finally run compile/tests/server and validate query, mutation, and subscription behavior with concrete requests.

Naming must follow these exact rules from `.agent/ABSINTHE.md`:

1. Query root fields are nouns: singular for one record (`conversation`), plural for lists (`conversations`).
2. Mutation root fields are `<resource>_<verb>` (`conversation_create`).
3. Subscription root fields are `<resource>_<past_tense_event>` (`conversation_created`).
4. Query object names are `<resource>_queries`.
5. Mutation object names are `<resource>_mutations`.
6. Subscription object names are `<resource>_subscriptions`.
7. Input names mirror field names (`conversation_create_input`, `conversations_filter_input`).
8. Payload names mirror field names (`conversation_create_payload`).
9. Resolver function names exactly match field names (`conversation_create/3` resolves `:conversation_create`).
10. Type, query, mutation, and subscription module names use `<AppName>.Schema.<Group>.<Resource>`.

Client-side GraphQL documents will usually use camelCase (`conversationCreate`, `participantIds`, `displayName`) even though schema field identifiers in Elixir are written as snake_case. This is normal Absinthe language-convention behavior.

## Concrete Steps

Run all commands exactly as shown.

### Step 1: Create the Phoenix app scaffold

From repository root (`/Users/kurthogarth/Documents/GitHub/ecto_shorts`):

    mkdir -p examples
    mix archive.install hex phx_new --force
    mix phx.new examples/chat_web_service --module ChatWebService --app chat_web_service --no-ecto --no-html --no-live --no-mailer --no-dashboard

Enter the app:

    cd examples/chat_web_service

### Step 2: Add Absinthe dependencies

Edit `examples/chat_web_service/mix.exs` and add these dependencies to `deps/0`:

    {:absinthe, "~> 1.9"},
    {:absinthe_relay, "~> 1.6"},
    {:absinthe_plug, "~> 1.5"},
    {:absinthe_phoenix, "~> 2.0"},
    {:gql_error_message, git: "https://github.com/cylkdev/gql_error_message.git", branch: "main"}

Fetch dependencies:

    mix deps.get

Create directories for the schema and resolver modules:

    mkdir -p lib/chat_web_service/schema/queries
    mkdir -p lib/chat_web_service/schema/mutations
    mkdir -p lib/chat_web_service/schema/subscriptions
    mkdir -p lib/chat_web_service/schema/types
    mkdir -p lib/chat_web_service/resolvers
    mkdir -p lib/chat_web_service/channels

### Step 3: Replace `lib/chat_web_service.ex` with Absinthe service macros

Create this exact content:

    defmodule ChatWebService do
      @moduledoc """
      Documentation for `ChatWebService`.
      """

      @doc false
      @spec absinthe_schema :: Macro.t()
      def absinthe_schema do
        quote do
          use Absinthe.Schema
          use Absinthe.Relay.Schema, :modern

          import_types Absinthe.Type.Custom
          import_types GQLErrorMessage.Absinthe.Type
        end
      end

      @doc false
      @spec absinthe_schema_notation :: Macro.t()
      def absinthe_schema_notation do
        quote do
          use Absinthe.Schema.Notation
          use Absinthe.Relay.Schema.Notation, :modern

          require GQLErrorMessage.Absinthe.Type
        end
      end

      @doc false
      defmacro __using__(which) when is_atom(which) do
        apply(__MODULE__, which, [])
      end
    end

### Step 4: Create schema helper

Create `lib/chat_web_service/schema_helper.ex`:

    defmodule ChatWebService.SchemaHelper do
      @moduledoc false

      def total_edges(%{edges: edges}, _resolution), do: {:ok, length(edges)}
      def total_edges(_, _resolution), do: {:ok, 0}
    end

### Step 5: Create root schema module

Create `lib/chat_web_service/schema.ex`:

    defmodule ChatWebService.Schema do
      use ChatWebService, :absinthe_schema

      alias ChatWebService.Schema.{
        Mutations,
        Queries,
        Subscriptions,
        Types
      }

      import_types Types.{
        Conversation,
        Filter,
        Message,
        Organization,
        Profile,
        User,
        UserError
      }

      import_types Mutations.Conversation
      import_types Queries.Conversation
      import_types Subscriptions.Conversation

      mutation do
        import_fields :conversation_mutations
      end

      query do
        import_fields :conversation_queries
      end

      subscription do
        import_fields :conversation_subscriptions
      end

      def middleware(middleware, _field, %{identifier: identifier})
          when identifier in [:query, :mutation, :subscription] do
        pre_resolution_middleware() ++ middleware ++ post_resolution_middleware()
      end

      def middleware(middleware, _, _) do
        middleware
      end

      defp pre_resolution_middleware do
        []
      end

      defp post_resolution_middleware do
        [GQLErrorMessage.Absinthe.Middleware]
      end
    end

### Step 6: Create type modules

Create `lib/chat_web_service/schema/types/filter.ex`:

    defmodule ChatWebService.Schema.Types.Filter do
      use ChatWebService, :absinthe_schema_notation

      input_object :id_filter_input do
        field :eq, :id
        field :in, list_of(:id)
      end

      input_object :string_filter_input do
        field :eq, :string
        field :ilike, :string
      end
    end

Create `lib/chat_web_service/schema/types/user_error.ex`:

    defmodule ChatWebService.Schema.Types.UserError do
      use ChatWebService, :absinthe_schema_notation

      object :user_error do
        field :field, :string
        field :message, :string
      end
    end

Create `lib/chat_web_service/schema/types/organization.ex`:

    defmodule ChatWebService.Schema.Types.Organization do
      use ChatWebService, :absinthe_schema_notation

      object :organization do
        field :id, :id
        field :name, :string
        field :inserted_at, :datetime
        field :updated_at, :datetime
      end

      connection node_type: :organization do
        field :total_edges, :integer do
          resolve &ChatWebService.SchemaHelper.total_edges/2
        end

        edge do
        end
      end
    end

Create `lib/chat_web_service/schema/types/profile.ex`:

    defmodule ChatWebService.Schema.Types.Profile do
      use ChatWebService, :absinthe_schema_notation

      alias ChatWebService.ProfileResolver

      object :profile do
        field :avatar_url, :string
        field :timezone, :string

        field :organization, :organization do
          resolve &ProfileResolver.organization/3
        end

        field :inserted_at, :datetime
        field :updated_at, :datetime
      end
    end

Create `lib/chat_web_service/schema/types/user.ex`:

    defmodule ChatWebService.Schema.Types.User do
      use ChatWebService, :absinthe_schema_notation

      alias ChatWebService.UserResolver

      object :user do
        field :id, :id
        field :display_name, :string

        field :profile, :profile do
          resolve &UserResolver.profile/3
        end

        field :inserted_at, :datetime
        field :updated_at, :datetime
      end
    end

Create `lib/chat_web_service/schema/types/message.ex`:

    defmodule ChatWebService.Schema.Types.Message do
      use ChatWebService, :absinthe_schema_notation

      alias ChatWebService.MessageResolver

      object :message do
        field :id, :id
        field :body, :string

        field :creator, :user do
          resolve &MessageResolver.creator/3
        end

        field :inserted_at, :datetime
        field :updated_at, :datetime
      end
    end

Create `lib/chat_web_service/schema/types/conversation.ex`:

    defmodule ChatWebService.Schema.Types.Conversation do
      use ChatWebService, :absinthe_schema_notation

      alias ChatWebService.ConversationResolver

      object :conversation do
        field :id, :id
        field :title, :string
        field :description, :string
        field :creator_id, :id

        field :creator, :user do
          resolve &ConversationResolver.creator/3
        end

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

      connection node_type: :conversation do
        field :total_edges, :integer do
          resolve &ChatWebService.SchemaHelper.total_edges/2
        end

        edge do
        end
      end

      input_object :conversations_filter_input do
        field :id, :id_filter_input
        field :creator_id, :id_filter_input
        field :title, :string_filter_input
      end

      input_object :messages_filter_input do
        field :id, :id_filter_input
        field :creator_id, :id_filter_input
        field :body, :string_filter_input
      end

      input_object :participants_filter_input do
        field :id, :id_filter_input
        field :display_name, :string_filter_input
      end

      input_object :conversation_create_input do
        field :title, non_null(:string)
        field :participant_ids, non_null(list_of(non_null(:id)))
      end

      object :conversation_create_payload do
        field :conversation, :conversation
        field :user_errors, list_of(:user_error)
      end
    end

### Step 7: Create query, mutation, and subscription modules

Create `lib/chat_web_service/schema/queries/conversation.ex`:

    defmodule ChatWebService.Schema.Queries.Conversation do
      use ChatWebService, :absinthe_schema_notation

      alias ChatWebService.ConversationResolver

      object :conversation_queries do
        field :conversation, :conversation do
          arg :id, :id

          resolve &ConversationResolver.conversation/3
        end

        connection field :conversations, node_type: :conversation do
          arg :id, list_of(:id)
          arg :creator_id, list_of(:id)
          arg :filter, :conversations_filter_input

          resolve &ConversationResolver.conversations/3
        end
      end
    end

Create `lib/chat_web_service/schema/mutations/conversation.ex`:

    defmodule ChatWebService.Schema.Mutations.Conversation do
      use ChatWebService, :absinthe_schema_notation

      alias ChatWebService.ConversationResolver

      object :conversation_mutations do
        field :conversation_create, :conversation_create_payload do
          arg :input, non_null(:conversation_create_input)

          resolve &ConversationResolver.conversation_create/3
        end
      end
    end

Create `lib/chat_web_service/schema/subscriptions/conversation.ex`:

    defmodule ChatWebService.Schema.Subscriptions.Conversation do
      use ChatWebService, :absinthe_schema_notation

      object :conversation_subscriptions do
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

### Step 8: Create resolver modules

Create `lib/chat_web_service/resolvers/conversation_resolver.ex`:

    defmodule ChatWebService.ConversationResolver do
      alias ChatWebServiceWeb.Endpoint

      def conversation(_parent, %{id: id}, _resolution) do
        {:ok, %{id: id, title: "General", description: "Default room", creator_id: "u-1"}}
      end

      def conversation(_parent, _args, _resolution) do
        {:ok, %{id: "c-1", title: "General", description: "Default room", creator_id: "u-1"}}
      end

      def conversations(_parent, _args, _resolution) do
        {:ok,
         [
           %{id: "c-1", title: "General", description: "Default room", creator_id: "u-1"},
           %{id: "c-2", title: "Engineering", description: "Engineering room", creator_id: "u-2"}
         ]}
      end

      def conversation_create(_parent, %{input: input}, _resolution) do
        conversation = %{
          id: "c-#{System.unique_integer([:positive])}",
          title: input.title,
          description: "Created from mutation",
          creator_id: "u-1"
        }

        Absinthe.Subscription.publish(
          Endpoint,
          conversation,
          conversation_created: conversation.id
        )

        Absinthe.Subscription.publish(
          Endpoint,
          conversation,
          conversation_created: "*"
        )

        {:ok, %{conversation: conversation, user_errors: []}}
      end

      def creator(conversation, _args, _resolution) do
        {:ok, %{id: conversation.creator_id, display_name: "User #{conversation.creator_id}"}}
      end

      def messages(_conversation, _args, _resolution) do
        {:ok,
         [
           %{id: "m-1", body: "Hello", creator_id: "u-1"},
           %{id: "m-2", body: "Welcome", creator_id: "u-2"}
         ]}
      end

      def participants(_conversation, _args, _resolution) do
        {:ok,
         [
           %{id: "u-1", display_name: "User One"},
           %{id: "u-2", display_name: "User Two"}
         ]}
      end
    end

Create `lib/chat_web_service/resolvers/message_resolver.ex`:

    defmodule ChatWebService.MessageResolver do
      def creator(message, _args, _resolution) do
        {:ok, %{id: message.creator_id, display_name: "Creator"}}
      end
    end

Create `lib/chat_web_service/resolvers/user_resolver.ex`:

    defmodule ChatWebService.UserResolver do
      def profile(user, _args, _resolution) do
        {:ok,
         %{
           avatar_url: "https://example.com/avatars/#{user.id}.png",
           timezone: "America/Toronto",
           organization_id: "org-1"
         }}
      end
    end

Create `lib/chat_web_service/resolvers/profile_resolver.ex`:

    defmodule ChatWebService.ProfileResolver do
      def organization(profile, _args, _resolution) do
        {:ok, %{id: profile.organization_id, name: "Acme Inc"}}
      end
    end

### Step 9: Add UserSocket for Absinthe subscriptions

Create `lib/chat_web_service/channels/user_socket.ex`:

    defmodule ChatWebService.UserSocket do
      use Phoenix.Socket

      use Absinthe.Phoenix.Socket,
        schema: ChatWebService.Schema,
        gc_interval: 60_000

      @impl true
      def connect(params, socket, _connect_info) do
        with {:ok, context} <- authorize(params) do
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
            {:ok, %{current_user: %{id: "guest"}}}

          token ->
            {:ok, %{current_user: %{id: token}}}
        end
      end

      defp extract_bearer_token(params) do
        Enum.find_value(params, fn {key, value} ->
          if String.downcase(to_string(key)) == "authorization" do
            case String.split(value, " ", parts: 2) do
              [scheme, token] ->
                if String.downcase(scheme) == "bearer" and token != "" do
                  token
                end

              _ -> nil
            end
          end
        end)
      end
    end

### Step 10: Wire endpoint, router, and supervision tree

Edit `lib/chat_web_service_web/endpoint.ex`.

1. Add `use Absinthe.Phoenix.Endpoint` inside the endpoint module.
2. Add socket configuration for the Absinthe socket.
3. Ensure `Plug.Parsers` includes `Absinthe.Plug.Parser`.

Final relevant shape:

    defmodule ChatWebServiceWeb.Endpoint do
      use Phoenix.Endpoint, otp_app: :chat_web_service
      use Absinthe.Phoenix.Endpoint

      socket "/socket", ChatWebService.UserSocket,
        websocket: true,
        longpoll: false

      plug Plug.Parsers,
        parsers: [:urlencoded, :multipart, :json, Absinthe.Plug.Parser],
        pass: ["*/*"],
        json_decoder: Phoenix.json_library()

      # keep the rest of the generated endpoint plugs as generated
    end

Edit `lib/chat_web_service_web/router.ex` and add API forwards:

    defmodule ChatWebServiceWeb.Router do
      use ChatWebServiceWeb, :router

      pipeline :api do
        plug :accepts, ["json"]
      end

      scope "/api" do
        pipe_through :api

        forward "/graphql", Absinthe.Plug,
          schema: ChatWebService.Schema

        if Mix.env() == :dev do
          forward "/graphiql", Absinthe.Plug.GraphiQL,
            schema: ChatWebService.Schema,
            socket: ChatWebService.UserSocket
        end
      end
    end

Edit `lib/chat_web_service/application.ex` and add `Absinthe.Subscription` after endpoint in children.

If you generated with `--no-ecto`, the child list will not include a Repo. Keep every existing child and insert only this new one after `ChatWebServiceWeb.Endpoint`:

    children = [
      ChatWebServiceWeb.Telemetry,
      {DNSCluster, query: Application.get_env(:chat_web_service, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: ChatWebService.PubSub},
      ChatWebServiceWeb.Endpoint,
      {Absinthe.Subscription, ChatWebServiceWeb.Endpoint}
    ]

### Step 11: Compile and run

From `examples/chat_web_service`:

    mix deps.get
    mix compile
    mix test
    mix phx.server

### Step 12: Validate behavior end-to-end

Open a new terminal and run a query:

    curl -s http://localhost:4000/api/graphql \
      -H 'content-type: application/json' \
      -d '{"query":"query { conversation(id: \"c-1\") { id title creator { id displayName } messages(first: 2) { edges { node { id body } } } } }"}'

You should see JSON with `conversation.id`, `conversation.title`, and `messages.edges`.

Run a mutation:

    curl -s http://localhost:4000/api/graphql \
      -H 'content-type: application/json' \
      -d '{"query":"mutation($input: ConversationCreateInput!) { conversationCreate(input: $input) { conversation { id title } userErrors { field message } } }","variables":{"input":{"title":"GraphQL Room","participantIds":["u-1","u-2"]}}}'

You should see `conversationCreate.conversation.id` and an empty `userErrors` array.

Validate subscription manually in GraphiQL:

1. Visit `http://localhost:4000/api/graphiql`.
2. In tab A, run:

       subscription {
         conversationCreated {
           id
           title
         }
       }

3. In tab B, run the `conversationCreate` mutation again.
4. Tab A should receive a pushed payload with new conversation data.

## Validation and Acceptance

This plan is complete only when all acceptance checks pass.

1. `mix compile` succeeds with no undefined module/function errors.
2. `mix test` succeeds.
3. `POST /api/graphql` serves query and mutation responses.
4. `GET /api/graphiql` loads in `dev` and can execute operations.
5. Subscription receives a live event after running `conversationCreate`.
6. Naming conventions match `.agent/ABSINTHE.md` exactly:
   root fields, input names, payload names, module names, resolver names, and file paths.

## Idempotence and Recovery

These steps are safe to re-run.

- Running `mix deps.get`, `mix compile`, and `mix test` repeatedly is safe.
- If scaffold creation fails halfway, delete `examples/chat_web_service` and run Step 1 again.
- If only one module has a syntax error, fix that file and rerun `mix compile`.
- If GraphiQL loads but subscriptions do not fire, re-check these three locations first:
  `application.ex` child list, `endpoint.ex` (`use Absinthe.Phoenix.Endpoint` + socket), and router `socket:` option in GraphiQL forward.

## Artifacts and Notes

Expected compile success line:

    Generated chat_web_service app

Expected query response shape:

    {
      "data": {
        "conversation": {
          "id": "c-1",
          "title": "General",
          "creator": {
            "id": "u-1",
            "displayName": "User u-1"
          }
        }
      }
    }

Expected mutation response shape:

    {
      "data": {
        "conversationCreate": {
          "conversation": {
            "id": "c-123",
            "title": "GraphQL Room"
          },
          "userErrors": []
        }
      }
    }

Expected subscription event shape in GraphiQL:

    {
      "data": {
        "conversationCreated": {
          "id": "c-123",
          "title": "GraphQL Room"
        }
      }
    }

## Interfaces and Dependencies

These modules and functions must exist when implementation is complete.

1. `ChatWebService.absinthe_schema/0`
2. `ChatWebService.absinthe_schema_notation/0`
3. `ChatWebService.__using__/1`
4. `ChatWebService.SchemaHelper.total_edges/2`
5. `ChatWebService.Schema`
6. `ChatWebService.Schema.Queries.Conversation`
7. `ChatWebService.Schema.Mutations.Conversation`
8. `ChatWebService.Schema.Subscriptions.Conversation`
9. `ChatWebService.Schema.Types.Conversation`
10. `ChatWebService.ConversationResolver.conversation/3`
11. `ChatWebService.ConversationResolver.conversations/3`
12. `ChatWebService.ConversationResolver.conversation_create/3`
13. `ChatWebService.ConversationResolver.creator/3`
14. `ChatWebService.ConversationResolver.messages/3`
15. `ChatWebService.ConversationResolver.participants/3`
16. `ChatWebService.UserSocket.connect/3`
17. `ChatWebService.UserSocket.id/1`
18. `ChatWebService.UserSocket.authorize/1`

Required dependencies in `mix.exs`:

1. `absinthe`
2. `absinthe_relay`
3. `absinthe_plug`
4. `absinthe_phoenix`
5. `gql_error_message`

Official documentation basis for the missing setup details used in this plan:

1. Absinthe Plug and Phoenix setup (`Absinthe.Plug`, router forwarding, GraphiQL).
2. Absinthe subscriptions setup (`Absinthe.Subscription`, `use Absinthe.Phoenix.Endpoint`, socket wiring, GraphiQL `socket:` option).
3. Absinthe Relay connection requirements (`connection field`, `connection node_type:`, and `edge do end`).

Official links:

1. https://hexdocs.pm/absinthe_plug/readme.html
2. https://hexdocs.pm/absinthe_phoenix/readme.html
3. https://hexdocs.pm/absinthe/subscriptions.html
4. https://hexdocs.pm/absinthe/plug-phoenix.html
5. https://hexdocs.pm/absinthe_relay/Absinthe.Relay.Connection.Notation.html

## Change Note

2026-03-04: Initial version created. It converts `.agent/ABSINTHE.md` naming conventions and module examples into an executable, beginner-safe, end-to-end implementation plan, and fills missing transport/subscription wiring using official Absinthe HexDocs guidance.
