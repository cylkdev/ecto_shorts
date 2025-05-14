defmodule EctoShorts.DynamicExpression do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Defines a behavior and interface for building dynamic Ecto query
  expressions based on the adapter in use (e.g., Postgres).

  In Ecto, the `dynamic/2` macro is used to build flexible queries where
  parts of the expression depend on input at runtime . This module provides
  an API for building these expressions for different database backends.

  You can implement the `EctoShorts.DynamicExpression` behavior in your own
  adapter module to customize how dynamic conditions are generated based
  on things like schema, field name, and value type.

  ## Example

  Suppose you want to filter a field using Postgres-specific operators
  like `ILIKE`. You could write a custom adapter like:

      defmodule MyApp.DynamicPostgresAdapter do
        @behaviour EctoShorts.DynamicExpression

        def create_dynamic(dyn, binding, condition, schema, key, {:ilike, val}) do
          case condition do
            :and -> dynamic([q], ^dyn and ilike(field(^binding, ^key), ^val))
            :or -> dynamic([q], ^dyn or ilike(field(^binding, ^key), ^val))
          end
        end

        def create_dynamic(dyn, binding, condition, schema, key, val) do
          case condition do
            :and -> dynamic([q], ^dyn and field(^binding, ^key) == ^val)
            :or -> dynamic([q], ^dyn or field(^binding, ^key) == ^val)
          end
        end
      end

  Then you can call:

      EctoShorts.DynamicExpression.create_dynamic(
        MyApp.DynamicPostgresAdapter,
        nil,
        nil,
        :and,
        :name,
        {:ilike, "john"}
      )

  This returns a dynamic expression like:

      dynamic([q], ilike(q.name, ^"john")
  """

  alias EctoShorts.Config

  @type schema_module :: Ecto.Queryable.t()
  @type dynamic_expr :: %Ecto.Query.DynamicExpr{}
  @type maybe_dynamic_expr :: dynamic_expr() | nil
  @type binding_alias :: atom()

  @type condition :: :and | :or

  @type key :: atom()
  @type value :: any()
  @type opts :: keyword()

  @default_adapter EctoShorts.DynamicExpressions.Postgres

  @default_adapters [
    {Ecto.Adapters.Postgres, adapter: EctoShorts.DynamicExpressions.Postgres}
  ]

  @doc false
  @spec default_adapters :: list({module(), keyword()})
  def default_adapters, do: @default_adapters

  @doc """
  Defines a callback to implement custom logic for building a dynamic query expression.

  It receives:
    - `dyn`: the current dynamic expression being built or `nil`.
    - `binding_alias`: the binding index (e.g. 0 for the main schema)
    - `schema_module`: the module for the schema being queried
    - `key`: the field name (e.g. `:name`)
    - `value`: the filter value (e.g. a string or a special operator like `%{ilike: "foo"}`)

  Returns an updated dynamic expression.
  """
  @callback create_dynamic(
              schema_module(),
              maybe_dynamic_expr(),
              binding_alias() | nil,
              condition(),
              key(),
              value()
            ) :: dynamic_expr()

  @doc """
  Delegates to the adapter module’s `create_dynamic/5` function.

  This is the main entry point used by `EctoShorts` to apply adapter specific
  dynamic filter logic.

  ## Options

      * `:dynamic_expression_adapter` - Specifies the dynamic builder adapter
        to use.

      * `:dynamic_expression_adapters` - Specifies the dynamic builder adapter
        to associate with each ecto repo adapter in your application. This
        can be a enumerable of key-value pairs where the `key` is the repo
        adapter module and the value is a keyword list of options that must
        contain the `:adapter` key. (e.g. `[{Ecto.Adapters.Postgres, adapter: EctoShorts.DynamicExpressions.Postgres}]`).

  ## Examples

        iex> EctoShorts.DynamicExpression.create_dynamic(EctoShorts.Schema.Post, nil, nil, :and, :tags, {:==, "blog"}, [])
  """
  @spec create_dynamic(
          schema_module(),
          maybe_dynamic_expr(),
          binding_alias() | nil,
          condition(),
          key(),
          value(),
          opts()
        ) :: dynamic_expr()
  def create_dynamic(
        schema_module,
        dyn,
        binding_alias,
        condition,
        key,
        value,
        opts
      ) do
    adapter!(opts).create_dynamic(
      schema_module,
      dyn,
      binding_alias,
      condition,
      key,
      value
    )
  end

  defp adapter!(opts) do
    if Keyword.has_key?(opts, :dynamic_expression_adapter) do
      opts[:dynamic_expression_adapter]
    else
      case find_adapter_for_repo(Config.repo!(opts).__adapter__(), opts) do
        nil -> @default_adapter
        {_, adapter_config} -> Keyword.fetch!(adapter_config, :adapter)
      end
    end
  end

  defp find_adapter_for_repo(repo_adapter, opts) do
    opts
    |> dynamic_expression_adapters()
    |> Enum.find(fn {key, _} -> key === repo_adapter end)
  end

  defp dynamic_expression_adapters(opts) do
    opts[:dynamic_expression_adapters] ||
      Config.dynamic_expression_adapters() ||
      @default_adapters
  end
end
