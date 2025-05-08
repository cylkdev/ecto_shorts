defmodule EctoShorts.DynamicExpression do
  @moduledoc """
  Defines a behavior and interface for building dynamic Ecto query expressions
  based on the adapter in use (e.g., Postgres).

  In Ecto, the `dynamic/2` macro is used to build flexible queries where
  parts of the expression depend on runtime input. This module allows
  defining custom logic to build those expressions for different database
  backends.

  You can implement the `EctoShorts.DynamicExpression` behavior in your own
  adapter module to customize how dynamic conditions are generated based
  on things like schema, field name, and value type.

  ## Example

  Suppose you want to filter a field using Postgres-specific operators like
  `ILIKE`. You could write a custom adapter like:

      defmodule MyApp.DynamicPostgresAdapter do
        @behaviour EctoShorts.DynamicExpression

        def build_dynamic_expression(dyn, binding, condition, schema, key, {:ilike, val}) do
          case condition do
            :and -> dynamic([q], ^dyn and ilike(field(^binding, ^key), ^val))
            :or -> dynamic([q], ^dyn or ilike(field(^binding, ^key), ^val))
          end
        end

        def build_dynamic_expression(dyn, binding, condition, schema, key, val) do
          case condition do
            :and -> dynamic([q], ^dyn and field(^binding, ^key) == ^val)
            :or -> dynamic([q], ^dyn or field(^binding, ^key) == ^val)
          end
        end
      end

  Then you can call:

      EctoShorts.DynamicExpression.build_dynamic_expression(
        MyApp.DynamicPostgresAdapter,
        true,
        0,
        MyApp.User,
        :name,
        %{ilike: "john"}
      )

  This returns a dynamic expression like:

      dynamic([q], ilike(q.name, ^"john"))
  """

  alias EctoShorts.Config

  @type adapter :: module()
  @type dyn :: Ecto.Query.DynamicExpr.t()
  @type binding_alias :: atom()
  @type queryable :: Ecto.Queryable.t()
  @type key :: atom()
  @type value :: any()
  @type opts :: keyword()
  @type condition :: atom()

  @adapters [
    {Ecto.Adapters.Postgres, adapter: EctoShorts.DynamicExpression.Postgres}
  ]

  @doc """
  Defines a callback to implement custom logic for building a dynamic query expression.

  It receives:
    - `dyn`: the current dynamic expression being built or `nil`.
    - `current_binding`: the binding index (e.g. 0 for the main schema)
    - `schema_module`: the module for the schema being queried
    - `key`: the field name (e.g. `:name`)
    - `value`: the filter value (e.g. a string or a special operator like `%{ilike: "foo"}`)

  Returns an updated dynamic expression.
  """
  @callback build_dynamic_expression(
              dyn() | nil,
              binding_alias() | nil,
              condition(),
              queryable(),
              key(),
              value()
            ) :: dyn()

  @doc """
  Delegates to the adapter module’s `build_dynamic_expression/5` function.

  This is the main entry point used by `EctoShorts` to apply adapter-specific
  dynamic filter logic.

  ## Examples

        iex> EctoShorts.DynamicExpression.build_dynamic_expression(nil, nil, EctoShorts.Schema.Post, :tags, {:==, "blog"})
        dynamic([q], ^"blog" in q.tags)
  """
  @spec build_dynamic_expression(
          dyn() | nil,
          binding_alias() | nil,
          condition(),
          queryable(),
          key(),
          value(),
          opts()
        ) :: dyn()
  def build_dynamic_expression(
        dyn,
        current_binding,
        condition,
        schema_module,
        key,
        value,
        opts \\ []
      ) do
    adapter!(opts).build_dynamic_expression(
      dyn,
      current_binding,
      condition,
      schema_module,
      key,
      value
    )
  end

  defp adapter!(opts) do
    repo = Config.repo!(opts)

    if Keyword.has_key?(opts, :dynamic_expression_adapter) do
      opts[:dynamic_expression_adapter]
    else
      repo_adapter = repo.__adapter__()

      case find_dynamic_expression_adapter(repo_adapter, opts) do
        nil ->
          raise "The adapter '#{inspect(repo_adapter)}' configured " <>
                  "for the repo '#{inspect(repo)}' is not supported. " <>
                  "Specify an adapter via the ':dynamic_expression_adapter' or " <>
                  "':dynamic_expression_adapters' option."

        {_, adapter_config} ->
          adapter_config
          |> Map.new()
          |> Map.fetch!(:adapter)
      end
    end
  end

  defp find_dynamic_expression_adapter(repo_adapter, opts) do
    opts
    |> adapters()
    |> Enum.find(fn {key, _} -> key === repo_adapter end)
  end

  defp adapters(opts) do
    opts[:dynamic_expression_adapters] ||
      EctoShorts.Config.dynamic_expression_adapters() ||
      @adapters
  end
end
