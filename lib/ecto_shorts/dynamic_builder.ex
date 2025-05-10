defmodule EctoShorts.DynamicBuilder do
  @moduledoc """
  Defines a behavior and interface for building dynamic Ecto query expressions
  based on the adapter in use (e.g., Postgres).

  In Ecto, the `dynamic/2` macro is used to build flexible queries where
  parts of the expression depend on input at runtime . This module provides an
  API for building these expressions for different database backends.

  You can implement the `EctoShorts.DynamicBuilder` behavior in your own
  adapter module to customize how dynamic conditions are generated based
  on things like schema, field name, and value type.

  ## Example

  Suppose you want to filter a field using Postgres-specific operators like
  `ILIKE`. You could write a custom adapter like:

      defmodule MyApp.DynamicPostgresAdapter do
        @behaviour EctoShorts.DynamicBuilder

        def build_dynamic(dyn, binding, condition, schema, key, {:ilike, val}) do
          case condition do
            :and -> dynamic([q], ^dyn and ilike(field(^binding, ^key), ^val))
            :or -> dynamic([q], ^dyn or ilike(field(^binding, ^key), ^val))
          end
        end

        def build_dynamic(dyn, binding, condition, schema, key, val) do
          case condition do
            :and -> dynamic([q], ^dyn and field(^binding, ^key) == ^val)
            :or -> dynamic([q], ^dyn or field(^binding, ^key) == ^val)
          end
        end
      end

  Then you can call:

      EctoShorts.DynamicBuilder.build_dynamic(
        MyApp.DynamicPostgresAdapter,
        true,
        0,
        MyApp.User,
        :name,
        %{ilike: "john"}
      )

  This returns a dynamic expression like:

      dynamic([q], ilike(q.name, ^"john"))

  ## Understanding Ecto Query Composition with `dynamic/2`

  In this section, we cover how Ecto composes and validates queries
  using `dynamic/2`, focusing on the lifecycle of Ecto query building,
  macro expansion, and named binding resolution.

  Ecto query construction happens through macros like `from`, `where`,
  and `join`, which transform your Elixir code into a normalized `Ecto.Query`
  struct. During this macro expansion step, Ecto validates bindings (such as
  those created with `as(:binding)`) and rewrites expressions to ensure they
  are valid and safe before generating SQL. This step is often referred to as
  query normalization which is distinct from Elixir's own compilation and
  from query execution.

  The `Ecto.Query.dynamic/2` macro builds a dynamic query expression. It
  wraps the expression you provide in a `%Ecto.Query.DynamicExpr{}` struct.
  This struct holds a deferred function that will produce the quoted
  expression and its binding context when the query is normalized.

  For example:

      iex> import Ecto.Query
      ...> dyn = dynamic([], as(:post).id == 1)
      ...> is_struct(dyn, Ecto.Query.DynamicExpr)
      true

  This struct is used to delay evaluation, carry the quoted expression, and
  attach the binding context so that it can be safely injected into a query
  later.

  To see the expression it contains:

      iex> import Ecto.Query
      ...> dyn = dynamic([], as(:post).id == 1)
      ...> Macro.to_string(dyn)
      "dynamic([], as(:comments).id == 1)"

  or to see the normalized ecto expression after resolving bindings:

      iex> import Ecto.Query
      ...> query = from p in EctoShorts.Schema.Post
      ...> dyn = dynamic([p], p.id == 1)
      ...> {ast, _, _, _} = dyn.fun.(query)
      ...> Macro.to_string(ast)
      "&0.id() == %Ecto.Query.Tagged{tag: nil, type: {0, :id}, value: 1}"

  In short, `dynamic/2` allows you to construct reusable query fragments
  as data. These fragments are stored in the `%DynamicExpr{}` struct and
  later expanded as part of query normalization.

  The `Ecto.Query` struct itself is a container for all aspects of a query
  (e.g. sources, joins, filters, selects, and so on). Query macros like
  `from`, `where`, and `select` are used to modify or build this struct
  through macro expansion. Meanwhile, dynamic expressions represent
  deferred parts of a query that are validated and injected during
  query normalization.

  For example:

      iex> import Ecto.Query
      ...> dyn = dynamic([{:comments, c}], c.id == 1)
      ...> from c in EctoShorts.Schema.Comment, where: ^dyn
      ** (Ecto.QueryError) unknown bind name `:comments` in query

  This error occurs because the dynamic expression refers to a named
  binding `:comments`, but the query did not define one. When from is
  expanded, Ecto normalizes the query and attempts to resolve all named
  bindings finding that `:comments` was never declared.

  To fix this, you must declare the binding using as: `:comments`:

      iex> import Ecto.Query
      ...> dyn = dynamic([{:comments, c}], c.id == 1)
      ...> from c in EctoShorts.Schema.Comment, as: :comments, where: ^dyn
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, as: :comments, where: p0.id == 1>

  You can also use the `as/1` macro to reference a named binding:

      iex> dyn = dynamic([], as(:comments).id == 1)

  The `as/1` macro defers binding resolution by injecting a placeholder
  into the AST. Ecto typically validates the existence of that binding
  during query normalization which happens when macros like from or join
  are expanded. However, Ecto does not immediately validate the existence
  of the named binding when the `dynamic/2` is created.

  For example:

        iex> import Ecto.Query
        ...> dynamic(as(:does_not_exist).id == 1)

  If that dynamic expression is injected into a query via pin (`^dyn`), the
  validation is deferred until the query is compiled to SQL - for example,
  when passed to `Repo.all/1` or another Repo function. If `:comments` is
  not declared by that point, Ecto will raise an error during SQL generation.
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

  @default_adapter EctoShorts.DynamicBuilders.Postgres

  @adapters [
    {Ecto.Adapters.Postgres, adapter: EctoShorts.DynamicBuilders.Postgres}
  ]

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
  @callback build_dynamic(
              schema_module(),
              maybe_dynamic_expr(),
              binding_alias(),
              condition(),
              key(),
              value()
            ) :: dynamic_expr()

  @doc """
  Delegates to the adapter module’s `build_dynamic/5` function.

  This is the main entry point used by `EctoShorts` to apply adapter-specific
  dynamic filter logic.

  ## Examples

        iex> EctoShorts.DynamicBuilder.build_dynamic(nil, nil, EctoShorts.Schema.Post, :tags, {:==, "blog"})
        dynamic([q], ^"blog" in q.tags)
  """
  @spec build_dynamic(
          schema_module(),
          maybe_dynamic_expr(),
          binding_alias(),
          condition(),
          key(),
          value(),
          opts()
        ) :: dynamic_expr()
  def build_dynamic(
        schema_module,
        dyn,
        binding_alias,
        condition,
        key,
        value,
        opts
      ) do
    adapter_for_repo!(opts).build_dynamic(
      schema_module,
      dyn,
      binding_alias,
      condition,
      key,
      value
    )
  end

  defp adapter_for_repo!(opts) do
    if Keyword.has_key?(opts, :dynamic_adapter) do
      opts[:dynamic_adapter]
    else
      case get_repo_dynamic_adapter(Config.repo!(opts).__adapter__(), opts) do
        nil -> @default_adapter
        {_, adapter_config} -> Keyword.fetch!(adapter_config, :adapter)
      end
    end
  end

  defp get_repo_dynamic_adapter(repo_adapter, opts) do
    opts
    |> dynamic_adapters()
    |> Enum.find(fn {key, _} -> key === repo_adapter end)
  end

  defp dynamic_adapters(opts) do
    opts[:dynamic_adapters] ||
      Config.dynamic_adapters() ||
      @adapters
  end
end
