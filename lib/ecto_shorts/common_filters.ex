defmodule EctoShorts.CommonFilters do
  @moduledoc """
  Data-driven query composition for Ecto.

  `EctoShorts.CommonFilters` provides a declarative interface for building
  Ecto queries using maps or keyword lists. It enables dynamic query generation
  without requiring direct use of Ecto’s query DSL.

  This reduces boilerplate and makes it easier to build data-driven APIs where
  filters are derived from user input or external sources.

  For example, rather than writing:

  ```elixir
  from p in Post,
    where: p.id == 1,
    join: c in assoc(p, :comments),
    where: c.body == "example"
  ```

  You can represent the same logic with:

  ```elixir
  %{id: 1, comments: %{body: "example"}}
  ```

  and pass it to convert_params_to_filter/2.

  ## Example Usage

  Find a post by ID:

  ```elixir
  iex> EctoShorts.CommonFilters.convert_params_to_filter(MyApp.Post, %{id: 1})
  #Ecto.Query<from p in MyApp.Post, where: p.id == 1>
  ```

  Join on an association and filter nested fields:

  ```elixir
  iex> EctoShorts.CommonFilters.convert_params_to_filter(MyApp.Post, %{
  ...>   id: 1,
  ...>   comments: %{id: [1, 2], body: %{ilike: "example"}}
  ...> })
  #Ecto.Query<from p in MyApp.Post,
    join: c in assoc(p, :comments),
    where: p.id == 1 and c.id in [1, 2]
      and ilike(c.body, "%example%")>
  ```

  ## Filters

  The filter API is split into two components:

    * `EctoShorts.QueryBuilders.Common` — Handles common filters like
    pagination, ordering, and range-based conditions.

    * `EctoShorts.QueryBuilders.Schema` — Handles schema-specific filters and
    joins on associations or subqueries.
  """

  alias EctoShorts.{
    CommonSchemas,
    QueryBuilder,
    QueryBuilders.Common,
    QueryBuilders.Schema
  }

  @type prefix :: binary()
  @type query :: Ecto.Query.t()
  @type schema_module :: Ecto.Queryable.t()
  @type schema_source :: binary()
  @type sourceable :: schema_module() | {schema_source(), schema_module()}
  @type query_source :: query_source()
  @type binding_alias :: atom()
  @type key :: atom()
  @type value :: any()
  @type params :: keyword() | map()
  @type opts :: keyword()

  @behaviour EctoShorts.QueryBuilder

  @default_query_builder_adapter __MODULE__

  @common_filters Common.filters()
  @schema_filters Schema.filters()
  @filters @common_filters ++ @schema_filters

  @doc """
  Converts a map or keyword list of parameters into an Ecto query.
  """
  @spec convert_params_to_filter(
          query_source() | nil,
          params(),
          opts()
        ) :: query_source()
  def convert_params_to_filter(query_source, params, _opts)
      when params === %{} or params === [] do
    query_source
  end

  def convert_params_to_filter(query_source, params, opts) when is_map(params) do
    convert_params_to_filter(query_source, Map.to_list(params), opts)
  end

  def convert_params_to_filter(query_source, params, opts) do
    base = Keyword.take(params, [:as, :query])

    query_source =
      case base[:query] do
        nil -> query_source
        base_query -> base_query
      end

    if is_nil(query_source) do
      raise ArgumentError,
            "A query must be provided either as the query argument or as the :query key in params. Both cannot be nil."
    end

    binding_alias = base[:as]

    schema_module = CommonSchemas.module_for_schema(query_source)

    params =
      params
      |> Keyword.drop([:as, :query])
      |> ensure_last_is_final_filter()

    reduce_filters(query_source, binding_alias, schema_module, params, opts)
  end

  defp reduce_filters(query_source, binding_alias, schema_module, params, opts) do
    Enum.reduce(params, query_source, fn {key, value}, query_source ->
      reduce_filter(query_source, binding_alias, schema_module, key, value, opts)
    end)
  end

  defp reduce_filter(query_source, binding_alias, schema_module, key, value, opts) do
    with query_source <-
           maybe_apply_exported_filter(
             query_source,
             binding_alias,
             schema_module,
             key,
             value
           ) do
      opts
      |> query_builder_adapter()
      |> QueryBuilder.build_query(
        query_source,
        binding_alias,
        schema_module,
        key,
        value,
        opts
      )
    end
  end

  defp maybe_apply_exported_filter(query_source, binding_alias, schema_module, key, value) do
    if schema_exported_filter?(schema_module, key) do
      if function_exported?(schema_module, :build_query_source, 4) do
        schema_module.build_query(
          query_source,
          binding_alias,
          key,
          value
        )
      else
        EctoShorts.Utils.Logger.warning(
          __MODULE__,
          "callback function build_query/4 not found in schema module #{inspect(schema_module)} for filter: #{inspect(key)}"
        )

        query_source
      end
    else
      query_source
    end
  end

  defp schema_exported_filter?(schema_module, key) do
    schema_exported_filters?(schema_module) and key in schema_module.filters()
  end

  defp schema_exported_filters?(schema_module) do
    function_exported?(schema_module, :filters, 0)
  end

  defp query_builder_adapter(opts) do
    opts[:query_builder_adapter] ||
      EctoShorts.Config.query_builder_adapter() ||
      @default_query_builder_adapter
  end

  @impl EctoShorts.QueryBuilder
  @doc since: "2.5.0"
  @doc """
  ...
  """
  def filters, do: @filters

  @doc since: "2.5.0"
  @doc """
  ...
  """
  def filters(opts) do
    opts
    |> query_builder_adapter()
    |> QueryBuilder.filters()
  end

  @impl EctoShorts.QueryBuilder
  @doc since: "2.5.0"
  @doc """
  Builds a query based on a single filter key and value.

  The schema module given as an argument must be the schema module
  for the query being targeted by the binding_alias argument.
  If the binding_alias is nil then the binding is the root query
  and the schema module must be for the root query. If the binding_alias
  given is an association then the schema module must be for that
  association schema module.

  ## Examples

      iex> build_query(Post, :post, Post, :limit, 10)
      #Ecto.Query<from p in Post, limit: 10>

      iex> build_query(Post, :post, Post, :title, "Hello")
      #Ecto.Query<from p in Post, where: p.title == "Hello">
  """
  @spec build_query(
          query_source(),
          binding_alias(),
          schema_module(),
          key(),
          value()
        ) :: query_source()
  @spec build_query(
          query_source(),
          binding_alias(),
          schema_module(),
          key(),
          value(),
          opts()
        ) :: query_source()
  def build_query(query_source, binding_alias, schema_module, key, value, opts \\ [])

  def build_query(query_source, binding_alias, schema_module, key, value, opts)
      when key in @common_filters do
    QueryBuilder.build_query(
      Common,
      query_source,
      binding_alias,
      schema_module,
      key,
      value,
      opts
    )
  end

  def build_query(query_source, binding_alias, schema_module, key, value, opts) do
    QueryBuilder.build_query(
      Schema,
      query_source,
      binding_alias,
      schema_module,
      key,
      value,
      opts
    )
  end

  defp ensure_last_is_final_filter(params) do
    if Keyword.has_key?(params, :last) do
      params
      |> Keyword.delete(:last)
      |> Kernel.++(last: params[:last])
    else
      params
    end
  end
end
