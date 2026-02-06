defmodule EctoShorts.Actions.Preloader do
  # alias EctoShorts.Actions.Batch
  # alias EctoShorts.CommonSchema
  # alias EctoShorts.CommonFilters
  # alias EctoShorts.Utils

  # import Ecto.Query

  # @doc """
  # EctoShorts.Repo.start_link()
  # EctoShorts.Actions.all(EctoShorts.Schema.Post)
  # EctoShorts.Actions.all(EctoShorts.Schema.User)
  # EctoShorts.Actions.Preloader.load_associations(EctoShorts.Schema.Post, :author, [%{id: 1}], [])
  # EctoShorts.Actions.Preloader.load_associations(EctoShorts.Schema.Post, :authors, [%{id: 1}], [])
  # """
  # def load_associations(source, assoc_field, params_list, _opts \\ []) do
  #   source
  #   |> fetch_schema!()
  #   |> fetch_assoc!(assoc_field)
  #   |> normalize_operation()
  #   |> prepare_batch_params(params_list)
  #   # primary_key = get_primary_key(opts, assoc_schema)
  #   # Batch.batch(assoc_schema, params_list, primary_key, :many, opts)
  # end

  # # def batch(schema, params, batch_keys, cardinality, opts)

  # defp prepare_batch_params({:one, _queryable, related_key}, params_list) do
  #   Enum.map(params_list, &%{related_key => Utils.enum_fetch!(&1, related_key)})
  # end

  # defp parepare_batch_params(
  #   {
  #     :many,
  #     queryable,
  #     {
  #       {join_schema, field},
  #       {owner_join_key, owner_key},
  #       {related_join_key, related_key}
  #     }
  #   },
  #   params_list
  # ) do
  #   from a in queryable,
  #     join: j in ^join_schema,
  #     on: field(j, ^related_join_key) == field(a, ^related_key),
  #     where: field(j, ^owner_join_key) in [1, 2, 3]
  # end

  # defp fetch_schema!(source) do
  #   {_, schema} = fetch_schema_source!(source)
  #   schema
  # end

  # defp fetch_schema_source!(source) do
  #   case CommonSchema.get_schema_source(source) do
  #     {table_name, schema} when is_atom(schema) and not is_nil(schema) ->
  #       {table_name, schema}

  #     _ ->
  #       raise ArgumentError,
  #             "Expected source to contain a schema module, got: #{inspect(source)}"
  #   end
  # end

  # defp normalize_operation(
  #   %Ecto.Association.BelongsTo{
  #     queryable: queryable,
  #     cardinality: cardinality,
  #     related_key: related_key
  #   }
  # ) do
  #   {cardinality, queryable, related_key}
  # end

  # defp normalize_operation(
  #   %Ecto.Association.ManyToMany{
  #     queryable: queryable,
  #     cardinality: cardinality,
  #     field: field,
  #     join_through: join_schema,
  #     join_keys: [
  #       {owner_join_key, owner_key},
  #       {related_join_key, related_key}
  #     ]
  #   }
  # ) do
  #   {cardinality, queryable, {{join_schema, field}, {owner_join_key, owner_key}, {related_join_key, related_key}}}
  # end

  # defp fetch_assoc!(parent_schema, assoc_field) do
  #   case parent_schema.__schema__(:association, assoc_field) do
  #     nil ->
  #       raise ArgumentError,
  #             "The field #{assoc_field} is not an association on schema #{inspect(parent_schema)}"

  #     assoc ->
  #       assoc
  #   end
  # end
end
