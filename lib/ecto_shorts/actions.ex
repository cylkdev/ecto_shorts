defmodule EctoShorts.Actions do
  alias EctoShorts.Actions.{
    Batch,
    CRUD,
    Bulk,
    Transaction,
    Multi
  }

  def preload(data, preloads, opts \\ []), do: CRUD.preload(data, preloads, opts)
  def exists?(source, params, opts \\ []), do: CRUD.exists?(source, params, opts)

  def all(source), do: CRUD.all(source)
  def all(source, params_or_opts), do: CRUD.all(source, params_or_opts)
  def all(source, params, opts), do: CRUD.all(source, params, opts)

  def create(schema, params, opts \\ []) do
    CRUD.create(schema, params, opts)
  end

  def get(queryable, id, opts \\ []) do
    CRUD.get(queryable, id, opts)
  end

  def find(source, params, opts \\ []) do
    CRUD.find(source, params, opts)
  end

  def update(queryable, id_or_schema_data, params, opts \\ []) do
    CRUD.update(queryable, id_or_schema_data, params, opts)
  end

  def delete(structs_or_changesets) do
    delete(structs_or_changesets, [])
  end

  def delete(structs_or_changesets, opts) do
    CRUD.delete(structs_or_changesets, opts)
  end

  def delete(queryable, id, opts) do
    CRUD.delete(queryable, id, opts)
  end

  def stream(queryable, params \\ %{}, opts \\ []) do
    CRUD.stream(queryable, params, opts)
  end

  def aggregate(queryable, params \\ %{}, aggregate \\ :count, key \\ :id, opts \\ []) do
    CRUD.aggregate(queryable, params, aggregate, key, opts)
  end

  def find_and_create(queryable, find_params, create_params, opts \\ []) do
    CRUD.find_and_create(queryable, find_params, create_params, opts)
  end

  def find_and_update(source, find_params, update_params, opts \\ []) do
    CRUD.find_and_update(source, find_params, update_params, opts)
  end

  def find_and_upsert(source, find_params, upsert_params, opts \\ []) do
    CRUD.find_and_upsert(source, find_params, upsert_params, opts)
  end

  def find_and_delete(source, find_params, opts \\ []) do
    CRUD.find_and_delete(source, find_params, opts)
  end

  def find_or_create(queryable, params, opts \\ []) do
    CRUD.find_or_create(queryable, params, opts)
  end

  def transaction(fun_or_multi, opts \\ []) do
    Transaction.transaction(fun_or_multi, opts)
  end

  def transact(fun_or_multi, opts \\ []) do
    Transaction.transact(fun_or_multi, opts)
  end

  def batch(schema, args, batch_keys \\ :id, cardinality \\ :many, opts \\ []) do
    Batch.batch(schema, args, batch_keys, cardinality, opts)
  end

  def batch_preload(schema, entries, keys, opts \\ []) do
    Batch.batch_preload(schema, entries, keys, opts)
  end

  def insert_all(schema, params_list, opts \\ []) do
    Bulk.insert_all(schema, params_list, opts)
  end

  def update_all(schema, find_params, update_params, opts \\ []) do
    Bulk.update_all(schema, find_params, update_params, opts)
  end

  def delete_all(schema, params_list, opts \\ []) do
    Bulk.delete_all(schema, params_list, opts)
  end

  def create_many(schema, params_list, opts \\ []) do
    Multi.create_many(schema, params_list, opts)
  end

  def find_many(schema, params_list, opts \\ []) do
    Multi.find_many(schema, params_list, opts)
  end

  def update_many(schema, entries, opts \\ []) do
    Multi.update_many(schema, entries, opts)
  end

  def delete_many(schema, records, opts \\ []) do
    Multi.delete_many(schema, records, opts)
  end

  def find_or_create_many(schema, params_list, opts \\ []) do
    Multi.find_or_create_many(schema, params_list, opts)
  end

  def find_and_upsert_many(schema, entries, opts \\ []) do
    Multi.find_and_upsert_many(schema, entries, opts)
  end
end
