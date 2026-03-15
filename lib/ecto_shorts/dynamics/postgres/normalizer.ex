defmodule EctoShorts.Dynamics.Postgres.Normalizer do
  @moduledoc """
  Input normalization for the Postgres dynamic expression adapter.

  This module converts the flexible filter term shapes accepted by the
  public EctoShorts filter API into a canonical internal representation
  before expression building begins. The caller (`Dynamics.Adapters.Postgres`)
  calls `normalize_params/1` to flatten and canonicalize the raw filter
  value, then dispatches each normalized entry to the appropriate
  expression builder.

  ## Supported input shapes

  Filter values may arrive in many shapes depending on how the caller
  constructed the params map:

    * Plain scalar values — passed through unchanged.
    * Maps (`%{field: value}`) — converted to keyword list form.
    * Keyword lists — iterated; each entry is normalized recursively.
    * Quantifier tuples (`{:all, payload}`, `{:any, payload}`) — kept
      as-is so the expression builder can handle subquery expansion.
    * Arithmetic value tuples (`{:+, [left, right]}`) — converted to
      `{op, {normalized_left, normalized_right}}`.
    * Datetime wrapper tuples (`{:datetime, payload}`, `{:date, payload}`)
      — payload is normalized and rewrapped.
    * Field/value marker tuples (`{:field, name}`, `{:value, v}`) —
      field name is atomized; value is normalized recursively.

  ## Value node normalization

  `normalize_value_node/1` converts a single value node into its
  canonical form. It is called recursively by `normalize_params/1` and
  by itself for nested structures.
  """

  @quantifier_operators [:all, :any]
  @arithmetic_value_operators [:+, :-, :*, :/]
  @datetime_wrappers [:datetime, :date]
  @datetime_value_operators [:add, :ago, :from_now]

  @doc """
  Normalizes a raw filter term into a flat list of canonical entries.

  Each element of the returned list is one of:

    * `{key, value}` — a field/operator pair ready for expression building.
    * `{:and, {key, value}}` / `{:or, {key, value}}` — a merge-operator
      tagged pair produced by `normalize_keyword_params/2`.
    * `{quantifier, payload}` — a quantifier operator pair.

  ## Examples

      iex> Normalizer.normalize_params(%{views: 5})
      [{:views, 5}]

      iex> Normalizer.normalize_params([views: %{>: 10}])
      [{:views, {:%{}, [], [>: 10]}}]

      iex> Normalizer.normalize_params(true)
      [true]
  """
  @spec normalize_params(term()) :: list()
  def normalize_params(term) do
    cond do
      is_map(term) and not is_struct(term) ->
        term
        |> Map.to_list()
        |> normalize_params()

      Keyword.keyword?(term) ->
        normalize_keyword_params(term, [])

      true ->
        [normalize_value_node(term)]
    end
  end

  @doc """
  Normalizes a keyword list of filter params into a flat list.

  Entries that are quantifier operators or arithmetic/datetime wrappers
  are preserved with their structure. Other entries are recursively
  normalized by calling `normalize_params/1` on their value, then each
  resulting entry is re-paired with the original key.
  """
  @spec normalize_keyword_params(list(), list()) :: list()
  def normalize_keyword_params([], acc), do: Enum.reverse(acc)

  def normalize_keyword_params([head | tail], acc) do
    with acc2 <- normalize_keyword_params(head, acc) do
      normalize_keyword_params(tail, acc2)
    end
  end

  def normalize_keyword_params({quantifier, payload}, acc)
      when quantifier in @quantifier_operators do
    [{quantifier, payload} | acc]
  end

  def normalize_keyword_params({op, inner_term}, acc) when op in @arithmetic_value_operators do
    [normalize_value_node({op, inner_term}) | acc]
  end

  def normalize_keyword_params({wrapper, inner_term}, acc) when wrapper in @datetime_wrappers do
    [normalize_value_node({wrapper, inner_term}) | acc]
  end

  def normalize_keyword_params({key, inner_term}, acc) do
    prepend_key(key, normalize_params(inner_term), acc)
  end

  @doc """
  Normalizes a single value node into its canonical form.

  Handles the full range of value shapes: plain scalars, maps, keyword
  lists with `:field`/`:value` markers, arithmetic expressions, datetime
  wrappers, and datetime operation nodes.

  ## Examples

      iex> Normalizer.normalize_value_node({:field, "inserted_at"})
      {:field, :inserted_at}

      iex> Normalizer.normalize_value_node({:+, [1, 2]})
      {:+, {1, 2}}

      iex> Normalizer.normalize_value_node(:something)
      :something
  """
  @spec normalize_value_node(term()) :: term()
  def normalize_value_node(term) when is_map(term) and not is_struct(term) do
    term
    |> Map.to_list()
    |> normalize_value_node()
  end

  def normalize_value_node({:field, field_name}) do
    {:field, normalize_field_name(field_name)}
  end

  def normalize_value_node({:value, value}) do
    {:value, normalize_value_node(value)}
  end

  def normalize_value_node({op, term}) when op in @arithmetic_value_operators do
    case term do
      [left, right] ->
        {op, {normalize_value_node(left), normalize_value_node(right)}}

      _ ->
        raise ArgumentError,
              "Expected arithmetic operator value to be a two-element list [left, right], got: #{inspect(term)}"
    end
  end

  def normalize_value_node({wrapper, term}) when wrapper in @datetime_wrappers do
    case normalize_datetime_wrapper_payload(term) do
      {datetime_op, datetime_term} when datetime_op in @datetime_value_operators ->
        {wrapper, {datetime_op, datetime_term}}

      _ ->
        raise ArgumentError,
              "Expected datetime wrapper payload to be a keyword or map with a datetime operation key (:add, :ago, :from_now), got: #{inspect(term)}"
    end
  end

  def normalize_value_node({op, term}) when op in @datetime_value_operators do
    {op, normalize_datetime_node(term)}
  end

  def normalize_value_node(field: field_name) do
    {:field, normalize_field_name(field_name)}
  end

  def normalize_value_node(value: value) do
    {:value, normalize_value_node(value)}
  end

  def normalize_value_node([]) do
    []
  end

  def normalize_value_node([head | tail]) do
    [normalize_value_node(head) | normalize_value_node(tail)]
  end

  def normalize_value_node(term), do: term

  @doc """
  Normalizes a datetime wrapper payload (the value inside `{:datetime, ...}`
  or `{:date, ...}`).

  Accepts a map or a single-entry keyword list whose key is a datetime
  operation (`:add`, `:ago`, `:from_now`). Returns the normalized
  `{op, term}` pair.

  Raises `ArgumentError` for unrecognized shapes.
  """
  @spec normalize_datetime_wrapper_payload(term()) :: {atom(), term()}
  def normalize_datetime_wrapper_payload(term) when is_map(term) and not is_struct(term) do
    term
    |> Map.to_list()
    |> normalize_datetime_wrapper_payload()
  end

  def normalize_datetime_wrapper_payload(term) when is_list(term) do
    if Keyword.keyword?(term) do
      case term do
        [{datetime_op, datetime_term}] when datetime_op in @datetime_value_operators ->
          normalize_value_node({datetime_op, datetime_term})

        _ ->
          raise ArgumentError,
                "Expected datetime wrapper payload to be a single-key keyword list with one of #{inspect(@datetime_value_operators)}, got: #{inspect(term)}"
      end
    else
      raise ArgumentError,
            "Expected datetime wrapper payload to be a keyword list or map, got: #{inspect(term)}"
    end
  end

  @doc """
  Normalizes a datetime operation node (`{:add, ...}`, `{:ago, ...}`,
  `{:from_now, ...}`).

  Accepts a map or keyword list with `:count`, `:interval`, and
  optionally `:field` keys. Returns a keyword list in canonical order.

  Raises `ArgumentError` if the input is not a keyword list or map.
  """
  @spec normalize_datetime_node(term()) :: keyword()
  def normalize_datetime_node(term) when is_map(term) and not is_struct(term) do
    term
    |> Map.to_list()
    |> normalize_datetime_node()
  end

  def normalize_datetime_node(term) when is_list(term) do
    if Keyword.keyword?(term) do
      field_name = Keyword.get(term, :field)
      count = Keyword.fetch!(term, :count)
      interval = Keyword.fetch!(term, :interval)

      []
      |> maybe_put_datetime_field(field_name)
      |> Kernel.++(count: count, interval: interval)
    else
      raise ArgumentError, "Expected datetime params to be a keyword list or map, got: #{inspect(term)}"
    end
  end

  @doc """
  Normalizes a field name to an atom.

  Accepts an atom (passed through) or a binary string (converted with
  `String.to_existing_atom/1`).

  Raises `ArgumentError` if the string does not correspond to an
  existing atom.
  """
  @spec normalize_field_name(atom() | binary()) :: atom()
  def normalize_field_name(field_name) when is_atom(field_name), do: field_name
  def normalize_field_name(field_name) when is_binary(field_name), do: String.to_existing_atom(field_name)

  defp prepend_key(_key, [], acc), do: acc

  defp prepend_key(key, [normalized_term | rest], acc) do
    prepend_key(key, rest, [{key, normalized_term} | acc])
  end

  defp maybe_put_datetime_field(params, nil), do: params
  defp maybe_put_datetime_field(params, field_name), do: [{:field, normalize_field_name(field_name)} | params]
end
