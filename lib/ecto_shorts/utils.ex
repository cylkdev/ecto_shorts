defmodule EctoShorts.Utils do
  @moduledoc since: "3.0.0"
  @moduledoc """
  General-purpose utility functions used internally by EctoShorts.

  Provides `atomize_keys/1` for recursively converting string keys in maps
  and keyword lists to existing atoms.
  """

  def map_to_list(map) when is_map(map) and not is_struct(map) do
    map
    |> Map.to_list()
    |> map_to_list()
  end

  def map_to_list([]) do
    []
  end

  def map_to_list([head | tail]) do
    [map_to_list(head) | map_to_list(tail)]
  end

  def map_to_list({k, v}) do
    {k, map_to_list(v)}
  end

  def map_to_list(term) do
    term
  end

  def atomize_keys(enum) do
    transform_keys(enum, fn
      {key, value} when is_binary(key) ->
        atom_key =
          try do
            String.to_existing_atom(key)
          rescue
            ArgumentError ->
              key
          end

        {atom_key, value}

      other ->
        other
    end)
  end

  defp transform_keys({key, value}, fun) do
    {fun.(key), transform_keys(value, fun)}
  end

  defp transform_keys([head | tail], fun) do
    [transform_keys(head, fun) | transform_keys(tail, fun)]
  end

  defp transform_keys(params, fun) when is_map(params) do
    params
    |> Map.to_list()
    |> transform_keys(fun)
    |> Map.new()
  end

  defp transform_keys(value, _fun) do
    value
  end
end
