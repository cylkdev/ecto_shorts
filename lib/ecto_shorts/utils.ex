defmodule EctoShorts.Utils do
  @moduledoc false

  def key_values?(m) when is_map(m) and not is_struct(m), do: true
  def key_values?([]), do: true
  def key_values?([{_, _} | rest]), do: key_values?(rest)
  def key_values?(_), do: false

  def enum_has_key?(map, key) when is_map(map) do
    Map.has_key?(map, key)
  end

  def enum_has_key?(list, key) do
    Enum.any?(list, fn
      {^key, _v} -> true
      _ -> false
    end)
  end

  def enum_get(map, key, default) when is_map(map) do
    Map.get(map, key, default)
  end

  def enum_get([], _key, default), do: default

  def enum_get(list, key, default) when is_list(list) do
    case :lists.keyfind(key, 1, list) do
      false -> default
      {_, v} -> v
    end
  end

  def enum_drop(map, keys) when is_map(map) do
    Map.drop(map, keys)
  end

  def enum_drop(list, keys) do
    Enum.reject(list, fn {key, _} -> key in keys end)
  end

  def enum_take(map, keys) when is_map(map) do
    Map.take(map, keys)
  end

  def enum_take(list, keys) do
    Enum.filter(list, fn {key, _} -> key in keys end)
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
