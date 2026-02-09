defmodule EctoShorts.TestQuerySourceProvider do
  @moduledoc false

  import Ecto.Query

  def resolve_expression(binding_selector, source_key, params) do
    build_resolve_expression(source_key, params, binding_selector)
  end

  def build_resolve_expression(:active_users, params, _binding_selector) do
    params = normalize_params(params)

    with {:ok, min_age} <- fetch_integer(params, :min_age) do
      {:ok, from(u in fragment("SELECT * FROM users WHERE age >= ?", ^min_age), select: u)}
    end
  end

  def build_resolve_expression(:for_update, _params, _binding_selector) do
    {:ok, fn query -> from(q in query, lock: "FOR UPDATE") end}
  end

  def build_resolve_expression(:for_share, _params, _binding_selector) do
    {:ok, fn query -> from(q in query, lock: "FOR SHARE") end}
  end

  def build_resolve_expression(:post_window, _params, _binding_selector) do
    {:ok, [partition_by: [:author_id], order_by: [desc: :inserted_at]]}
  end

  def build_resolve_expression(:error_fragment, _params, _binding_selector) do
    {:error, :forced_error}
  end

  def build_resolve_expression(_source_key, _params, _binding_selector) do
    {:error, :unsupported_fragment_key}
  end

  defp normalize_params(params) when is_map(params), do: Map.to_list(params)
  defp normalize_params(params), do: params

  defp fetch_integer(params, key) do
    case Keyword.get(params, key) do
      value when is_integer(value) -> {:ok, value}
      _ -> {:error, {:missing_or_invalid, key}}
    end
  end
end
