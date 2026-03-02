defmodule EctoShorts.TestFragmentProvider do
  @moduledoc false

  import Ecto.Query

  def build_fragment_expression(_binding_selector, :active_users, params) do
    params = normalize_params(params)

    with {:ok, min_age} <- fetch_integer(params, :min_age) do
      {:ok, from(u in fragment("SELECT * FROM users WHERE age >= ?", ^min_age), select: u)}
    end
  end

  def build_fragment_expression(_binding_selector, :for_update, _params) do
    {:ok, fn query -> from(q in query, lock: "FOR UPDATE") end}
  end

  def build_fragment_expression(_binding_selector, :for_share, _params) do
    {:ok, fn query -> from(q in query, lock: "FOR SHARE") end}
  end

  def build_fragment_expression(_binding_selector, :post_window, _params) do
    {:ok, [partition_by: [:author_id], order_by: [desc: :inserted_at]]}
  end

  def build_fragment_expression(_binding_selector, :error_fragment, _params) do
    {:error, :forced_error}
  end

  def build_fragment_expression(_binding_selector, _source_key, _params) do
    {:error, :unsupported_fragment_key}
  end

  defp normalize_params(params) when is_map(params), do: Map.to_list(params())
  defp normalize_params(params), do: params

  defp fetch_integer(params, key) do
    case Keyword.get(params, key) do
      value when is_integer(value) -> {:ok, value}
      _ -> {:error, {:missing_or_invalid, key}}
    end
  end
end
