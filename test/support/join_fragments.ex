defmodule EctoShorts.TestJoinFragments do
  @moduledoc false

  import Ecto.Query

  alias EctoShorts.Schema.User

  def fragment(binding_selector, fragment_key, params) do
    build_fragment_source(fragment_key, params, binding_selector)
  end

  def build_fragment_source(:active_users, params, _binding_selector) do
    params = normalize_params(params)

    with {:ok, min_age} <- fetch_integer(params, :min_age) do
      {:ok, from(u in User, where: u.age >= ^min_age)}
    end
  end

  def build_fragment_source(:error_fragment, _params, _binding_selector) do
    {:error, :forced_error}
  end

  def build_fragment_source(_fragment_key, _params, _binding_selector) do
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
