defmodule EctoShorts.Dynamics do
  alias EctoShorts.Config

  def build_dynamic(source, selected_binding, term, opts) do
    adapter_for_repo!(opts).build_dynamic(source, selected_binding, term, opts)
  end

  defp adapter_for_repo!(opts) do
    if Keyword.has_key?(opts, :dynamic_adapter) do
      Keyword.fetch!(opts, :dynamic_adapter)
    else
      repo = opts[:repo] || opts[:replica] || Config.repo!(opts)

      case repo.__adapter__() do
        Ecto.Adapters.Postgres ->
          EctoShorts.Dynamics.Postgres

        Ecto.Adapters.MyXQL ->
          raise "Adapter not yet implemented: Ecto.Adapters.MyXQL"

        Ecto.Adapters.SQL ->
          raise "Adapter not yet implemented: Ecto.Adapters.SQL"

        Ecto.Adapters.Tds ->
          raise "Adapter not yet implemented: Ecto.Adapters.SQL"

        adapter ->
          raise "The adapter #{inspect(adapter)} is not supported. You must specify the option :dynamic_adapter..."
      end
    end
  end
end
