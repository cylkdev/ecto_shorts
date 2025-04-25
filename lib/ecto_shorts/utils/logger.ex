defmodule EctoShorts.Utils.Logger do
  @moduledoc false
  require Logger

  @doc false
  @spec debug(identifier :: atom() | binary(), message :: binary()) :: :ok
  @spec debug(identifier :: atom() | binary(), message :: binary(), opts :: keyword()) :: :ok
  def debug(identifier, message, opts \\ []) do
    identifier
    |> format_message(message, opts)
    |> Logger.debug(opts)
  end

  @doc false
  @spec info(identifier :: atom() | binary(), message :: binary()) :: :ok
  @spec info(identifier :: atom() | binary(), message :: binary(), opts :: keyword()) :: :ok
  def info(identifier, message, opts \\ []) do
    identifier
    |> format_message(message, opts)
    |> Logger.info(opts)
  end

  @doc false
  @spec warning(identifier :: atom() | binary(), message :: binary()) :: :ok
  @spec warning(identifier :: atom() | binary(), message :: binary(), opts :: keyword()) :: :ok
  if Code.ensure_loaded?(:logger) and function_exported?(:logger, :warning, 2) do
    def warning(identifier, message, opts \\ []) do
      identifier
      |> format_message(message, opts)
      |> Logger.warning(opts)
    end
  else
    def warning(identifier, message, opts \\ []) do
      identifier
      |> format_message(message, opts)
      |> Logger.warn(opts)
    end
  end

  @doc false
  @spec error(identifier :: atom() | binary(), message :: binary()) :: :ok
  @spec error(identifier :: atom() | binary(), message :: binary(), opts :: keyword()) :: :ok
  def error(identifier, message, opts \\ []) do
    identifier
    |> format_message(message, opts)
    |> Logger.error(opts)
  end

  defp format_message(identifier, message, opts) do
    stacktrace =
      if opts[:stacktrace] do
        self()
        |> Process.info(:current_stacktrace)
        |> elem(1)
        |> Enum.drop(1)
        |> Exception.format_stacktrace()
      end

    "[#{normalize_identifier(identifier)}] #{message}" <>
      "#{stacktrace && "\n\n" <> stacktrace}"
  end

  defp normalize_identifier(identifier) do
    identifier = to_string(identifier)

    if String.contains?(identifier, "Elixir.") do
      String.replace(identifier, "Elixir.", "")
    else
      identifier
    end
  end
end
