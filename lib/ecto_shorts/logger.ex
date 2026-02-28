defmodule EctoShorts.Logger do
  @moduledoc """
  Prefixed logging wrapper used internally by EctoShorts modules.

  Each log function prepends a `[prefix]` tag to the message before
  delegating to Elixir's `Logger`.
  """
  require Logger

  def debug(prefix, message) do
    message
    |> format_message(prefix)
    |> Logger.debug()
  end

  def info(prefix, message) do
    message
    |> format_message(prefix)
    |> Logger.info()
  end

  def error(prefix, message) do
    message
    |> format_message(prefix)
    |> Logger.error()
  end

  def warning(prefix, message) do
    message
    |> format_message(prefix)
    |> Logger.warning()
  end

  defp format_message(message, prefix) do
    "[#{prefix}] " <> message
  end
end
