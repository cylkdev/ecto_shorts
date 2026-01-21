defmodule EctoShorts.Logger do
  @moduledoc false
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
