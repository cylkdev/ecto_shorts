defmodule EctoShorts.Actions.Error do
  @moduledoc """
  This module generates errors from actions it can be
  overridden by config by setting error module

  Errors from Actions: [:not_found, :bad_request, :internal_server_error]
  """

  @type t :: %{
    optional(key :: atom()) => value :: term(),
    code: atom(),
    message: binary(),
    details: nil | map()
  }

  @callback create_error(atom, String.t, map) :: t()

  @default_error_module __MODULE__

  @spec call(
    code :: atom(),
    message :: binary(),
    details :: map() | nil
  ) :: t()
  def call(code, message, details) do
    call(code, message, details, [])
  end

  @spec call(
    code :: atom(),
    message :: binary(),
    details :: map() | nil,
    opts :: keyword()
  ) :: t()
  def call(code, message, details, opts) do
    module = error_module(opts)

    case module.create_error(code, message, details) do
      %{code: _, message: _, details: _} = error_message ->
        error_message

      term ->
        raise """
        Expected the error returned by #{inspect(module)} to a map of type:

        ```
        %{
          code: atom(),
          message: binary(),
          details: nil | map()
        }
        ```

        got:

        #{inspect(term)}
        """
    end
  end

  defp error_module(opts) do
    opts[:error_module] ||
    EctoShorts.Config.from_app_env(:error_module) ||
    @default_error_module
  end

  def create_error(code, message, details) do
    struct!(ErrorMessage, code: code, message: message, details: details)
  end
end
