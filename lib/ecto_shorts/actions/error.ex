defmodule EctoShorts.Actions.Error do
  @moduledoc """
  Defines a standard error structure used across EctoShorts actions and
  provides a flexible interface for generating actionable errors.

  This module can be overridden via configuration by setting
  `:error_module` in your application config or passing it in explicitly
  via options to `call/4`.
  """

  @doc """
  Defines the callback used to construct an error struct.

  This callback must be implemented by custom error modules and
  is invoked by `EctoShorts.Actions.Error.call/4`.

  The returned map must include the keys `:code`, `:message`, and `:details`.

  ## Usage

  ```elixir

  defmodule MyApp.CustomError do
    @behaviour EctoShorts.Actions.Error

    def create_error(code, message, details) do
      %{code: code, message: "[MyApp] " <> message, details: details}
    end
  end

  ```
  """
  @callback create_error(atom(), binary(), map()) :: any()

  alias EctoShorts.Config

  @default_error_module __MODULE__

  @doc """
  Creates an error struct using the default or configured error module.

  `code` is an atom representing the error type (e.g., `:not_found`,
  `:conflict`). `message` is a human-readable binary string. `details`
  is a map of additional context. `opts` is an optional keyword list.

  Resolves the error module in this order: the `:error_module` key in
  `opts`, then the `:error_module` application config, then falls back
  to `EctoShorts.Actions.Error` itself.

  Returns the result of calling `create_error/3` on the resolved module.
  By default this is an `ErrorMessage` struct with `:code`, `:message`,
  and `:details` fields.

  ## Options

    * `:error_module` — a module implementing the
      `EctoShorts.Actions.Error` behaviour.

  ## Examples

      iex> EctoShorts.Actions.Error.call(:not_found, "User not found", %{id: 123})
      %ErrorMessage{code: :not_found, message: "User not found", details: %{id: 123}}

      iex> EctoShorts.Actions.Error.call(:bad_request, "Missing param", nil, error_module: MyApp.CustomError)
  """
  @spec call(atom(), binary(), map() | nil, keyword()) :: any()
  def call(code, message, details, opts \\ []) do
    error_module(opts).create_error(code, message, details)
  end

  defp error_module(opts) do
    opts[:error_module] ||
      Config.error_module() ||
      @default_error_module
  end

  @doc """
  Default implementation of `create_error/3`.

  Builds an `ErrorMessage` struct from the given `code`, `message`, and
  `details`. This is the fallback used when no custom error module is
  configured.

  Returns `%ErrorMessage{code: code, message: message, details: details}`.
  """
  def create_error(code, message, details) do
    struct!(ErrorMessage,
      code: code,
      message: message,
      details: details
    )
  end
end
