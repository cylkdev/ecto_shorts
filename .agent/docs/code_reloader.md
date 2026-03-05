```elixir
defmodule Phoenix.CodeReloader do
  @moduledoc """
  A plug and module to handle automatic code reloading.

  To avoid race conditions, all code reloads are funneled through a
  sequential call operation.
  """

  ## Server delegation

  @doc """
  Reloads code for the current Mix project by invoking the
  `:reloadable_compilers` on the list of `:reloadable_apps`.

  This is configured in your application environment like:

      config :your_app, YourAppWeb.Endpoint,
        reloadable_compilers: [:gettext, :elixir],
        reloadable_apps: [:ui, :backend]

  Keep in mind `:reloadable_compilers` must be a subset of the
  `:compilers` specified in `project/0` in your `mix.exs`.

  The `:reloadable_apps` defaults to `nil`. In such case
  default behavior is to reload the current project if it
  consists of a single app, or all applications within an umbrella
  project. You can set `:reloadable_apps` to a subset of default
  applications to reload only some of them, an empty list - to
  effectively disable the code reloader, or include external
  applications from library dependencies.

  This function is a no-op and returns `:ok` if Mix is not available.

  The reloader should also be configured as a Mix listener in project's
  mix.exs file (since Elixir v1.18):

      def project do
        [
          ...,
          listeners: [Phoenix.CodeReloader]
        ]
      end

  This way the reloader can notice whenever the project is compiled
  concurrently.

  ## Options

    * `:reloadable_args` - additional CLI args to pass to the compiler tasks.
      Defaults to `["--no-all-warnings"]` so only warnings related to the
      files being compiled are printed

  """
  @spec reload(module, keyword) :: :ok | {:error, binary()}
  def reload(endpoint, opts \\ [])  do
    # ...
  end

  @doc """
  Same as `reload/1` but it will raise if Mix is not available.
  """
  @spec reload!(module, keyword) :: :ok | {:error, binary()}
  defdelegate reload!(endpoint, opts), to: Phoenix.CodeReloader.Server

  @doc """
  Synchronizes with the code server if it is alive.

  It returns `:ok`. If it is not running, it also returns `:ok`.
  """
  @spec sync :: :ok
  defdelegate sync, to: Phoenix.CodeReloader.Server

  @doc false
  @spec child_spec(keyword) :: Supervisor.child_spec()
  defdelegate child_spec(opts), to: Phoenix.CodeReloader.MixListener

  ## Plug

  @behaviour Plug
  import Plug.Conn

  @style %{
    light: %{
      primary: "#EB532D",
      accent: "#a0b0c0",
      text_color: "#304050",
      background: "#ffffff",
      heading_background: "#f9f9fa"
    },
    dark: %{
      primary: "#FF6B4A",
      accent: "#c0c0c0",
      text_color: "#e5e5e5",
      background: "#1a1a1a",
      heading_background: "#2a2a2a"
    },
    logo: "data:image/svg+xml;base64,...",
    monospace_font: "menlo, consolas, monospace"
  }

  @doc """
  API used by Plug to start the code reloader.
  """
  def init(opts)  do
    # ...
  end

  @doc """
  API used by Plug to invoke the code reloader on every request.
  """
  def call(conn, opts)  do
    # ...
  end

end

```
